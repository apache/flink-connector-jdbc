package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.XADataSource;
import javax.transaction.xa.Xid;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/** */
public class XaTransaction implements Serializable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(XaTransaction.class);

    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;
    private final XidGenerator xidGenerator;
    private final JdbcExactlyOnceOptions exactlyOnceOptions;

    // checkpoints and the corresponding transactions waiting for completion notification from JM
    private transient List<CheckpointAndXid> preparedXids = new ArrayList<>();
    // hanging XIDs - used for cleanup
    // it's a list to support retries and scaling down
    // possible transaction states: active, idle, prepared
    // last element is the current xid
    private transient Deque<Xid> hangingXids = new LinkedList<>();
    private transient Xid currentXid;

    public XaTransaction(
            JdbcExactlyOnceOptions exactlyOnceOptions,
            SerializableSupplier<XADataSource> dataSourceSupplier) {
        this(
                exactlyOnceOptions,
                XaFacade.fromXaDataSourceSupplier(
                        dataSourceSupplier,
                        exactlyOnceOptions.getTimeoutSec(),
                        exactlyOnceOptions.isTransactionPerConnection()));
    }

    public XaTransaction(JdbcExactlyOnceOptions exactlyOnceOptions, XaFacade xaFacade) {
        this.xaFacade = xaFacade;
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
        this.xidGenerator = XidGenerator.semanticXidGenerator();
        this.exactlyOnceOptions = exactlyOnceOptions;
    }

    public Xid getCurrentXid() {
        return currentXid;
    }

    public XaFacade getXaFacade() {
        return xaFacade;
    }

    public JdbcXaSinkFunctionState getState() {
        return JdbcXaSinkFunctionState.of(preparedXids, hangingXids);
    }

    public void open(JobSubtask subtask) throws IOException {
        this.open(subtask, JdbcXaSinkFunctionState.empty());
    }

    public void open(JobSubtask subtask, JdbcXaSinkFunctionState state) throws IOException {
        try {
            xidGenerator.open();
            xaFacade.open();
            recoverState(state);
            hangingXids = new LinkedList<>(xaGroupOps.failOrRollback(hangingXids).getForRetry());
            commitTx();
            if (exactlyOnceOptions.isDiscoverAndRollbackOnRecovery()) {
                // Pending transactions which are not included into the checkpoint might hold locks
                // and
                // should be rolled back. However, rolling back ALL transactions can cause data
                // loss. So
                // each subtask first commits transactions from its state and then rolls back
                // discovered
                // transactions if they belong to it.
                xaGroupOps.recoverAndRollback(subtask, xidGenerator);
            }
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (currentXid != null && xaFacade.isOpen()) {
            try {
                LOG.debug("remove current transaction before closing, xid={}", currentXid);
                xaFacade.failAndRollback(currentXid);
            } catch (Exception e) {
                LOG.warn("unable to fail/rollback current transaction, xid={}", currentXid, e);
            }
        }
        xaFacade.close();
        xidGenerator.close();
        currentXid = null;
        hangingXids = null;
        preparedXids = null;
    }

    public void recoverState(JdbcXaSinkFunctionState state) {
        hangingXids = new LinkedList<>(state.getHanging());
        preparedXids = new ArrayList<>(state.getPrepared());
        LOG.info(
                "initialized state: prepared xids: {}, hanging xids: {}",
                preparedXids.size(),
                hangingXids.size());
    }

    public void checkState() {
        Preconditions.checkState(currentXid != null, "current xid must not be null");
        Preconditions.checkState(
                !hangingXids.isEmpty() && hangingXids.peekLast().equals(currentXid),
                "inconsistent internal state");
    }

    /** @param checkpointId to associate with the new transaction. */
    public void createTx(JobSubtask subtask, long checkpointId) throws Exception {
        Preconditions.checkState(currentXid == null, "currentXid not null");
        currentXid = xidGenerator.generateXid(subtask, checkpointId);
        hangingXids.offerLast(currentXid);
        xaFacade.start(currentXid);
    }

    public void prepareTx(long checkpointId) throws IOException {
        checkState();
        hangingXids.pollLast();
        try {
            xaFacade.endAndPrepare(currentXid);
            preparedXids.add(CheckpointAndXid.createNew(checkpointId, currentXid));
        } catch (XaFacade.EmptyXaTransactionException e) {
            LOG.info(
                    "empty XA transaction (skip), xid: {}, checkpoint {}",
                    currentXid,
                    checkpointId);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
        currentXid = null;
    }

    private void commitTx() {
        List<CheckpointAndXid> toCommit = preparedXids;
        preparedXids = new ArrayList<>();
        preparedXids.addAll(commitXids(toCommit));
    }

    public void commitTxUntil(long checkpointId) {
        Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> splittedXids =
                split(preparedXids, checkpointId);

        if (splittedXids.f0.isEmpty()) {
            LOG.warn("nothing to commit up to checkpoint: {}", checkpointId);
        } else {
            preparedXids = splittedXids.f1;
            preparedXids.addAll(commitXids(splittedXids.f0));
        }
    }

    private List<CheckpointAndXid> commitXids(List<CheckpointAndXid> xids) {
        return xaGroupOps
                .commit(
                        xids,
                        exactlyOnceOptions.isAllowOutOfOrderCommits(),
                        exactlyOnceOptions.getMaxCommitAttempts())
                .getForRetry();
    }

    private Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> split(
            List<CheckpointAndXid> list, long checkpointId) {
        return split(list, checkpointId, true);
    }

    private Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> split(
            List<CheckpointAndXid> list, long checkpointId, boolean checkpointIntoLo) {

        List<CheckpointAndXid> lo = new ArrayList<>(list.size() / 2);
        List<CheckpointAndXid> hi = new ArrayList<>(list.size() / 2);
        list.forEach(
                i -> {
                    if (i.checkpointId < checkpointId
                            || (i.checkpointId == checkpointId && checkpointIntoLo)) {
                        lo.add(i);
                    } else {
                        hi.add(i);
                    }
                });
        return new Tuple2<>(lo, hi);
    }
}
