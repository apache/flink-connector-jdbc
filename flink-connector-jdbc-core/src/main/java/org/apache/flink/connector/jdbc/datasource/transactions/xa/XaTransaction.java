package org.apache.flink.connector.jdbc.datasource.transactions.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.datasource.connections.xa.XaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.EmptyTransactionXaException;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.TransientXaException;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterState;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/** Class that manages all relevant XA operations. */
@Internal
public class XaTransaction implements Serializable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(XaTransaction.class);

    private final XaConnectionProvider xaConnectionProvider;
    private final JdbcExactlyOnceOptions exactlyOnceOptions;

    // checkpoints and the corresponding transactions waiting for completion notification from JM
    private transient List<TransactionId> preparedXids = new ArrayList<>();
    // hanging XIDs - used for cleanup
    // it's a list to support retries and scaling down
    // possible transaction states: active, idle, prepared
    // last element is the current xid
    private transient Deque<TransactionId> hangingXids = new LinkedList<>();
    private transient TransactionId currentTid;

    private final TransactionId baseTransaction;

    public XaTransaction(
            JdbcExactlyOnceOptions exactlyOnceOptions,
            TransactionId transactionId,
            XaConnectionProvider xaFacade) {
        this.xaConnectionProvider = xaFacade;
        this.exactlyOnceOptions = exactlyOnceOptions;
        this.baseTransaction = transactionId;
    }

    public Xid getCurrentXid() {
        return currentTid;
    }

    public XaConnectionProvider getConnectionProvider() {
        return xaConnectionProvider;
    }

    public JdbcWriterState getState() {
        return JdbcWriterState.of(preparedXids, hangingXids);
    }

    public void open(JdbcWriterState state) throws IOException {
        try {
            xaConnectionProvider.open();
            recoverState(state);
            hangingXids = new LinkedList<>(failOrRollback(hangingXids).getForRetry());
            commitTx();
            if (exactlyOnceOptions.isDiscoverAndRollbackOnRecovery()) {
                // Pending transactions which are not included into the checkpoint might hold locks
                // and
                // should be rolled back. However, rolling back ALL transactions can cause data
                // loss. So
                // each subtask first commits transactions from its state and then rolls back
                // discovered
                // transactions if they belong to it.
                recoverAndRollback();
            }
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (currentTid != null && xaConnectionProvider.isOpen()) {
            try {
                LOG.debug(
                        "remove current transaction before closing, xid={}",
                        currentTid.getXidValue());
                xaConnectionProvider.failAndRollback(currentTid);
            } catch (Exception e) {
                LOG.warn(
                        "unable to fail/rollback current transaction, xid={}",
                        currentTid.getXidValue(),
                        e);
            }
        }
        xaConnectionProvider.close();
        currentTid = null;
        hangingXids = null;
        preparedXids = null;
    }

    public void recoverState(JdbcWriterState state) {
        hangingXids = new LinkedList<>(state.getHanging());
        preparedXids = new ArrayList<>(state.getPrepared());
        LOG.info(
                "initialized state: prepared xids: {}, hanging xids: {}",
                preparedXids.size(),
                hangingXids.size());
    }

    public void checkState() {
        Preconditions.checkState(currentTid != null, "current xid must not be null");
        Preconditions.checkState(
                !hangingXids.isEmpty() && hangingXids.peekLast().equals(currentTid),
                "inconsistent internal state");
    }

    /** @param checkpointId to associate with the new transaction. */
    public void createTx(long checkpointId) throws IOException {
        try {
            Preconditions.checkState(currentTid == null, "currentXid not null");
            currentTid = baseTransaction.withBranch(checkpointId);
            hangingXids.offerLast(currentTid);
            xaConnectionProvider.start(currentTid);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
    }

    public void prepareTx() throws IOException {
        checkState();
        hangingXids.pollLast();
        try {
            xaConnectionProvider.endAndPrepare(currentTid);
            preparedXids.add(currentTid);
        } catch (EmptyTransactionXaException e) {
            LOG.info(
                    "empty XA transaction (skip), xid: {}, checkpoint {}",
                    currentTid.getXidValue(),
                    currentTid.getCheckpointId());
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
        currentTid = null;
    }

    public void commitTx() {
        List<TransactionId> toCommit = preparedXids;
        preparedXids = new ArrayList<>();
        preparedXids.addAll(commitXids(toCommit));
    }

    public void commitTxUntil(long checkpointId) {
        Tuple2<List<TransactionId>, List<TransactionId>> splittedXids =
                split(preparedXids, checkpointId);

        if (splittedXids.f0.isEmpty()) {
            LOG.warn("nothing to commit up to checkpoint: {}", checkpointId);
        } else {
            preparedXids = splittedXids.f1;
            preparedXids.addAll(commitXids(splittedXids.f0));
        }
    }

    public List<TransactionId> commitXids(List<TransactionId> xids) {
        return commit(
                        xids,
                        exactlyOnceOptions.isAllowOutOfOrderCommits(),
                        exactlyOnceOptions.getMaxCommitAttempts())
                .getForRetry();
    }

    private Tuple2<List<TransactionId>, List<TransactionId>> split(
            List<TransactionId> list, long checkpointId) {
        return split(list, checkpointId, true);
    }

    private Tuple2<List<TransactionId>, List<TransactionId>> split(
            List<TransactionId> list, long checkpointId, boolean checkpointIntoLo) {

        List<TransactionId> lo = new ArrayList<>(list.size() / 2);
        List<TransactionId> hi = new ArrayList<>(list.size() / 2);
        list.forEach(
                i -> {
                    if (i.getCheckpointId() < checkpointId
                            || (i.getCheckpointId() == checkpointId && checkpointIntoLo)) {
                        lo.add(i);
                    } else {
                        hi.add(i);
                    }
                });
        return new Tuple2<>(lo, hi);
    }

    private XaTransactionResult<TransactionId> commit(
            List<TransactionId> xids, boolean allowOutOfOrderCommits, int maxCommitAttempts) {
        XaTransactionResult<TransactionId> result = new XaTransactionResult<>();
        int origSize = xids.size();
        LOG.debug("commit {} transactions", origSize);
        for (Iterator<TransactionId> i = xids.iterator();
                i.hasNext() && (result.hasNoFailures() || allowOutOfOrderCommits); ) {
            TransactionId x = i.next();
            i.remove();
            try {
                xaConnectionProvider.commit(x, x.getRestored());
                result.succeeded(x);
            } catch (TransientXaException e) {
                result.failedTransiently(x.withAttemptsIncremented(), e);
            } catch (Exception e) {
                result.failed(x, e);
            }
        }
        result.getForRetry().addAll(xids);
        result.throwIfAnyFailed("commit");
        throwIfAnyReachedMaxAttempts(result, maxCommitAttempts);
        result.getTransientFailure()
                .ifPresent(
                        f ->
                                LOG.warn(
                                        "failed to commit {} transactions out of {} (keep them to retry later)",
                                        result.getForRetry().size(),
                                        origSize,
                                        f));
        return result;
    }

    private XaTransactionResult<TransactionId> failOrRollback(Collection<TransactionId> xids) {
        XaTransactionResult<TransactionId> result = new XaTransactionResult<>();
        if (xids.isEmpty()) {
            return result;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("rolling back {} transactions: {}", xids.size(), xids);
        }
        for (TransactionId x : xids) {
            try {
                xaConnectionProvider.failAndRollback(x);
                result.succeeded(x);
            } catch (TransientXaException e) {
                LOG.info("unable to fail/rollback transaction, xid={}: {}", x, e.getMessage());
                result.failedTransiently(x, e);
            } catch (Exception e) {
                LOG.warn("unable to fail/rollback transaction, xid={}: {}", x, e.getMessage());
                result.failed(x, e);
            }
        }
        if (!result.getForRetry().isEmpty()) {
            LOG.info("failed to roll back {} transactions", result.getForRetry().size());
        }
        return result;
    }

    private void recoverAndRollback() {
        Collection<Xid> recovered = xaConnectionProvider.recover();
        if (recovered.isEmpty()) {
            return;
        }
        LOG.warn("rollback {} recovered transactions", recovered.size());
        for (Xid xid : recovered) {
            if (baseTransaction.belongsTo(xid)) {
                try {
                    xaConnectionProvider.rollback(xid);
                } catch (Exception e) {
                    LOG.info("unable to rollback recovered transaction, xid={}", xid, e);
                }
            }
        }
    }

    private void throwIfAnyReachedMaxAttempts(
            XaTransactionResult<TransactionId> result, int maxAttempts) {
        List<TransactionId> reached = null;
        for (TransactionId x : result.getForRetry()) {
            if (x.getAttempts() >= maxAttempts) {
                if (reached == null) {
                    reached = new ArrayList<>();
                }
                reached.add(x);
            }
        }
        if (reached != null) {
            throw new RuntimeException(
                    String.format(
                            "reached max number of commit attempts (%d) for transactions: %s",
                            maxAttempts, reached));
        }
    }
}
