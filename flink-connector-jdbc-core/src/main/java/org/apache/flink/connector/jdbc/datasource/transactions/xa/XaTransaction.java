/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.datasource.transactions.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.datasource.connections.xa.XaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.EmptyTransactionXaException;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.TransientXaException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/** Class that manages all relevant XA operations. */
@Internal
public class XaTransaction implements Serializable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(XaTransaction.class);

    private final XaConnectionProvider xaConnectionProvider;
    private final JdbcExactlyOnceOptions exactlyOnceOptions;

    // prepared xids from the restored writer state, committed on open
    private transient List<TransactionId> preparedXids = new ArrayList<>();
    // hanging XIDs - used for cleanup
    // it's a list to support retries and scaling down
    // possible transaction states: active, idle, prepared
    // last element is the current xid
    private transient Deque<TransactionId> hangingXids = new LinkedList<>();
    private transient TransactionId currentTid;
    // restored hanging xids under the prefix that another subtask owns; rolled back here only if
    // this writer can't list prepared transactions, as then their owner can't either
    private transient List<TransactionId> unownedHangingXids = new ArrayList<>();

    private final TransactionId baseTransaction;

    public XaTransaction(
            JdbcExactlyOnceOptions exactlyOnceOptions,
            TransactionId transactionId,
            XaConnectionProvider xaFacade) {
        this.xaConnectionProvider = xaFacade;
        this.exactlyOnceOptions = exactlyOnceOptions;
        this.baseTransaction = transactionId;
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
                // and should be rolled back. However, rolling back ALL transactions can cause data
                // loss: the transactions of checkpoints before the restored one are committed by
                // the committer, which restores them from its own state. rollbackPreparedTxs()
                // rolls back this writer's own transactions. The lookup below never matches a
                // driver Xid (FLINK-41101).
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
        unownedHangingXids = null;
        preparedXids = null;
    }

    /**
     * Keeps only the hanging transactions of this job, which {@link #open} rolls back. Those of
     * another job may belong to a job that still runs and is about to commit them, for example the
     * job that took the savepoint this one restores. With a prefix it keeps those under the prefix
     * that it owns, see {@link #isLeftoverFromCheckpoint}, which also covers earlier jobs; rolling
     * them back here needs no listing of the prepared transactions.
     */
    public void recoverState(JdbcWriterState state) {
        hangingXids = new LinkedList<>();
        unownedHangingXids = new ArrayList<>();
        for (TransactionId hanging : state.getHanging()) {
            if (baseTransaction.getPrefixBytes() == null
                    ? Arrays.equals(hanging.getJobId(), baseTransaction.getJobId())
                    : isOwnedUnderPrefix(hanging)) {
                hangingXids.add(hanging);
            } else if (baseTransaction.getPrefixBytes() != null
                    && Arrays.equals(hanging.getPrefixBytes(), baseTransaction.getPrefixBytes())) {
                unownedHangingXids.add(hanging);
            } else {
                LOG.info(
                        "leaving the restored transaction of another job, xid={}",
                        hanging.getXidValue());
            }
        }
        preparedXids = new ArrayList<>(state.getPrepared());
        LOG.info(
                "initialized state: prepared xids: {}, hanging xids: {}, hanging xids of other"
                        + " subtasks: {}",
                preparedXids.size(),
                hangingXids.size(),
                unownedHangingXids.size());
    }

    public void checkState() {
        Preconditions.checkState(currentTid != null, "current xid must not be null");
        Preconditions.checkState(
                !hangingXids.isEmpty() && hangingXids.peekLast().equals(currentTid),
                "inconsistent internal state");
    }

    /**
     * Rolls back the prepared transactions left behind from the given checkpoint on. Their records
     * are replayed, and the committer only commits transactions of earlier checkpoints.
     */
    public void rollbackPreparedTxs(long checkpointId) {
        Collection<Xid> recovered;
        try {
            recovered = xaConnectionProvider.recover();
        } catch (Exception e) {
            LOG.warn(
                    "unable to recover prepared transactions, those left behind that aren't in the"
                            + " restored state keep their locks",
                    e);
            hangingXids.addAll(failOrRollback(unownedHangingXids).getForRetry());
            return;
        }
        for (Xid xid : recovered) {
            if (isLeftoverFromCheckpoint(xid, checkpointId)) {
                LOG.info("rolling back a leftover transaction, xid={}", xid);
                try {
                    xaConnectionProvider.rollback(xid);
                } catch (RuntimeException e) {
                    if (isPrepared(xid)) {
                        throw e;
                    }
                    LOG.info("leftover transaction is gone already, xid={}", xid);
                }
            }
        }
    }

    /**
     * A transaction id names the job, the subtask and the parallelism, and the prefix if there is
     * one. Without a prefix this writer only rolls back its own transactions: another exactly-once
     * sink of the job at the same parallelism can't run, as it starts the same xids, and at another
     * parallelism its xids differ only in the parallelism.
     *
     * <p>With a prefix it also rolls back those of earlier jobs and parallelisms under the prefix,
     * of the subtasks whose index is its own modulo the parallelism, so each has a single owner as
     * long as all writers can list them. At the same job and parallelism that is only its own, as
     * in BATCH the other writers may have prepared their transactions for the committer already.
     */
    private boolean isLeftoverFromCheckpoint(Xid xid, long checkpointId) {
        TransactionId transactionId;
        try {
            transactionId =
                    TransactionId.createFromXid(
                            xid.getFormatId(),
                            xid.getGlobalTransactionId(),
                            xid.getBranchQualifier());
        } catch (IOException e) {
            return false;
        }
        if (transactionId.getCheckpointId() < checkpointId) {
            return false;
        }
        if (xid.getFormatId() == baseTransaction.getFormatId()
                && Arrays.equals(
                        xid.getGlobalTransactionId(), baseTransaction.getGlobalTransactionId())
                && transactionId.getNumberOfSubtasks() == baseTransaction.getNumberOfSubtasks()) {
            return true;
        }
        return baseTransaction.getPrefixBytes() != null && isOwnedUnderPrefix(transactionId);
    }

    private boolean isOwnedUnderPrefix(TransactionId transactionId) {
        return Arrays.equals(transactionId.getPrefixBytes(), baseTransaction.getPrefixBytes())
                && transactionId.getSubtaskId() % baseTransaction.getNumberOfSubtasks()
                        == baseTransaction.getSubtaskId();
    }

    private boolean isPrepared(Xid xid) {
        return xaConnectionProvider.recover().stream()
                .anyMatch(
                        recovered ->
                                recovered.getFormatId() == xid.getFormatId()
                                        && Arrays.equals(
                                                recovered.getGlobalTransactionId(),
                                                xid.getGlobalTransactionId())
                                        && Arrays.equals(
                                                recovered.getBranchQualifier(),
                                                xid.getBranchQualifier()));
    }

    /**
     * @param checkpointId to associate with the new transaction.
     */
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

    /**
     * Ends and prepares the current transaction. The caller is responsible for committing it.
     *
     * @return the xid of the prepared transaction, empty if it had nothing to commit
     */
    public Optional<TransactionId> prepareTx() throws IOException {
        checkState();
        hangingXids.pollLast();
        TransactionId prepared = null;
        try {
            xaConnectionProvider.endAndPrepare(currentTid);
            prepared = currentTid;
        } catch (EmptyTransactionXaException e) {
            LOG.info(
                    "empty XA transaction (skip), xid: {}, checkpoint {}",
                    currentTid.getXidValue(),
                    currentTid.getCheckpointId());
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
        currentTid = null;
        return Optional.ofNullable(prepared);
    }

    public void commitTx() {
        List<TransactionId> toCommit = preparedXids;
        preparedXids = new ArrayList<>();
        preparedXids.addAll(commitXids(toCommit));
    }

    public List<TransactionId> commitXids(List<TransactionId> xids) {
        return commit(
                        xids,
                        exactlyOnceOptions.isAllowOutOfOrderCommits(),
                        exactlyOnceOptions.getMaxCommitAttempts())
                .getForRetry();
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
