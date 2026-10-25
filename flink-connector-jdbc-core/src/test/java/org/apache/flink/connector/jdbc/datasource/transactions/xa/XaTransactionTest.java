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

import org.apache.flink.api.common.JobID;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.datasource.connections.xa.SimpleXaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.XaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import javax.transaction.xa.Xid;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link XaTransaction} against a database with two-phase commit. */
public abstract class XaTransactionTest implements DatabaseTest {

    private static final BooksTable TEST_TABLE = new BooksTable("XaTransactionTable");

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TEST_TABLE);
    }

    /** A failed test can leave a prepared transaction behind, which holds its locks. */
    @AfterEach
    void rollbackPreparedTransactions() throws Exception {
        try (SimpleXaConnectionProvider xa = newXaConnectionProvider()) {
            xa.open();
            for (Xid xid : xa.recover()) {
                xa.rollback(xid);
            }
        }
    }

    @Test
    void testRollbackPreparedTxsFromCheckpoint() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId earlier = prepare(jobId, 0, 1, 2, 1);
        TransactionId leftover = prepare(jobId, 0, 1, 3, 2);

        rollbackPreparedTxs(jobId, 0, 1, 3);

        assertThat(isPrepared(earlier)).isTrue();
        assertThat(isPrepared(leftover)).isFalse();
    }

    /** Another exactly-once sink of the same job writes with the same job id and subtask index. */
    @Test
    void testRollbackPreparedTxsKeepsOtherParallelism() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId otherSink = prepare(jobId, 0, 4, 5, 1);

        rollbackPreparedTxs(jobId, 0, 2, 5);

        assertThat(isPrepared(otherSink)).isTrue();
    }

    @Test
    void testRollbackPreparedTxsKeepsOtherSubtask() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId otherSubtask = prepare(jobId, 1, 2, 5, 1);

        rollbackPreparedTxs(jobId, 0, 2, 5);

        assertThat(isPrepared(otherSubtask)).isTrue();
    }

    /**
     * A job restored from a savepoint while the job that took it still runs would roll back the
     * transaction that job's committer is about to commit.
     */
    @Test
    void testOpenKeepsRestoredHangingOfEarlierJob() throws Exception {
        byte[] earlierJobId = new JobID().getBytes();
        TransactionId hanging = prepare(earlierJobId, 0, 1, 5, 1);

        rollbackPreparedTxs(new JobID().getBytes(), 0, 1, 5, restoredState(hanging));

        assertThat(isPrepared(hanging)).isTrue();
    }

    /** The state of a failed attempt of the same job, here before an upscale. */
    @Test
    void testOpenRollsBackRestoredHangingOfSameJob() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId hanging = prepare(jobId, 0, 1, 5, 1);

        rollbackPreparedTxs(jobId, 0, 2, 5, restoredState(hanging));

        assertThat(isPrepared(hanging)).isFalse();
    }

    @Test
    void testRollbackPreparedTxsKeepsEarlierJobWithoutPrefix() throws Exception {
        TransactionId earlierJob = prepare(new JobID().getBytes(), 0, 1, 5, 1);

        rollbackPreparedTxs(new JobID().getBytes(), 0, 1, 0);

        assertThat(isPrepared(earlierJob)).isTrue();
    }

    /** A restore from a savepoint into a new job, or a new job after a lost one. */
    @Test
    void testRollbackPreparedTxsOfEarlierJobWithPrefix() throws Exception {
        byte[] earlierJobId = new JobID().getBytes();
        TransactionId committed = prepare("books", earlierJobId, 0, 1, 4, 1);
        TransactionId leftover = prepare("books", earlierJobId, 0, 1, 5, 2);

        rollbackPreparedTxs("books", new JobID().getBytes(), 0, 1, 5, JdbcWriterState.empty());

        assertThat(isPrepared(committed)).isTrue();
        assertThat(isPrepared(leftover)).isFalse();
    }

    @Test
    void testRollbackPreparedTxsKeepsOtherPrefix() throws Exception {
        TransactionId otherSink = prepare("authors", new JobID().getBytes(), 0, 1, 5, 1);

        rollbackPreparedTxs("books", new JobID().getBytes(), 0, 1, 0, JdbcWriterState.empty());

        assertThat(isPrepared(otherSink)).isTrue();
    }

    /** Each subtask owns the subtask indexes equal to its own modulo the parallelism. */
    @Test
    void testRollbackPreparedTxsOfOtherParallelismWithPrefix() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId owned = prepare("books", jobId, 3, 4, 5, 1);
        TransactionId notOwned = prepare("books", jobId, 2, 4, 5, 2);

        rollbackPreparedTxs("books", jobId, 1, 2, 5, JdbcWriterState.empty());

        assertThat(isPrepared(owned)).isFalse();
        assertThat(isPrepared(notOwned)).isTrue();
    }

    /**
     * In BATCH a writer can restart while the others have already prepared their transactions for
     * the committer.
     */
    @Test
    void testRollbackPreparedTxsKeepsOtherSubtaskWithPrefix() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId otherSubtask = prepare("books", jobId, 1, 2, 0, 1);

        rollbackPreparedTxs("books", jobId, 0, 2, 0, JdbcWriterState.empty());

        assertThat(isPrepared(otherSubtask)).isTrue();
    }

    /**
     * A job restored from a savepoint with another prefix, or with one where the job that took it
     * had none, leaves that job alone, as it may still run.
     */
    @Test
    void testRollbackPreparedTxsKeepsRestoredHangingOfOtherPrefix() throws Exception {
        byte[] earlierJobId = new JobID().getBytes();
        TransactionId blue = prepare("blue", earlierJobId, 0, 1, 5, 1);
        TransactionId none = prepare(earlierJobId, 1, 2, 5, 2);
        JdbcWriterState state =
                JdbcWriterState.of(Collections.emptyList(), Arrays.asList(blue, none));

        rollbackPreparedTxs("green", new JobID().getBytes(), 0, 1, 5, state);

        assertThat(isPrepared(blue)).isTrue();
        assertThat(isPrepared(none)).isTrue();
    }

    /**
     * After a downscale from 4 to 2, split list state gives the states of subtasks 0 and 1 to
     * subtask 0, while subtask 1 owns old subtask 1 modulo 2. Without listing, only subtask 0 knows
     * that transaction.
     */
    @Test
    void testRollbackRestoredHangingWithPrefixAfterDownscaleWithoutRecover() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId hanging = prepare("books", jobId, 1, 4, 5, 1);
        for (int subtask = 0; subtask < 2; subtask++) {
            try (XaTransaction transaction =
                    new XaTransaction(
                            JdbcExactlyOnceOptions.builder()
                                    .withRecoveredAndRollback(false)
                                    .build(),
                            TransactionId.create("books", jobId, subtask, 2),
                            withoutRecover(newXaConnectionProvider()))) {
                transaction.open(subtask == 0 ? restoredState(hanging) : JdbcWriterState.empty());
                transaction.rollbackPreparedTxs(5);
            }
        }

        assertThat(isPrepared(hanging)).isFalse();
    }

    private static XaConnectionProvider withoutRecover(SimpleXaConnectionProvider provider) {
        return proxy(
                provider,
                (delegate, method, args) -> {
                    if (method.getName().equals("recover")) {
                        throw new RuntimeException("XA_RECOVER_ADMIN missing");
                    }
                    return method.invoke(delegate, args);
                });
    }

    /** A transaction that is gone by the time the writer rolls it back is no failure. */
    @Test
    void testRollbackPreparedTxsOfTransactionRolledBackMeanwhile() throws Exception {
        TransactionId leftover = prepare("books", new JobID().getBytes(), 0, 1, 5, 1);
        XaConnectionProvider racing =
                proxy(
                        newXaConnectionProvider(),
                        (delegate, method, args) -> {
                            if (method.getName().equals("rollback")) {
                                // another writer gets there first
                                try (SimpleXaConnectionProvider other = newXaConnectionProvider()) {
                                    other.open();
                                    other.rollback((Xid) args[0]);
                                }
                            }
                            return method.invoke(delegate, args);
                        });

        try (XaTransaction transaction =
                new XaTransaction(
                        JdbcExactlyOnceOptions.defaults(),
                        TransactionId.create("books", new JobID().getBytes(), 0, 1),
                        racing)) {
            transaction.open(JdbcWriterState.empty());
            transaction.rollbackPreparedTxs(5);
        }

        assertThat(isPrepared(leftover)).isFalse();
    }

    /** Calls the provider through the handler, which gets the provider to delegate to. */
    private static XaConnectionProvider proxy(
            SimpleXaConnectionProvider delegate, DelegatingHandler handler) {
        InvocationHandler invocationHandler =
                (proxy, method, args) -> {
                    try {
                        return handler.invoke(delegate, method, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                };
        return (XaConnectionProvider)
                Proxy.newProxyInstance(
                        XaTransactionTest.class.getClassLoader(),
                        new Class<?>[] {XaConnectionProvider.class},
                        invocationHandler);
    }

    private interface DelegatingHandler {
        Object invoke(SimpleXaConnectionProvider delegate, Method method, Object[] args)
                throws Throwable;
    }

    private static JdbcWriterState restoredState(TransactionId hanging) {
        return JdbcWriterState.of(Collections.emptyList(), Collections.singletonList(hanging));
    }

    /** Prepares a transaction with one record. */
    private TransactionId prepare(
            byte[] jobId, int subtask, int parallelism, long checkpointId, int id)
            throws Exception {
        return prepare(null, jobId, subtask, parallelism, checkpointId, id);
    }

    private TransactionId prepare(
            @Nullable String prefix,
            byte[] jobId,
            int subtask,
            int parallelism,
            long checkpointId,
            int id)
            throws Exception {
        try (XaTransaction transaction =
                newTransaction(prefix, jobId, subtask, parallelism, JdbcWriterState.empty())) {
            transaction.createTx(checkpointId);
            try (PreparedStatement ps =
                    transaction
                            .getConnectionProvider()
                            .getConnection()
                            .prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                TEST_TABLE
                        .getStatementBuilder()
                        .accept(ps, new BooksTable.BookEntry(id, "title", "author", 1D, 1));
                ps.execute();
            }
            return transaction.prepareTx().get();
        }
    }

    private void rollbackPreparedTxs(byte[] jobId, int subtask, int parallelism, long checkpointId)
            throws Exception {
        rollbackPreparedTxs(jobId, subtask, parallelism, checkpointId, JdbcWriterState.empty());
    }

    private void rollbackPreparedTxs(
            byte[] jobId, int subtask, int parallelism, long checkpointId, JdbcWriterState state)
            throws Exception {
        rollbackPreparedTxs(null, jobId, subtask, parallelism, checkpointId, state);
    }

    private void rollbackPreparedTxs(
            @Nullable String prefix,
            byte[] jobId,
            int subtask,
            int parallelism,
            long checkpointId,
            JdbcWriterState state)
            throws Exception {
        try (XaTransaction transaction =
                newTransaction(prefix, jobId, subtask, parallelism, state)) {
            transaction.rollbackPreparedTxs(checkpointId);
        }
    }

    private XaTransaction newTransaction(
            @Nullable String prefix,
            byte[] jobId,
            int subtask,
            int parallelism,
            JdbcWriterState state)
            throws Exception {
        XaTransaction transaction =
                new XaTransaction(
                        JdbcExactlyOnceOptions.defaults(),
                        TransactionId.create(prefix, jobId, subtask, parallelism),
                        newXaConnectionProvider());
        transaction.open(state);
        return transaction;
    }

    private boolean isPrepared(TransactionId transactionId) throws Exception {
        try (SimpleXaConnectionProvider xa = newXaConnectionProvider()) {
            xa.open();
            return xa.recover().stream()
                    .anyMatch(
                            xid ->
                                    Arrays.equals(
                                                    xid.getGlobalTransactionId(),
                                                    transactionId.getGlobalTransactionId())
                                            && Arrays.equals(
                                                    xid.getBranchQualifier(),
                                                    transactionId.getBranchQualifier()));
        }
    }

    private SimpleXaConnectionProvider newXaConnectionProvider() {
        return SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource());
    }
}
