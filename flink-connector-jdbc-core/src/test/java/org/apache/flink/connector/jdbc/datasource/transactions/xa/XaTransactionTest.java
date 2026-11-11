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
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    /**
     * A writer that can't list the prepared transactions fails instead of leaving a leftover that
     * holds its locks.
     */
    @Test
    void testRollbackPreparedTxsFailsIfItCantList() throws Exception {
        try (XaTransaction transaction =
                new XaTransaction(
                        JdbcExactlyOnceOptions.builder().withRecoveredAndRollback(false).build(),
                        TransactionId.create(new JobID().getBytes(), 0, 1),
                        failingRecover(newXaConnectionProvider()))) {
            transaction.open(JdbcWriterState.empty());

            assertThatThrownBy(() -> transaction.rollbackPreparedTxs(5))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessageContaining("XA_RECOVER_ADMIN");
        }
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

    /**
     * At another parallelism the writer only rolls back the transaction that the restored state
     * names, not those that the failed attempt prepared for later checkpoints.
     */
    @Test
    void testOpenAtOtherParallelismKeepsLaterLeftovers() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId hanging = prepare(jobId, 0, 1, 5, 1);
        TransactionId later = prepare(jobId, 0, 1, 6, 2);

        rollbackPreparedTxs(jobId, 0, 2, 5, restoredState(hanging));

        assertThat(isPrepared(hanging)).isFalse();
        assertThat(isPrepared(later)).isTrue();
    }

    private static JdbcWriterState restoredState(TransactionId hanging) {
        return JdbcWriterState.of(Collections.emptyList(), Collections.singletonList(hanging));
    }

    /** Prepares a transaction with one record. */
    private TransactionId prepare(
            byte[] jobId, int subtask, int parallelism, long checkpointId, int id)
            throws Exception {
        try (XaTransaction transaction =
                newTransaction(jobId, subtask, parallelism, JdbcWriterState.empty())) {
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
        try (XaTransaction transaction = newTransaction(jobId, subtask, parallelism, state)) {
            transaction.rollbackPreparedTxs(checkpointId);
        }
    }

    private XaTransaction newTransaction(
            byte[] jobId, int subtask, int parallelism, JdbcWriterState state) throws Exception {
        XaTransaction transaction =
                new XaTransaction(
                        JdbcExactlyOnceOptions.defaults(),
                        TransactionId.create(jobId, subtask, parallelism),
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

    private static XaConnectionProvider failingRecover(SimpleXaConnectionProvider delegate) {
        return (XaConnectionProvider)
                Proxy.newProxyInstance(
                        XaTransactionTest.class.getClassLoader(),
                        new Class<?>[] {XaConnectionProvider.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("recover")) {
                                throw new FlinkRuntimeException("XAER_RMERR");
                            }
                            try {
                                return method.invoke(delegate, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
    }
}
