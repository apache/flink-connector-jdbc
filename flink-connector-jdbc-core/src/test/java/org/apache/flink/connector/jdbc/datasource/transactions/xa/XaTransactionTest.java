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
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

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

    /** Prepares a transaction with one record. */
    private TransactionId prepare(
            byte[] jobId, int subtask, int parallelism, long checkpointId, int id)
            throws Exception {
        try (XaTransaction transaction = newTransaction(jobId, subtask, parallelism)) {
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
        try (XaTransaction transaction = newTransaction(jobId, subtask, parallelism)) {
            transaction.rollbackPreparedTxs(checkpointId);
        }
    }

    private XaTransaction newTransaction(byte[] jobId, int subtask, int parallelism)
            throws Exception {
        XaTransaction transaction =
                new XaTransaction(
                        JdbcExactlyOnceOptions.defaults(),
                        TransactionId.create(jobId, subtask, parallelism),
                        newXaConnectionProvider());
        transaction.open(JdbcWriterState.empty());
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
