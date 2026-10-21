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

package org.apache.flink.connector.jdbc.core.datastream.sink.writer;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.committer.JdbcCommitable;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.SimpleXaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.SimpleJdbcQueryStatement;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.testutils.TestingInitContext;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Smoke tests for the {@link JdbcWriter} with ExactlyOnce semantics and the underlying classes. */
class ExactlyOnceJdbcWriterTest extends BaseJdbcWriterTest {

    private static final List<BooksTable.BookEntry> MORE_BOOKS =
            Arrays.asList(
                    new BooksTable.BookEntry(2001, "Restored one", "Writer", 1D, 1),
                    new BooksTable.BookEntry(2002, "Restored two", "Writer", 2D, 2));

    @Override
    protected JdbcExecutionOptions getExecutionOptions() {
        return JdbcExecutionOptions.builder().withMaxRetries(0).build();
    }

    @Override
    protected JdbcExactlyOnceOptions getExactlyOnceOptions() {
        return JdbcExactlyOnceOptions.defaults();
    }

    @Override
    protected DeliveryGuarantee getDeliveryGuarantee() {
        return DeliveryGuarantee.EXACTLY_ONCE;
    }

    @Override
    protected JdbcConnectionProvider getConnectionProvider() {
        return SimpleXaConnectionProvider.from(
                () -> getMetadata().buildXaDataSource(), getExactlyOnceOptions().getTimeoutSec());
    }

    /**
     * A failed test must not leave a prepared branch behind: Derby keeps its row locks and the
     * table cleanup would wait on them.
     */
    @AfterEach
    void rollbackPreparedTransactions() throws Exception {
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            for (Xid xid : xa.recover()) {
                xa.rollback(xid);
            }
        }
    }

    @Test
    void testEmptyCheckpoint() throws Exception {
        checkPreCommitWithSnapshot(1L, false);
    }

    @Test
    void testCheckpoint() throws Exception {
        // add some books
        for (BooksTable.BookEntry book : BOOKS) {
            sinkWriter.write(book, writerContext);
        }

        checkPreCommitWithSnapshot(1L, true);

        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(BOOKS);
    }

    @Test
    void testMultipleCheckpoints() throws Exception {
        List<BooksTable.BookEntry> booksHalf1 = BOOKS.subList(0, BOOKS.size() / 2);
        List<BooksTable.BookEntry> booksHalf2 = BOOKS.subList(BOOKS.size() / 2, BOOKS.size());
        // check that all is empty
        checkPreCommitWithSnapshot(1L, false);

        // add some books
        for (BooksTable.BookEntry book : booksHalf1) {
            sinkWriter.write(book, writerContext);
        }
        checkPreCommitWithSnapshot(2L, true);

        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(booksHalf1);

        // add more books
        for (BooksTable.BookEntry book : booksHalf2) {
            sinkWriter.write(book, writerContext);
        }
        checkPreCommitWithSnapshot(3L, true);

        // empty transaction
        checkPreCommitWithSnapshot(4L, false);

        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(BOOKS);
    }

    /**
     * The restore path {@code JdbcSink.restoreWriter} takes: the snapshot of checkpoint 1 as the
     * state serializer hands it back, and the context of a writer restored from checkpoint 1.
     */
    @Test
    void testRestoreFromSnapshot() throws Exception {
        for (BooksTable.BookEntry book : BOOKS) {
            sinkWriter.write(book, writerContext);
        }
        JdbcWriterState snapshot = checkPreCommitWithSnapshot(1L, true);
        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(BOOKS);
        JdbcWriterStateSerializer serializer = new JdbcWriterStateSerializer();
        JdbcWriterState recovered =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(snapshot));

        // a row in the branch of checkpoint 1 that never gets prepared; the writer goes down
        // without a snapshot, and only its close() lets Derby release the branch
        sinkWriter.write(MORE_BOOKS.get(0), writerContext);
        sinkWriter.flush(true);
        sinkWriter.close();
        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(BOOKS);

        sinkWriter =
                newWriter(
                        Collections.singletonList(recovered),
                        initContext.withRestoredCheckpointId(1L));

        // the hanging branch of checkpoint 1 was rolled back on recovery, and the restored
        // checkpoint id is the writer's last one
        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(BOOKS);
        assertThatThrownBy(() -> sinkWriter.snapshotState(1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Expected 1 > 1");

        for (BooksTable.BookEntry book : MORE_BOOKS) {
            sinkWriter.write(book, writerContext);
        }
        // the committable carries the re-created branch of checkpoint 1, and the snapshot
        // holds that one hanging branch for checkpoint 2 only
        checkPreCommitWithSnapshot(2L, true);

        List<BooksTable.BookEntry> all = new ArrayList<>(BOOKS);
        all.addAll(MORE_BOOKS);
        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(all);
    }

    /**
     * A branch that was prepared but whose commit never happened before the job went down is
     * committed when the writer restores the state that lists it.
     */
    @Test
    void testRestoreCommitsPreparedTransaction() throws Exception {
        // the writer from init() holds branch 0 of this job; Derby refuses a second start of it
        sinkWriter.close();
        byte[] jobId = initContext.getJobInfo().getJobId().getBytes();
        TransactionId prepared = TransactionId.create(jobId, 1, 4).withBranch(0L);
        List<BooksTable.BookEntry> half = BOOKS.subList(0, BOOKS.size() / 2);
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            xa.start(prepared);
            try (PreparedStatement ps =
                    xa.getConnection().prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                for (BooksTable.BookEntry book : half) {
                    TEST_TABLE.getStatementBuilder().accept(ps, book);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            xa.endAndPrepare(prepared);
        }
        assertThat(inDoubt()).hasSize(1);
        JdbcWriterState state =
                JdbcWriterState.of(
                        Collections.singletonList(TransactionId.restore(jobId, 1, 4, 0L, 0)),
                        Collections.emptyList());

        sinkWriter =
                newWriter(
                        Collections.singletonList(state), initContext.withRestoredCheckpointId(1L));

        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(half);
        assertThat(inDoubt()).isEmpty();
        checkPreCommitWithSnapshot(2L, false);
    }

    /** A writer whose format fails to open leaves neither an XA connection nor a branch behind. */
    @Test
    void testFailedOpenClosesTransaction() throws Exception {
        SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(() -> getMetadata().buildXaDataSource(), null);
        TestingInitContext otherJob = new TestingInitContext(bookSerializer());

        assertThatThrownBy(
                        () ->
                                new JdbcWriter<>(
                                        xa,
                                        getExecutionOptions(),
                                        getExactlyOnceOptions(),
                                        new SimpleJdbcQueryStatement<>(
                                                "INSERT INTO NO_SUCH_TABLE (id) VALUES (?)",
                                                (ps, book) -> {}),
                                        outputSerializer,
                                        DeliveryGuarantee.EXACTLY_ONCE,
                                        Collections.emptyList(),
                                        otherJob))
                .isInstanceOf(IOException.class)
                .hasMessage("unable to open JDBC writer");

        assertThat(xa.isOpen()).isFalse();
        // the branch was rolled back, not left associated: a fresh resource can start it again
        TransactionId branch0 =
                TransactionId.create(otherJob.getJobInfo().getJobId().getBytes(), 0, 1)
                        .withBranch(0L);
        try (SimpleXaConnectionProvider check =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            check.open();
            assertThat(check.recover()).isEmpty();
            check.start(branch0);
            check.failAndRollback(branch0);
        }
    }

    /** The prepared branches the resource manager knows, seen from a second connection. */
    private Collection<Xid> inDoubt() throws Exception {
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            return xa.recover();
        }
    }

    protected JdbcWriterState checkPreCommitWithSnapshot(long checkpointId, boolean hasCommit)
            throws Exception {
        sinkWriter.flush(false);
        Collection<JdbcCommitable> commitables = sinkWriter.prepareCommit();
        if (hasCommit) {
            assertThat(commitables.size()).isEqualTo(1);
            checkCommitable(commitables.iterator().next(), withBranch(checkpointId - 1));
        } else {
            assertThat(commitables.size()).isEqualTo(0);
        }

        List<JdbcWriterState> snapshot = sinkWriter.snapshotState(checkpointId);
        assertThat(snapshot).hasSize(1);
        checkSnapshot(
                snapshot.get(0),
                Collections.emptyList(),
                Collections.singletonList(withBranch(checkpointId)));
        return snapshot.get(0);
    }
}
