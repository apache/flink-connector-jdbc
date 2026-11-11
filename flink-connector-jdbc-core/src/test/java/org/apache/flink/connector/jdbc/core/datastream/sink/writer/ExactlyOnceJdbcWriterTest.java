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
import org.apache.flink.connector.jdbc.core.datastream.sink.committer.JdbcCommitableSerializer;
import org.apache.flink.connector.jdbc.core.datastream.sink.committer.JdbcCommitter;
import org.apache.flink.connector.jdbc.core.datastream.sink.committer.TestingCommitRequest;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.SimpleXaConnectionProvider;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Smoke tests for the {@link JdbcWriter} with ExactlyOnce semantics and the underlying classes. */
class ExactlyOnceJdbcWriterTest extends BaseJdbcWriterTest {

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

    /** A failed test can leave a prepared transaction behind, which holds its locks. */
    @AfterEach
    void rollbackPreparedTransactions() throws Exception {
        sinkWriter.close();
        try (SimpleXaConnectionProvider xa = newXaConnectionProvider()) {
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

        JdbcCommitable committable = checkPreCommitWithSnapshot(1L, true);
        sinkWriter.close();

        // the transaction waits for the committer
        assertThat(recover()).contains(committable.getXid());

        commit(committable);

        assertThat(recover()).isEmpty();
        assertThat(TEST_TABLE.selectAllTable(getMetadata())).isEqualTo(BOOKS);
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
        JdbcCommitable committable2 = checkPreCommitWithSnapshot(2L, true);

        // add more books
        for (BooksTable.BookEntry book : booksHalf2) {
            sinkWriter.write(book, writerContext);
        }
        JdbcCommitable committable3 = checkPreCommitWithSnapshot(3L, true);

        // empty transaction
        checkPreCommitWithSnapshot(4L, false);
        sinkWriter.close();

        // the transactions wait for the committer
        assertThat(recover()).contains(committable2.getXid(), committable3.getXid());

        commit(committable2);
        commit(committable3);

        assertThat(recover()).isEmpty();
        assertThat(TEST_TABLE.selectAllTable(getMetadata())).isEqualTo(BOOKS);
    }

    /** Checkpoints can still come after the end of input, until the task finishes. */
    @Test
    void testCheckpointsAfterEndOfInput() throws Exception {
        for (BooksTable.BookEntry book : BOOKS) {
            sinkWriter.write(book, writerContext);
        }
        sinkWriter.flush(true);
        assertThat(sinkWriter.prepareCommit()).hasSize(1);

        sinkWriter.snapshotState(1L);
        assertThat(sinkWriter.snapshotState(2L).get(0).getHanging()).isEmpty();
    }

    /** Commits the committable the way the committer gets it when it is not chained. */
    private void commit(JdbcCommitable committable) throws Exception {
        JdbcCommitableSerializer serializer = new JdbcCommitableSerializer();
        TestingCommitRequest request =
                new TestingCommitRequest(
                        serializer.deserialize(
                                serializer.getVersion(), serializer.serialize(committable)));
        try (JdbcCommitter committer =
                new JdbcCommitter(
                        getDeliveryGuarantee(),
                        newXaConnectionProvider(),
                        getExactlyOnceOptions(),
                        OptionalLong.empty())) {
            committer.commit(Collections.singletonList(request));
        }
        assertThat(request.getFailure()).isNull();
    }

    private Collection<Xid> recover() throws Exception {
        try (SimpleXaConnectionProvider xa = newXaConnectionProvider()) {
            xa.open();
            return xa.recover();
        }
    }

    private SimpleXaConnectionProvider newXaConnectionProvider() {
        return SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource());
    }

    protected JdbcCommitable checkPreCommitWithSnapshot(long checkpointId, boolean hasCommit)
            throws Exception {
        sinkWriter.flush(false);
        Collection<JdbcCommitable> commitables = sinkWriter.prepareCommit();
        JdbcCommitable commitable = null;
        if (hasCommit) {
            assertThat(commitables.size()).isEqualTo(1);
            commitable = commitables.iterator().next();
            checkCommitable(commitable, withBranch(checkpointId - 1));
        } else {
            assertThat(commitables.size()).isEqualTo(0);
        }

        List<JdbcWriterState> snapshot = sinkWriter.snapshotState(checkpointId);
        assertThat(snapshot).hasSize(1);
        checkSnapshot(
                snapshot.get(0),
                Collections.emptyList(),
                Collections.singletonList(withBranch(checkpointId)));
        return commitable;
    }
}
