package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.SimpleXaConnectionProvider;
import org.apache.flink.connector.jdbc.sink.committer.JdbcCommitable;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

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

    protected void checkPreCommitWithSnapshot(long checkpointId, boolean hasCommit)
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
    }
}
