package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.sink.committer.JdbcCommitable;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Smoke tests for the {@link JdbcWriter} with AtLeastOnce semantics and the underlying classes. */
class AlLeastOnceJdbcWriterTest extends BaseJdbcWriterTest {

    @Override
    protected JdbcExecutionOptions getExecutionOptions() {
        return JdbcExecutionOptions.defaults();
    }

    @Override
    protected JdbcExactlyOnceOptions getExactlyOnceOptions() {
        return JdbcExactlyOnceOptions.defaults();
    }

    @Override
    protected DeliveryGuarantee getDeliveryGuarantee() {
        return DeliveryGuarantee.AT_LEAST_ONCE;
    }

    @Override
    protected JdbcConnectionProvider getConnectionProvider() {
        return new SimpleJdbcConnectionProvider(getMetadata().getConnectionOptions());
    }

    @Test
    void testCheckpoint() throws Exception {
        Collection<JdbcCommitable> commitables = sinkWriter.prepareCommit();
        assertThat(commitables.size()).isEqualTo(0);

        List<JdbcWriterState> snapshot = sinkWriter.snapshotState(1L);
        assertThat(snapshot).hasSize(0);
    }

    @Test
    void testFlush() throws Exception {
        // add some books
        for (BooksTable.BookEntry book : BOOKS) {
            sinkWriter.write(book, writerContext);
        }

        sinkWriter.flush(false);

        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(BOOKS);
    }

    @Test
    void testMultipleFlush() throws Exception {
        List<BooksTable.BookEntry> booksHalf1 = BOOKS.subList(0, BOOKS.size() / 2);
        List<BooksTable.BookEntry> booksHalf2 = BOOKS.subList(BOOKS.size() / 2, BOOKS.size());
        // check that all is empty
        sinkWriter.flush(false);

        // add some books
        for (BooksTable.BookEntry book : booksHalf1) {
            sinkWriter.write(book, writerContext);
        }
        sinkWriter.flush(false);

        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(booksHalf1);

        // add more books
        for (BooksTable.BookEntry book : booksHalf2) {
            sinkWriter.write(book, writerContext);
        }
        sinkWriter.flush(false);

        // empty transaction
        sinkWriter.flush(false);

        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(BOOKS);
    }
}
