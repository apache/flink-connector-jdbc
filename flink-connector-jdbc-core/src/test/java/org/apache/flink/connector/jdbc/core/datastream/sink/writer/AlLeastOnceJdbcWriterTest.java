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
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.SimpleJdbcQueryStatement;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Smoke tests for the {@link JdbcWriter} with AtLeastOnce semantics and the underlying classes. */
class AlLeastOnceJdbcWriterTest extends BaseJdbcWriterTest {

    private static final String INSERT_INTO_MISSING_TABLE =
            "INSERT INTO NO_SUCH_TABLE (id) VALUES (?)";

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

    @Test
    void testPrebuiltOutputFormat() throws Exception {
        JdbcWriter<BooksTable.BookEntry> writer =
                new JdbcWriter<>(
                        insertFormat(getConnectionProvider(), TEST_TABLE.getInsertIntoQuery()),
                        outputSerializer,
                        initContext);
        try {
            for (BooksTable.BookEntry book : BOOKS) {
                writer.write(book, writerContext);
            }
            writer.flush(false);

            assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(BOOKS);
        } finally {
            writer.close();
        }
    }

    /** Nobody closes a writer whose constructor threw, so the constructor has to clean up. */
    @Test
    void testFailedOpenClosesConnection() {
        SimpleJdbcConnectionProvider provider =
                new SimpleJdbcConnectionProvider(getMetadata().getConnectionOptions());

        assertThatThrownBy(
                        () ->
                                new JdbcWriter<>(
                                        insertFormat(provider, INSERT_INTO_MISSING_TABLE),
                                        outputSerializer,
                                        initContext))
                .isInstanceOf(IOException.class)
                .hasMessage("unable to open JDBC writer")
                .hasCauseInstanceOf(SQLException.class);

        assertThat(provider.getConnection()).isNull();
    }

    @Test
    void testFailedOpenClosesConnectionOfTheDataStreamWriter() {
        SimpleJdbcConnectionProvider provider =
                new SimpleJdbcConnectionProvider(getMetadata().getConnectionOptions());

        assertThatThrownBy(
                        () ->
                                new JdbcWriter<>(
                                        provider,
                                        getExecutionOptions(),
                                        getExactlyOnceOptions(),
                                        new SimpleJdbcQueryStatement<>(
                                                INSERT_INTO_MISSING_TABLE, (ps, book) -> {}),
                                        outputSerializer,
                                        DeliveryGuarantee.AT_LEAST_ONCE,
                                        Collections.emptyList(),
                                        initContext))
                .isInstanceOf(IOException.class)
                .hasMessage("unable to open JDBC writer");

        assertThat(provider.getConnection()).isNull();
    }

    private JdbcOutputFormat<BooksTable.BookEntry, ?, ?> insertFormat(
            JdbcConnectionProvider provider, String insert) {
        return new JdbcOutputFormat<>(
                provider,
                getExecutionOptions(),
                () -> JdbcBatchStatementExecutor.simple(insert, TEST_TABLE.getStatementBuilder()));
    }
}
