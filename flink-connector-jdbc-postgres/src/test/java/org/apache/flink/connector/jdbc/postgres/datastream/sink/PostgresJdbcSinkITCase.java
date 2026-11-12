/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.postgres.datastream.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.jdbc.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.assertj.core.api.Assertions.assertThat;

/** The {@link JdbcSink} ITCase for PostgreSQL, at-least-once and exactly-once. */
class PostgresJdbcSinkITCase implements PostgresTestBase, JdbcITCaseBase {

    private static final BooksTable TEST_TABLE = new BooksTable("SinkTable");

    private static final List<BooksTable.BookEntry> BOOKS =
            Arrays.stream(TEST_DATA)
                    .map(
                            book ->
                                    new BooksTable.BookEntry(
                                            book.id, book.title, book.author, book.price, book.qty))
                    .collect(Collectors.toList());

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TEST_TABLE);
    }

    @Test
    void testAtLeastOnce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);

        assertResult(Collections.emptyList());

        env.fromCollection(BOOKS)
                .sinkTo(
                        JdbcSink.<BooksTable.BookEntry>builder()
                                .withQueryStatement(
                                        TEST_TABLE.getInsertIntoQuery(),
                                        TEST_TABLE.getStatementBuilder())
                                .buildAtLeastOnce(
                                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                                .withUrl(getMetadata().getJdbcUrl())
                                                .withUsername(getMetadata().getUsername())
                                                .withPassword(getMetadata().getPassword())
                                                .withDriverName(getMetadata().getDriverClass())
                                                .build()));
        env.execute();

        assertResult(BOOKS);
    }

    @Test
    void testExactlyOnce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);

        assertResult(Collections.emptyList());

        env.fromCollection(BOOKS)
                .sinkTo(
                        JdbcSink.<BooksTable.BookEntry>builder()
                                .withQueryStatement(
                                        TEST_TABLE.getInsertIntoQuery(),
                                        TEST_TABLE.getStatementBuilder())
                                .withExecutionOptions(
                                        JdbcExecutionOptions.builder().withMaxRetries(0).build())
                                .buildExactlyOnce(
                                        JdbcExactlyOnceOptions.defaults(),
                                        getMetadata().getXaSourceSupplier()));
        env.execute();

        assertResult(BOOKS);
    }

    private void assertResult(List<BooksTable.BookEntry> expected) throws SQLException {
        try (Connection connection = getMetadata().getConnection()) {
            assertThat(TEST_TABLE.selectAllTable(connection)).isEqualTo(expected);
        }
    }
}
