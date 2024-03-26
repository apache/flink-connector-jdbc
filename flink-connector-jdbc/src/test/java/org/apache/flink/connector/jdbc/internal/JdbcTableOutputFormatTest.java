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

package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link JdbcOutputFormat}. */
public class JdbcTableOutputFormatTest extends JdbcDataTestBase {

    private TableJdbcUpsertOutputFormat format;
    private String[] fieldNames;
    private String[] keyFields;

    @BeforeEach
    void setup() {
        fieldNames = new String[] {"id", "title", "author", "price", "qty"};
        keyFields = new String[] {"id"};
    }

    @Test
    void testUpsertFormatCloseBeforeOpen() throws Exception {
        InternalJdbcConnectionOptions options =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setTableName(OUTPUT_TABLE)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(fieldNames)
                        .withKeyFields(keyFields)
                        .build();
        format =
                new TableJdbcUpsertOutputFormat(
                        new SimpleJdbcConnectionProvider(options),
                        dmlOptions,
                        JdbcExecutionOptions.defaults());
        // FLINK-17544: There should be no NPE thrown from this method
        format.close();
    }

    /**
     * Test that the delete executor in {@link TableJdbcUpsertOutputFormat} is updated when {@link
     * JdbcOutputFormat#attemptFlush()} fails.
     */
    @Test
    void testDeleteExecutorUpdatedOnReconnect() throws Exception {
        // first fail flush from the main executor
        boolean[] exceptionThrown = {false};
        // then record whether the delete executor was updated
        // and check it on the next flush attempt
        boolean[] deleteExecutorPrepared = {false};
        boolean[] deleteExecuted = {false};
        format =
                new TableJdbcUpsertOutputFormat(
                        new SimpleJdbcConnectionProvider(
                                InternalJdbcConnectionOptions.builder()
                                        .setDBUrl(getMetadata().getJdbcUrl())
                                        .setTableName(OUTPUT_TABLE)
                                        .build()) {
                            @Override
                            public boolean isConnectionValid() throws SQLException {
                                return false; // trigger reconnect and re-prepare on flush failure
                            }
                        },
                        JdbcExecutionOptions.builder()
                                .withMaxRetries(1)
                                .withBatchIntervalMs(Long.MAX_VALUE) // disable periodic flush
                                .build(),
                        () ->
                                new JdbcBatchStatementExecutor<Row>() {

                                    @Override
                                    public void executeBatch() throws SQLException {
                                        if (!exceptionThrown[0]) {
                                            exceptionThrown[0] = true;
                                            throw new SQLException();
                                        }
                                    }

                                    @Override
                                    public void prepareStatements(Connection connection) {}

                                    @Override
                                    public void addToBatch(Row record) {}

                                    @Override
                                    public void closeStatements() {}
                                },
                        () ->
                                new JdbcBatchStatementExecutor<Row>() {
                                    @Override
                                    public void prepareStatements(Connection connection) {
                                        if (exceptionThrown[0]) {
                                            deleteExecutorPrepared[0] = true;
                                        }
                                    }

                                    @Override
                                    public void addToBatch(Row record) {}

                                    @Override
                                    public void executeBatch() {
                                        deleteExecuted[0] = true;
                                    }

                                    @Override
                                    public void closeStatements() {}
                                });

        JdbcOutputSerializer<Row> serializer =
                JdbcOutputSerializer.of(getSerializer(TypeInformation.of(Row.class), true));
        format.open(serializer);

        format.writeRecord(toRowDelete(TEST_DATA[0]));
        format.flush();

        assertThat(deleteExecuted[0]).as("Delete should be executed").isTrue();
        assertThat(deleteExecutorPrepared[0])
                .as("Delete executor should be prepared" + exceptionThrown[0])
                .isTrue();
    }

    @Test
    void testJdbcOutputFormat() throws Exception {
        InternalJdbcConnectionOptions options =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setTableName(OUTPUT_TABLE)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(fieldNames)
                        .withKeyFields(keyFields)
                        .build();
        format =
                new TableJdbcUpsertOutputFormat(
                        new SimpleJdbcConnectionProvider(options),
                        dmlOptions,
                        JdbcExecutionOptions.defaults());

        JdbcOutputSerializer<Row> serializer =
                JdbcOutputSerializer.of(getSerializer(TypeInformation.of(Row.class), true));
        format.open(serializer);

        for (TestEntry entry : TEST_DATA) {
            format.writeRecord(toRow(entry));
        }
        format.flush();
        check(Arrays.stream(TEST_DATA).map(JdbcDataTestBase::toRow).toArray(Row[]::new));

        // override
        for (TestEntry entry : TEST_DATA) {
            format.writeRecord(toRow(entry));
        }
        format.flush();
        check(Arrays.stream(TEST_DATA).map(JdbcDataTestBase::toRow).toArray(Row[]::new));

        // delete
        for (int i = 0; i < TEST_DATA.length / 2; i++) {
            format.writeRecord(toRowDelete(TEST_DATA[i]));
        }
        Row[] expected = new Row[TEST_DATA.length - TEST_DATA.length / 2];
        for (int i = TEST_DATA.length / 2; i < TEST_DATA.length; i++) {
            expected[i - TEST_DATA.length / 2] = toRow(TEST_DATA[i]);
        }
        format.flush();
        check(expected);
    }

    private void check(Row[] rows) throws SQLException {
        check(rows, getMetadata().getJdbcUrl(), OUTPUT_TABLE, fieldNames);
    }

    public static void check(Row[] rows, String url, String table, String[] fields)
            throws SQLException {
        Connection conn = DriverManager.getConnection(url);
        check(rows, conn, table, fields);
        conn.close();
    }

    public static void check(Row[] rows, Connection conn, String table, String[] fields)
            throws SQLException {
        try (Connection dbConn = conn;
                PreparedStatement statement = dbConn.prepareStatement("select * from " + table);
                ResultSet resultSet = statement.executeQuery()) {
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                Row row = new Row(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    row.setField(i, resultSet.getObject(fields[i]));
                }
                results.add(row.toString());
            }
            String[] sortedExpect = Arrays.stream(rows).map(Row::toString).toArray(String[]::new);
            String[] sortedResult = results.toArray(new String[0]);
            Arrays.sort(sortedExpect);
            Arrays.sort(sortedResult);
            assertThat(sortedResult).isEqualTo(sortedExpect);
        }
    }

    @AfterEach
    void clearOutputTable() throws Exception {
        if (format != null) {
            format.close();
        }
        format = null;

        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + OUTPUT_TABLE);
        }
    }
}
