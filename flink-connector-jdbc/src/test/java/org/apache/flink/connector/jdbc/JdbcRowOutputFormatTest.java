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

package org.apache.flink.connector.jdbc;

import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE_2;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE_3;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS_2;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS_3;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link JdbcRowOutputFormat}. */
class JdbcRowOutputFormatTest extends JdbcDataTestBase {

    private JdbcRowOutputFormat jdbcOutputFormat;

    @AfterEach
    void tearDown() throws Exception {
        if (jdbcOutputFormat != null) {
            jdbcOutputFormat.close();
        }
        jdbcOutputFormat = null;

        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + OUTPUT_TABLE);
        }
    }

    @Test
    void testInvalidDriver() {
        String expectedMsg = "unable to open JDBC writer";
        try {
            jdbcOutputFormat =
                    JdbcRowOutputFormat.buildJdbcOutputFormat()
                            .setDrivername("org.apache.derby.jdbc.idontexist")
                            .setDBUrl(getMetadata().getJdbcUrl())
                            .setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
                            .finish();
            jdbcOutputFormat.open(0, 1);
        } catch (Exception e) {
            assertThat(findThrowable(e, IOException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    @Test
    void testInvalidURL() {
        String expectedMsg = "No suitable driver found for jdbc:der:iamanerror:mory:ebookshop";

        jdbcOutputFormat =
                JdbcRowOutputFormat.buildJdbcOutputFormat()
                        .setDrivername(getMetadata().getDriverClass())
                        .setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
                        .setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
                        .finish();
        assertThatThrownBy(() -> jdbcOutputFormat.open(0, 1))
                .isInstanceOf(IOException.class)
                .satisfies(anyCauseMatches(SQLException.class, expectedMsg));
    }

    @Test
    void testInvalidQuery() {
        String expectedMsg = "unable to open JDBC writer";
        try {
            jdbcOutputFormat =
                    JdbcRowOutputFormat.buildJdbcOutputFormat()
                            .setDrivername(getMetadata().getDriverClass())
                            .setDBUrl(getMetadata().getJdbcUrl())
                            .setQuery("iamnotsql")
                            .finish();
            setRuntimeContext(jdbcOutputFormat, true);
            jdbcOutputFormat.open(0, 1);
        } catch (Exception e) {
            assertThat(findThrowable(e, IOException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    @Test
    void testIncompleteConfiguration() {
        String expectedMsg = "jdbc url is empty";
        try {
            jdbcOutputFormat =
                    JdbcRowOutputFormat.buildJdbcOutputFormat()
                            .setDrivername(getMetadata().getDriverClass())
                            .setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
                            .finish();
        } catch (Exception e) {
            assertThat(findThrowable(e, NullPointerException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    @Test
    void testIncompatibleTypes() {
        String expectedMsg = "Invalid character string format for type INTEGER.";
        try {
            jdbcOutputFormat =
                    JdbcRowOutputFormat.buildJdbcOutputFormat()
                            .setDrivername(getMetadata().getDriverClass())
                            .setDBUrl(getMetadata().getJdbcUrl())
                            .setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
                            .finish();
            setRuntimeContext(jdbcOutputFormat, true);
            jdbcOutputFormat.open(0, 1);

            Row row = new Row(5);
            row.setField(0, 4);
            row.setField(1, "hello");
            row.setField(2, "world");
            row.setField(3, 0.99);
            row.setField(4, "imthewrongtype");

            jdbcOutputFormat.writeRecord(row);
            jdbcOutputFormat.close();
        } catch (Exception e) {
            assertThat(findThrowable(e, SQLDataException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    @Test
    void testExceptionOnInvalidType() {
        String expectedMsg = "field index: 3, field value: 0.";
        try {
            jdbcOutputFormat =
                    JdbcRowOutputFormat.buildJdbcOutputFormat()
                            .setDrivername(getMetadata().getDriverClass())
                            .setDBUrl(getMetadata().getJdbcUrl())
                            .setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
                            .setSqlTypes(
                                    new int[] {
                                        Types.INTEGER,
                                        Types.VARCHAR,
                                        Types.VARCHAR,
                                        Types.DOUBLE,
                                        Types.INTEGER
                                    })
                            .finish();
            setRuntimeContext(jdbcOutputFormat, true);
            jdbcOutputFormat.open(0, 1);

            TestEntry entry = TEST_DATA[0];
            Row row = new Row(5);
            row.setField(0, entry.id);
            row.setField(1, entry.title);
            row.setField(2, entry.author);
            row.setField(3, 0L); // use incompatible type (Long instead of Double)
            row.setField(4, entry.qty);
            jdbcOutputFormat.writeRecord(row);
            jdbcOutputFormat.close();
        } catch (Exception e) {
            assertThat(findThrowable(e, ClassCastException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    @Test
    void testExceptionOnClose() {
        String expectedMsg = "Writing records to JDBC failed.";
        try {
            jdbcOutputFormat =
                    JdbcRowOutputFormat.buildJdbcOutputFormat()
                            .setDrivername(getMetadata().getDriverClass())
                            .setDBUrl(getMetadata().getJdbcUrl())
                            .setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
                            .setSqlTypes(
                                    new int[] {
                                        Types.INTEGER,
                                        Types.VARCHAR,
                                        Types.VARCHAR,
                                        Types.DOUBLE,
                                        Types.INTEGER
                                    })
                            .finish();
            setRuntimeContext(jdbcOutputFormat, true);
            jdbcOutputFormat.open(0, 1);

            TestEntry entry = TEST_DATA[0];
            Row row = new Row(5);
            row.setField(0, entry.id);
            row.setField(1, entry.title);
            row.setField(2, entry.author);
            row.setField(3, entry.price);
            row.setField(4, entry.qty);
            jdbcOutputFormat.writeRecord(row);
            jdbcOutputFormat.writeRecord(
                    row); // writing the same record twice must yield a unique key violation.

            jdbcOutputFormat.close();
        } catch (Exception e) {
            assertThat(findThrowable(e, RuntimeException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    @Test
    void testJdbcOutputFormat() throws IOException, SQLException {
        jdbcOutputFormat =
                JdbcRowOutputFormat.buildJdbcOutputFormat()
                        .setDrivername(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
                        .finish();
        setRuntimeContext(jdbcOutputFormat, true);
        jdbcOutputFormat.open(0, 1);

        for (TestEntry entry : TEST_DATA) {
            jdbcOutputFormat.writeRecord(toRow(entry));
        }

        jdbcOutputFormat.close();

        try (Connection dbConn = DriverManager.getConnection(getMetadata().getJdbcUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS);
                ResultSet resultSet = statement.executeQuery()) {
            int recordCount = 0;
            while (resultSet.next()) {
                assertThat(resultSet.getObject("id")).isEqualTo(TEST_DATA[recordCount].id);
                assertThat(resultSet.getObject("title")).isEqualTo(TEST_DATA[recordCount].title);
                assertThat(resultSet.getObject("author")).isEqualTo(TEST_DATA[recordCount].author);
                assertThat(resultSet.getObject("price")).isEqualTo(TEST_DATA[recordCount].price);
                assertThat(resultSet.getObject("qty")).isEqualTo(TEST_DATA[recordCount].qty);

                recordCount++;
            }
            assertThat(recordCount).isEqualTo(TEST_DATA.length);
        }
    }

    @Test
    void testFlush() throws SQLException, IOException {
        jdbcOutputFormat =
                JdbcRowOutputFormat.buildJdbcOutputFormat()
                        .setDrivername(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE_2))
                        .setBatchSize(3)
                        .finish();
        setRuntimeContext(jdbcOutputFormat, true);
        try (Connection dbConn = DriverManager.getConnection(getMetadata().getJdbcUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_2)) {
            jdbcOutputFormat.open(0, 1);
            for (int i = 0; i < 2; ++i) {
                jdbcOutputFormat.writeRecord(toRow(TEST_DATA[i]));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isFalse();
            }
            jdbcOutputFormat.writeRecord(toRow(TEST_DATA[2]));
            try (ResultSet resultSet = statement.executeQuery()) {
                int recordCount = 0;
                while (resultSet.next()) {
                    assertThat(resultSet.getObject("id")).isEqualTo(TEST_DATA[recordCount].id);
                    assertThat(resultSet.getObject("title"))
                            .isEqualTo(TEST_DATA[recordCount].title);
                    assertThat(resultSet.getObject("author"))
                            .isEqualTo(TEST_DATA[recordCount].author);
                    assertThat(resultSet.getObject("price"))
                            .isEqualTo(TEST_DATA[recordCount].price);
                    assertThat(resultSet.getObject("qty")).isEqualTo(TEST_DATA[recordCount].qty);
                    recordCount++;
                }
                assertThat(recordCount).isEqualTo(3);
            }
        } finally {
            jdbcOutputFormat.close();
        }
    }

    @Test
    void testInvalidConnectionInJdbcOutputFormat() throws IOException, SQLException {
        jdbcOutputFormat =
                JdbcRowOutputFormat.buildJdbcOutputFormat()
                        .setDrivername(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE_3))
                        .finish();
        setRuntimeContext(jdbcOutputFormat, true);
        jdbcOutputFormat.open(0, 1);

        // write records
        for (int i = 0; i < 3; i++) {
            jdbcOutputFormat.writeRecord(toRow(TEST_DATA[i]));
        }

        // close connection
        jdbcOutputFormat.getConnection().close();

        for (int i = 3; i < TEST_DATA.length; i++) {
            jdbcOutputFormat.writeRecord(toRow(TEST_DATA[i]));
        }

        jdbcOutputFormat.close();

        try (Connection dbConn = DriverManager.getConnection(getMetadata().getJdbcUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_3);
                ResultSet resultSet = statement.executeQuery()) {
            int recordCount = 0;
            while (resultSet.next()) {
                assertThat(resultSet.getObject("id")).isEqualTo(TEST_DATA[recordCount].id);
                assertThat(resultSet.getObject("title")).isEqualTo(TEST_DATA[recordCount].title);
                assertThat(resultSet.getObject("author")).isEqualTo(TEST_DATA[recordCount].author);
                assertThat(resultSet.getObject("price")).isEqualTo(TEST_DATA[recordCount].price);
                assertThat(resultSet.getObject("qty")).isEqualTo(TEST_DATA[recordCount].qty);

                recordCount++;
            }
            assertThat(recordCount).isEqualTo(TEST_DATA.length);
        }
    }
}
