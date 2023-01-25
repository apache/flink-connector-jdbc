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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.JdbcBookStoreTestBase;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.templates.BooksTable.BookEntry;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test suite for {@link JdbcOutputFormatBuilder}. */
class JdbcOutputFormatTest extends JdbcBookStoreTestBase {

    private static JdbcOutputFormat<RowData, ?, ?> outputFormat;
    private static final String[] fieldNames = BOOKS_TABLE.getTableFields();
    private static final DataType[] fieldDataTypes = BOOKS_TABLE.getTableDataTypes();
    private static final RowType rowType = BOOKS_TABLE.getTableRowType();
    private static final InternalTypeInfo<RowData> rowDataTypeInfo = InternalTypeInfo.of(rowType);

    @AfterEach
    void tearDown() {
        if (outputFormat != null) {
            outputFormat.close();
        }
        outputFormat = null;
    }

    @Test
    void testInvalidDriver() {
        String expectedMsg = "unable to open JDBC writer";
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDriverName("org.apache.derby.jdbc.idontexist")
                                            .setDBUrl(getDbMetadata().getUrl())
                                            .setTableName(BOOKS_TABLE.getTableName())
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            outputFormat =
                                    new JdbcOutputFormatBuilder()
                                            .setJdbcOptions(jdbcOptions)
                                            .setFieldDataTypes(fieldDataTypes)
                                            .setJdbcDmlOptions(dmlOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .build();
                            outputFormat.open(0, 1);
                        })
                .isInstanceOf(IOException.class)
                .hasMessage(expectedMsg);
    }

    @Test
    void testInvalidURL() {
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDriverName(getDbMetadata().getDriverClass())
                                            .setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
                                            .setTableName(BOOKS_TABLE.getTableName())
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            outputFormat =
                                    new JdbcOutputFormatBuilder()
                                            .setJdbcOptions(jdbcOptions)
                                            .setFieldDataTypes(fieldDataTypes)
                                            .setJdbcDmlOptions(dmlOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .build();
                            outputFormat.open(0, 1);
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testIncompatibleTypes() {
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDriverName(getDbMetadata().getDriverClass())
                                            .setDBUrl(getDbMetadata().getUrl())
                                            .setTableName(BOOKS_TABLE.getTableName())
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            outputFormat =
                                    new JdbcOutputFormatBuilder()
                                            .setJdbcOptions(jdbcOptions)
                                            .setFieldDataTypes(fieldDataTypes)
                                            .setJdbcDmlOptions(dmlOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .setRowDataTypeInfo(rowDataTypeInfo)
                                            .build();

                            setRuntimeContext(outputFormat, false);
                            outputFormat.open(0, 1);

                            RowData row =
                                    buildGenericData(4, "hello", "world", 0.99, "imthewrongtype");
                            outputFormat.writeRecord(row);
                            outputFormat.close();
                        })
                .rootCause()
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    void testExceptionOnInvalidType() {
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDriverName(getDbMetadata().getDriverClass())
                                            .setDBUrl(getDbMetadata().getUrl())
                                            .setTableName(NEWBOOKS_TABLE.getTableName())
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            outputFormat =
                                    new JdbcOutputFormatBuilder()
                                            .setJdbcOptions(jdbcOptions)
                                            .setFieldDataTypes(fieldDataTypes)
                                            .setJdbcDmlOptions(dmlOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .setRowDataTypeInfo(rowDataTypeInfo)
                                            .build();
                            setRuntimeContext(outputFormat, false);
                            outputFormat.open(0, 1);

                            BookEntry entry = TEST_DATA[0];
                            RowData row =
                                    buildGenericData(
                                            entry.id, entry.title, entry.author, 0L, entry.qty);
                            outputFormat.writeRecord(row);
                            outputFormat.close();
                        })
                .rootCause()
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    void testExceptionOnClose() {
        String expectedMsg = "Writing records to JDBC failed.";
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDriverName(getDbMetadata().getDriverClass())
                                            .setDBUrl(getDbMetadata().getUrl())
                                            .setTableName(NEWBOOKS_TABLE.getTableName())
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            outputFormat =
                                    new JdbcOutputFormatBuilder()
                                            .setJdbcOptions(jdbcOptions)
                                            .setFieldDataTypes(fieldDataTypes)
                                            .setJdbcDmlOptions(dmlOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .setRowDataTypeInfo(rowDataTypeInfo)
                                            .build();
                            setRuntimeContext(outputFormat, true);
                            outputFormat.open(0, 1);

                            BookEntry entry = TEST_DATA[0];
                            RowData row =
                                    buildGenericData(
                                            entry.id,
                                            entry.title,
                                            entry.author,
                                            entry.price,
                                            entry.qty);

                            outputFormat.writeRecord(row);
                            outputFormat.writeRecord(
                                    row); // writing the same record twice must yield a unique key
                            // violation.
                            outputFormat.close();
                        })
                .isInstanceOf(RuntimeException.class)
                .hasMessage(expectedMsg);
    }

    @Test
    void testJdbcOutputFormat() throws IOException, SQLException {
        JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(getDbMetadata().getDriverClass())
                        .setDBUrl(getDbMetadata().getUrl())
                        .setTableName(NEWBOOKS_TABLE.getTableName())
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        for (BookEntry entry : TEST_DATA) {
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        outputFormat.close();

        try (Connection dbConn = getDbMetadata().getConnection()) {
            List<BookEntry> books = NEWBOOKS_TABLE.selectAllTable(dbConn);
            assertThat(books.size()).isEqualTo(TEST_DATA.length);
            assertThat(books.toArray()).isEqualTo(TEST_DATA);
        }
    }

    @Test
    void testFlush() throws SQLException, IOException {
        JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(getDbMetadata().getDriverClass())
                        .setDBUrl(getDbMetadata().getUrl())
                        .setTableName(NEWBOOKS_TABLE.getTableName())
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder().withBatchSize(3).build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(executionOptions)
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        try (Connection dbConn = getDbMetadata().getConnection();
                PreparedStatement statement =
                        dbConn.prepareStatement(NEWBOOKS_TABLE.getSelectAllQuery())) {
            outputFormat.open(0, 1);
            for (int i = 0; i < 2; ++i) {
                outputFormat.writeRecord(
                        buildGenericData(
                                TEST_DATA[i].id,
                                TEST_DATA[i].title,
                                TEST_DATA[i].author,
                                TEST_DATA[i].price,
                                TEST_DATA[i].qty));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isFalse();
            }
            outputFormat.writeRecord(
                    buildGenericData(
                            TEST_DATA[2].id,
                            TEST_DATA[2].title,
                            TEST_DATA[2].author,
                            TEST_DATA[2].price,
                            TEST_DATA[2].qty));
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
            outputFormat.close();
        }
    }

    @Test
    void testFlushWithBatchSizeEqualsZero() throws SQLException, IOException {
        JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(getDbMetadata().getDriverClass())
                        .setDBUrl(getDbMetadata().getUrl())
                        .setTableName(NEWBOOKS_TABLE.getTableName())
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder().withBatchSize(0).build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(executionOptions)
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);

        try (Connection dbConn = getDbMetadata().getConnection();
                PreparedStatement statement =
                        dbConn.prepareStatement(NEWBOOKS_TABLE.getSelectAllQuery())) {
            outputFormat.open(0, 1);
            for (int i = 0; i < 2; ++i) {
                outputFormat.writeRecord(
                        buildGenericData(
                                TEST_DATA[i].id,
                                TEST_DATA[i].title,
                                TEST_DATA[i].author,
                                TEST_DATA[i].price,
                                TEST_DATA[i].qty));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isFalse();
            }
        } finally {
            outputFormat.close();
        }
    }

    @Test
    void testInvalidConnectionInJdbcOutputFormat() throws IOException, SQLException {
        JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(getDbMetadata().getDriverClass())
                        .setDBUrl(getDbMetadata().getUrl())
                        .setTableName(NEWBOOKS_TABLE.getTableName())
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        // write records
        for (int i = 0; i < 3; i++) {
            BookEntry entry = TEST_DATA[i];
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        // close connection
        outputFormat.getConnection().close();

        // continue to write rest records
        for (int i = 3; i < TEST_DATA.length; i++) {
            BookEntry entry = TEST_DATA[i];
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        outputFormat.close();

        try (Connection dbConn = getDbMetadata().getConnection()) {
            List<BookEntry> books = NEWBOOKS_TABLE.selectAllTable(dbConn);
            assertThat(books.size()).isEqualTo(TEST_DATA.length);
            assertThat(books.toArray()).isEqualTo(TEST_DATA);
        }
    }
}
