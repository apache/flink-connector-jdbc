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

package org.apache.flink.connector.jdbc.core.table.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.derby.database.dialect.DerbyDialect;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE_2;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE_3;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS_2;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS_3;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Test suite for {@link JdbcOutputFormatBuilder}. */
class JdbcOutputFormatTest extends JdbcDataTestBase {

    private static JdbcOutputFormat<RowData, ?, ?> outputFormat;
    private static String[] fieldNames = new String[] {"id", "title", "author", "price", "qty"};
    private static DataType[] fieldDataTypes =
            new DataType[] {
                DataTypes.INT(),
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.DOUBLE(),
                DataTypes.INT()
            };
    private static RowType rowType =
            RowType.of(
                    Arrays.stream(fieldDataTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new),
                    fieldNames);

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
                            InternalJdbcConnectionOptions jdbcOptions =
                                    InternalJdbcConnectionOptions.builder()
                                            .setDriverName("org.apache.derby.jdbc.idontexist")
                                            .setDBUrl(getMetadata().getJdbcUrl())
                                            .setTableName(INPUT_TABLE)
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
                            JdbcOutputSerializer<RowData> serializer =
                                    JdbcOutputSerializer.of(
                                            getSerializer(TypeInformation.of(RowData.class), true));
                            outputFormat.open(serializer);
                        })
                .isInstanceOf(IOException.class)
                .hasMessage(expectedMsg);
    }

    @Test
    void testInvalidURL() {
        assertThatThrownBy(
                        () -> {
                            InternalJdbcConnectionOptions jdbcOptions =
                                    InternalJdbcConnectionOptions.builder()
                                            .setDriverName(getMetadata().getDriverClass())
                                            .setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
                                            .setTableName(INPUT_TABLE)
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

                            JdbcOutputSerializer<RowData> serializer =
                                    JdbcOutputSerializer.of(
                                            getSerializer(TypeInformation.of(RowData.class), true));
                            outputFormat.open(serializer);
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testInvalidCompatibleMode() {
        assertThatThrownBy(
                        () -> {
                            InternalJdbcConnectionOptions jdbcOptions =
                                    InternalJdbcConnectionOptions.builder()
                                            .setDriverName(getMetadata().getDriverClass())
                                            .setDBUrl(getMetadata().getJdbcUrl())
                                            .setTableName(INPUT_TABLE)
                                            .setCompatibleMode("invalidCompatibleMode")
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

                            JdbcOutputSerializer<RowData> serializer =
                                    JdbcOutputSerializer.of(
                                            getSerializer(TypeInformation.of(RowData.class), true));
                            outputFormat.open(serializer);
                        })
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testIncompatibleTypes() {
        assertThatThrownBy(
                        () -> {
                            InternalJdbcConnectionOptions jdbcOptions =
                                    InternalJdbcConnectionOptions.builder()
                                            .setDriverName(getMetadata().getDriverClass())
                                            .setDBUrl(getMetadata().getJdbcUrl())
                                            .setTableName(INPUT_TABLE)
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

                            JdbcOutputSerializer<RowData> serializer =
                                    JdbcOutputSerializer.of(
                                            getSerializer(TypeInformation.of(RowData.class), true));
                            outputFormat.open(serializer);

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
                            InternalJdbcConnectionOptions jdbcOptions =
                                    InternalJdbcConnectionOptions.builder()
                                            .setDriverName(getMetadata().getDriverClass())
                                            .setDBUrl(getMetadata().getJdbcUrl())
                                            .setTableName(OUTPUT_TABLE)
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

                            JdbcOutputSerializer<RowData> serializer =
                                    JdbcOutputSerializer.of(
                                            getSerializer(TypeInformation.of(RowData.class), true));
                            outputFormat.open(serializer);

                            TestEntry entry = TEST_DATA[0];
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
                            InternalJdbcConnectionOptions jdbcOptions =
                                    InternalJdbcConnectionOptions.builder()
                                            .setDriverName(getMetadata().getDriverClass())
                                            .setDBUrl(getMetadata().getJdbcUrl())
                                            .setTableName(OUTPUT_TABLE)
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

                            JdbcOutputSerializer<RowData> serializer =
                                    JdbcOutputSerializer.of(
                                            getSerializer(TypeInformation.of(RowData.class), true));
                            outputFormat.open(serializer);

                            TestEntry entry = TEST_DATA[0];
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
        InternalJdbcConnectionOptions jdbcOptions =
                InternalJdbcConnectionOptions.builder()
                        .setDriverName(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setTableName(OUTPUT_TABLE)
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
                        .build();

        JdbcOutputSerializer<RowData> serializer =
                JdbcOutputSerializer.of(getSerializer(TypeInformation.of(RowData.class), true));
        outputFormat.open(serializer);

        for (TestEntry entry : TEST_DATA) {
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        outputFormat.close();

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
    void testGetLineageVertex() throws Exception {
        InternalJdbcConnectionOptions jdbcOptions =
                InternalJdbcConnectionOptions.builder()
                        .setDriverName(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setTableName(OUTPUT_TABLE)
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
                        .build();

        JdbcOutputSerializer<RowData> serializer =
                JdbcOutputSerializer.of(getSerializer(TypeInformation.of(RowData.class), true));
        outputFormat.open(serializer);

        LineageVertex lineageVertex = outputFormat.getLineageVertex();
        assertThat(lineageVertex.datasets().size()).isEqualTo(1);
        assertThat(lineageVertex.datasets().get(0).name()).isEqualTo("newbooks");
        assertThat(lineageVertex.datasets().get(0).namespace()).isEqualTo("derby:memory:test");
    }

    @Test
    void testFlush() throws SQLException, IOException {
        InternalJdbcConnectionOptions jdbcOptions =
                InternalJdbcConnectionOptions.builder()
                        .setDriverName(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setTableName(OUTPUT_TABLE_2)
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
                        .build();

        JdbcOutputSerializer<RowData> serializer =
                JdbcOutputSerializer.of(getSerializer(TypeInformation.of(RowData.class), true));
        outputFormat.open(serializer);

        try (Connection dbConn = DriverManager.getConnection(getMetadata().getJdbcUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_2)) {

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
        InternalJdbcConnectionOptions jdbcOptions =
                InternalJdbcConnectionOptions.builder()
                        .setDriverName(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setTableName(OUTPUT_TABLE_2)
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
                        .build();

        JdbcOutputSerializer<RowData> serializer =
                JdbcOutputSerializer.of(getSerializer(TypeInformation.of(RowData.class), true));
        outputFormat.open(serializer);

        try (Connection dbConn = DriverManager.getConnection(getMetadata().getJdbcUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_2)) {

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
        InternalJdbcConnectionOptions jdbcOptions =
                InternalJdbcConnectionOptions.builder()
                        .setDriverName(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setTableName(OUTPUT_TABLE_3)
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
                        .build();

        JdbcOutputSerializer<RowData> serializer =
                JdbcOutputSerializer.of(getSerializer(TypeInformation.of(RowData.class), true));
        outputFormat.open(serializer);

        // write records
        for (int i = 0; i < 3; i++) {
            TestEntry entry = TEST_DATA[i];
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        // close connection
        outputFormat.getConnection().close();

        // continue to write rest records
        for (int i = 3; i < TEST_DATA.length; i++) {
            TestEntry entry = TEST_DATA[i];
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        outputFormat.close();

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

    @Test
    void testUpsertBranchWithNativeUpsertReducesByKey() throws Exception {
        RecordingDialect dialect = new RecordingDialect(true);

        assertChangelogIsReducedByKey(dialect);

        // the dialect's own upsert is used; the insert-or-update fallback is never assembled
        assertThat(dialect.upsertCalls).isEqualTo(1);
        assertThat(dialect.deleteCalls).isEqualTo(1);
        assertThat(dialect.rowExistsCalls).isZero();
        assertThat(dialect.updateCalls).isZero();
    }

    @Test
    void testUpsertBranchWithInsertOrUpdateFallbackReducesByKey() throws Exception {
        RecordingDialect dialect = new RecordingDialect(false);

        assertChangelogIsReducedByKey(dialect);

        // no native upsert (Derby, Trino): exists + insert + update replace it
        assertThat(dialect.upsertCalls).isEqualTo(1);
        assertThat(dialect.rowExistsCalls).isEqualTo(1);
        assertThat(dialect.insertCalls).isEqualTo(1);
        assertThat(dialect.updateCalls).isEqualTo(1);
        assertThat(dialect.deleteCalls).isEqualTo(1);
    }

    /**
     * Shared by both upsert branches: within one buffer the last change to a key wins, the key
     * alone decides identity, and a delete of an unknown key is a no-op.
     */
    private void assertChangelogIsReducedByKey(JdbcDialect dialect) throws Exception {
        openOutputFormat(dialect, new String[] {"id"}, batchOf(100, 0), false);
        TestEntry first = TEST_DATA[0];
        TestEntry second = TEST_DATA[1];

        outputFormat.writeRecord(changelogRow(RowKind.INSERT, first, "v1"));
        outputFormat.writeRecord(changelogRow(RowKind.UPDATE_AFTER, first, "v2"));
        outputFormat.flush();
        assertThat(titlesById()).containsOnly(entry(first.id, "v2"));

        // DELETE then INSERT of the same key keeps the row: the reduce key carries no row kind
        outputFormat.writeRecord(changelogRow(RowKind.DELETE, first, "v2"));
        outputFormat.writeRecord(changelogRow(RowKind.INSERT, first, "v3"));
        outputFormat.flush();
        assertThat(titlesById()).containsOnly(entry(first.id, "v3"));

        // INSERT then DELETE of a new key writes nothing; a DELETE of an unknown key is a no-op
        outputFormat.writeRecord(changelogRow(RowKind.INSERT, second, "v1"));
        outputFormat.writeRecord(changelogRow(RowKind.DELETE, second, "v1"));
        outputFormat.writeRecord(changelogRow(RowKind.DELETE, TEST_DATA[3], "never written"));
        outputFormat.flush();
        assertThat(titlesById()).containsOnly(entry(first.id, "v3"));

        // an UPDATE_BEFORE on its own is a delete
        outputFormat.writeRecord(changelogRow(RowKind.UPDATE_BEFORE, first, "v3"));
        outputFormat.flush();
        assertThat(titlesById()).isEmpty();
    }

    @Test
    void testAppendOnlyBranchUsesThePlainInsert() throws Exception {
        RecordingDialect dialect = new RecordingDialect(true);
        openOutputFormat(dialect, null, batchOf(100, 0), false);

        outputFormat.writeRecord(changelogRow(RowKind.INSERT, TEST_DATA[0], "a"));
        outputFormat.writeRecord(changelogRow(RowKind.INSERT, TEST_DATA[1], "b"));
        outputFormat.flush();

        assertThat(titlesById())
                .containsOnly(entry(TEST_DATA[0].id, "a"), entry(TEST_DATA[1].id, "b"));
        assertThat(dialect.insertCalls).isEqualTo(1);
        assertThat(dialect.upsertCalls).isZero();
        assertThat(dialect.rowExistsCalls).isZero();
        assertThat(dialect.deleteCalls).isZero();
    }

    @Test
    void testKeyFieldOutsideTheFieldNamesFailsAtOpen() {
        // indexOf gives -1 for the unknown key, and the builder indexes the field types with it
        assertThatThrownBy(
                        () ->
                                openOutputFormat(
                                        new DerbyDialect(),
                                        new String[] {"nope"},
                                        batchOf(100, 0),
                                        false))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    @Test
    void testNullKeyValueMatchesNeitherExistsNorDelete() throws Exception {
        openOutputFormat(new DerbyDialect(), new String[] {"title"}, batchOf(100, 0), false);
        TestEntry entry = TEST_DATA[0];

        outputFormat.writeRecord(changelogRow(RowKind.INSERT, entry, null));
        outputFormat.flush();
        assertThat(titlesById()).containsOnly(entry(entry.id, null));

        // `DELETE ... WHERE title = ?` with NULL matches nothing: the row is silently retained
        outputFormat.writeRecord(changelogRow(RowKind.DELETE, entry, null));
        outputFormat.flush();
        assertThat(titlesById()).containsOnly(entry(entry.id, null));

        // `exists` never matches either, so the same key is inserted again, which the table's
        // primary key rejects
        outputFormat.writeRecord(changelogRow(RowKind.INSERT, entry, null));
        assertThatThrownBy(outputFormat::flush)
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(SQLException.class);

        // the buffer survived the failed flush: once the conflict is gone the replay lands
        executeUpdate("DELETE FROM " + OUTPUT_TABLE_3 + " WHERE id = " + entry.id);
        outputFormat.flush();
        assertThat(titlesById()).containsOnly(entry(entry.id, null));
    }

    @Test
    void testObjectReuseCopiesTheRecordBeforeBuffering() throws Exception {
        openOutputFormat(new DerbyDialect(), new String[] {"id"}, batchOf(100, 0), true);

        GenericRowData row = (GenericRowData) changelogRow(RowKind.INSERT, TEST_DATA[0], "first");
        outputFormat.writeRecord(row);
        // with object reuse on, the runtime hands the same object over again for the next record
        row.setField(0, TEST_DATA[1].id);
        row.setField(1, StringData.fromString("second"));
        outputFormat.writeRecord(row);
        outputFormat.flush();

        assertThat(titlesById())
                .containsOnly(entry(TEST_DATA[0].id, "first"), entry(TEST_DATA[1].id, "second"));
    }

    @Test
    void testFailedFlushKeepsTheBufferAndReplaysUpsertsIdempotently() throws Exception {
        String child = OUTPUT_TABLE_3 + "_child";
        TestEntry kept = TEST_DATA[0];
        TestEntry deleted = TEST_DATA[1];
        executeUpdate(
                "INSERT INTO "
                        + OUTPUT_TABLE_3
                        + " (id, title) VALUES ("
                        + deleted.id
                        + ", 'old')");
        executeUpdate(
                "CREATE TABLE "
                        + child
                        + " (id INT NOT NULL PRIMARY KEY, parent INT NOT NULL,"
                        + " CONSTRAINT child_fk FOREIGN KEY (parent) REFERENCES "
                        + OUTPUT_TABLE_3
                        + " (id))");
        try {
            executeUpdate("INSERT INTO " + child + " VALUES (1, " + deleted.id + ")");
            openOutputFormat(new DerbyDialect(), new String[] {"id"}, batchOf(100, 1), false);

            outputFormat.writeRecord(changelogRow(RowKind.INSERT, kept, "kept"));
            outputFormat.writeRecord(changelogRow(RowKind.DELETE, deleted, "old"));

            // the upsert batch commits, the delete batch fails on the foreign key, the one retry
            // re-prepares the statements and replays both, and fails the same way
            assertThatThrownBy(outputFormat::flush)
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(SQLException.class);
            assertThat(titlesById()).containsOnly(entry(kept.id, "kept"), entry(deleted.id, "old"));

            // remove the conflict: the next flush replays the whole buffer and the delete lands,
            // and the replayed upsert did not duplicate the row
            executeUpdate("DELETE FROM " + child);
            outputFormat.flush();
            assertThat(titlesById()).containsOnly(entry(kept.id, "kept"));

            // and the buffer is empty afterwards
            outputFormat.flush();
            assertThat(titlesById()).containsOnly(entry(kept.id, "kept"));
        } finally {
            executeUpdate("DELETE FROM " + child);
            executeUpdate("DROP TABLE " + child);
        }
    }

    private void openOutputFormat(
            JdbcDialect dialect,
            String[] keyFields,
            JdbcExecutionOptions executionOptions,
            boolean objectReuse)
            throws IOException {
        InternalJdbcConnectionOptions jdbcOptions =
                InternalJdbcConnectionOptions.builder()
                        .setDriverName(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setTableName(OUTPUT_TABLE_3)
                        .build();
        JdbcDmlOptions.JdbcDmlOptionsBuilder dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(OUTPUT_TABLE_3)
                        .withDialect(dialect)
                        .withFieldNames(fieldNames);
        if (keyFields != null) {
            dmlOptions.withKeyFields(keyFields);
        }
        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions.build())
                        .setJdbcExecutionOptions(executionOptions)
                        .build();
        outputFormat.open(
                JdbcOutputSerializer.of(
                        getSerializer(InternalTypeInfo.of(rowType), objectReuse), objectReuse));
    }

    private static JdbcExecutionOptions batchOf(int batchSize, int maxRetries) {
        return JdbcExecutionOptions.builder()
                .withBatchSize(batchSize)
                .withBatchIntervalMs(0)
                .withMaxRetries(maxRetries)
                .build();
    }

    private static RowData changelogRow(RowKind kind, TestEntry entry, String title) {
        GenericRowData row =
                (GenericRowData)
                        buildGenericData(entry.id, title, entry.author, entry.price, entry.qty);
        row.setRowKind(kind);
        return row;
    }

    private Map<Integer, String> titlesById() throws SQLException {
        Map<Integer, String> titles = new HashMap<>();
        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement();
                ResultSet rs = stat.executeQuery("SELECT id, title FROM " + OUTPUT_TABLE_3)) {
            while (rs.next()) {
                assertThat(titles.put(rs.getInt("id"), rs.getString("title"))).isNull();
            }
        }
        return titles;
    }

    private void executeUpdate(String sql) throws SQLException {
        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.executeUpdate(sql);
        }
    }

    /**
     * A Derby dialect that counts which statement texts the builder asks it for, and can offer a
     * native upsert through Derby's {@code MERGE}, which {@link DerbyDialect} itself does not use.
     */
    private static class RecordingDialect extends DerbyDialect {
        private final boolean nativeUpsert;
        int upsertCalls;
        int rowExistsCalls;
        int insertCalls;
        int updateCalls;
        int deleteCalls;

        RecordingDialect(boolean nativeUpsert) {
            this.nativeUpsert = nativeUpsert;
        }

        @Override
        public Optional<String> getUpsertStatement(
                String tableName, String[] fieldNames, String[] uniqueKeyFields) {
            upsertCalls++;
            if (!nativeUpsert) {
                return super.getUpsertStatement(tableName, fieldNames, uniqueKeyFields);
            }
            String on =
                    Arrays.stream(uniqueKeyFields)
                            .map(f -> "t." + f + " = :" + f)
                            .collect(Collectors.joining(" AND "));
            String set =
                    Arrays.stream(fieldNames)
                            .filter(f -> !Arrays.asList(uniqueKeyFields).contains(f))
                            .map(f -> f + " = :" + f)
                            .collect(Collectors.joining(", "));
            String values =
                    Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
            return Optional.of(
                    "MERGE INTO "
                            + tableName
                            + " t USING SYSIBM.SYSDUMMY1 ON "
                            + on
                            + " WHEN MATCHED THEN UPDATE SET "
                            + set
                            + " WHEN NOT MATCHED THEN INSERT ("
                            + String.join(", ", fieldNames)
                            + ") VALUES ("
                            + values
                            + ")");
        }

        @Override
        public String getRowExistsStatement(String tableName, String[] conditionFields) {
            rowExistsCalls++;
            return super.getRowExistsStatement(tableName, conditionFields);
        }

        @Override
        public String getInsertIntoStatement(String tableName, String[] fieldNames) {
            insertCalls++;
            return super.getInsertIntoStatement(tableName, fieldNames);
        }

        @Override
        public String getUpdateStatement(
                String tableName, String[] fieldNames, String[] conditionFields) {
            updateCalls++;
            return super.getUpdateStatement(tableName, fieldNames, conditionFields);
        }

        @Override
        public String getDeleteStatement(String tableName, String[] conditionFields) {
            deleteCalls++;
            return super.getDeleteStatement(tableName, conditionFields);
        }
    }

    @AfterEach
    void clearOutputTable() throws Exception {
        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + OUTPUT_TABLE);
        }
    }
}
