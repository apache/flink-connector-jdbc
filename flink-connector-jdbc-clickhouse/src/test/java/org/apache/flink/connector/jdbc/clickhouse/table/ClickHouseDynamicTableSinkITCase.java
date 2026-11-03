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

package org.apache.flink.connector.jdbc.clickhouse.table;

import org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase;
import org.apache.flink.connector.jdbc.clickhouse.database.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.core.table.sink.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase.tableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;
import static org.assertj.core.api.Assertions.assertThat;

/** The Table Sink ITCase for {@link ClickHouseDialect}. */
class ClickHouseDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements ClickHouseTestBase {

    private final TableRow complexOutputTable =
            tableRow(
                    "map_array_table",
                    "MergeTree()",
                    "PRIMARY KEY",
                    field("id", dbType("Int32"), DataTypes.INT()),
                    field(
                            "map_col",
                            dbType("Map(Int, String)"),
                            DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())),
                    field(
                            "array_col",
                            dbType("Array(String)"),
                            DataTypes.ARRAY(DataTypes.STRING())));

    private final TableRow updateOutputTable =
            tableRow(
                    "update_sink",
                    "MergeTree()",
                    "ORDER BY",
                    pkField("id", dbType("Int32"), DataTypes.INT()),
                    field("val", dbType("String"), DataTypes.STRING()),
                    field("num", dbType("Float32"), DataTypes.FLOAT()));

    private final TableRow nullableOutputTable =
            tableRow(
                    "nullable_sink",
                    "MergeTree()",
                    "PRIMARY KEY",
                    pkField("id", dbType("Int32"), DataTypes.INT()),
                    field("ts1", dbType("Nullable(DateTime64(3))"), DataTypes.TIMESTAMP(3)),
                    field("ts2", dbType("Nullable(DateTime64(3))"), DataTypes.TIMESTAMP(3)));

    private static List<Row> complexExpectedRows() {
        return Arrays.asList(
                Row.of(
                        1,
                        new HashMap<Integer, String>() {
                            {
                                put(1, "one");
                                put(2, "two");
                            }
                        },
                        new String[] {"a", "b"}),
                Row.of(
                        2,
                        new HashMap<Integer, String>() {
                            {
                                put(3, "three");
                            }
                        },
                        new String[] {"c"}),
                Row.of(3, new HashMap<Integer, String>(), new String[] {}));
    }

    private static List<Row> complexInsertRows() {
        return Arrays.asList(
                Row.of(
                        1,
                        new HashMap<Integer, String>() {
                            {
                                put(1, "one");
                                put(2, "two");
                            }
                        },
                        new String[] {"a", "b"}),
                Row.of(
                        2,
                        new HashMap<Integer, String>() {
                            {
                                put(3, "three");
                            }
                        },
                        new String[] {"c"}),
                Row.of(3, new HashMap<Integer, String>(), new String[] {}));
    }

    private static List<Row> updateInsertRows() {
        return Arrays.asList(
                Row.of(1, "first", 1.0f), Row.of(2, "second", 2.0f), Row.of(1, "updated", 99.99f));
    }

    private static List<Row> expectedRowsAfterUpdate() {
        return Arrays.asList(Row.of(1, "updated", 99.99f), Row.of(2, "second", 2.0f));
    }

    private static List<Row> insertNullableRows() {
        return Arrays.asList(
                Row.of(
                        1,
                        LocalDateTime.parse("2026-02-02T10:10:10"),
                        LocalDateTime.parse("2026-02-02T10:10:10")),
                Row.of(
                        2,
                        LocalDateTime.parse("2026-03-03T10:10:10"),
                        LocalDateTime.parse("2026-03-03T10:10:10")),
                Row.of(3, null, null),
                Row.of(4, LocalDateTime.parse("2026-04-04T10:10:10"), null),
                Row.of(5, null, LocalDateTime.parse("2026-05-05T10:10:10")));
    }

    private static List<Row> expectedNullableRows() {
        return Arrays.asList(
                Row.of(
                        1,
                        Timestamp.valueOf("2026-02-02 10:10:10"),
                        Timestamp.valueOf("2026-02-02 10:10:10")),
                Row.of(
                        2,
                        Timestamp.valueOf("2026-03-03 10:10:10"),
                        Timestamp.valueOf("2026-03-03 10:10:10")),
                Row.of(3, null, null),
                Row.of(4, Timestamp.valueOf("2026-04-04 10:10:10"), null),
                Row.of(5, null, Timestamp.valueOf("2026-05-05 10:10:10")));
    }

    @Override
    protected TableRow createUpsertOutputTable() {
        return tableRow(
                "dynamicSinkForUpsert",
                "ReplacingMergeTree()",
                "ORDER BY",
                pkField("cnt", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("lencnt", dbType("Int64"), DataTypes.BIGINT().notNull()),
                pkField("cTag", dbType("Int32"), DataTypes.INT().notNull()),
                field("ts", dbType("DateTime64(6)"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createAppendOutputTable() {
        return tableRow(
                "dynamicSinkForAppend",
                "MergeTree()",
                "PRIMARY KEY",
                field("id", dbType("Int32"), DataTypes.INT().notNull()),
                field("num", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("ts", dbType("DateTime64(6)"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createBatchOutputTable() {
        return tableRow(
                "dynamicSinkForBatch",
                "MergeTree()",
                "PRIMARY KEY",
                field("NAME", dbType("String"), DataTypes.VARCHAR(20).notNull()),
                field("SCORE", dbType("Int64"), DataTypes.BIGINT().notNull()));
    }

    @Override
    protected TableRow createRealOutputTable() {
        return tableRow(
                "REAL_TABLE",
                "MergeTree()",
                "PRIMARY KEY",
                field("real_data", dbType("Float32"), DataTypes.FLOAT()));
    }

    @Override
    protected TableRow createUserOutputTable() {
        return tableRow(
                "USER_TABLE",
                "ReplacingMergeTree()",
                "ORDER BY",
                pkField("user_id", dbType("String"), DataTypes.VARCHAR(20).notNull()),
                pkField("user_name", dbType("String"), DataTypes.VARCHAR(20).notNull()),
                field("email", dbType("String"), DataTypes.VARCHAR(255)),
                field("balance", dbType("Decimal(18, 2)"), DataTypes.DECIMAL(18, 2)),
                field("balance2", dbType("Decimal(18, 2)"), DataTypes.DECIMAL(18, 2)));
    }

    @Override
    protected TableRow createCheckpointOutputTable() {
        return tableRow(
                "checkpointTable",
                "MergeTree()",
                "PRIMARY KEY",
                field("id", dbType("Int64"), DataTypes.BIGINT().notNull()));
    }

    @Test
    protected void testMapAndArrayRoundTrip() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String dataId = TestValuesTableFactory.registerData(complexInsertRows());
        try (java.sql.Connection conn = getMetadata().getConnection()) {
            complexOutputTable.createTable(conn);
        }

        String sourceTable = "map_array_source";
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s ( "
                                + "  id INT, "
                                + "  map_col MAP<INT, STRING>, "
                                + "  array_col ARRAY<STRING> "
                                + ") WITH ( "
                                + " 'connector' = 'values', "
                                + " 'data-id' = '%s', "
                                + " 'changelog-mode' = 'I' "
                                + ")",
                        sourceTable, dataId));

        String sinkTable = "map_array_sink";
        tEnv.executeSql(
                complexOutputTable.getCreateQueryForFlink(
                        getMetadata(),
                        sinkTable,
                        Arrays.asList(
                                "'sink.buffer-flush.max-rows' = '3'",
                                "'sink.buffer-flush.interval' = '0'")));

        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM %s", sinkTable, sourceTable))
                .await();

        assertThat(complexOutputTable.selectAllTable(getMetadata()))
                .containsExactlyInAnyOrderElementsOf(complexExpectedRows());
    }

    @Test
    protected void testUpdateNonKeyColumns() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String dataId = TestValuesTableFactory.registerData(updateInsertRows());

        try (java.sql.Connection conn = getMetadata().getConnection()) {
            updateOutputTable.createTable(conn);
        }

        String sourceTable = "update_source";
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s ( "
                                + "  id INT PRIMARY KEY NOT ENFORCED, "
                                + "  val STRING, "
                                + "  num FLOAT "
                                + ") WITH ( "
                                + " 'connector' = 'values', "
                                + " 'data-id' = '%s', "
                                + " 'changelog-mode' = 'I,UA,UB,D' "
                                + ");",
                        sourceTable, dataId));

        String sinkTable = "update_sink";

        tEnv.executeSql(
                updateOutputTable.getCreateQueryForFlink(
                        getMetadata(),
                        sinkTable,
                        Arrays.asList(
                                "'sink.buffer-flush.max-rows' = '3'",
                                "'sink.buffer-flush.interval' = '0'")));

        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM %s", sinkTable, sourceTable))
                .await();

        try (java.sql.Statement stmt = getMetadata().getConnection().createStatement()) {
            stmt.execute(String.format("OPTIMIZE TABLE %s FINAL", sinkTable));
        }

        assertThat(updateOutputTable.selectAllTable(getMetadata()))
                .containsExactlyInAnyOrderElementsOf(expectedRowsAfterUpdate());
    }

    @Test
    protected void testNullableTable() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String dataId = TestValuesTableFactory.registerData(insertNullableRows());

        try (java.sql.Connection conn = getMetadata().getConnection()) {
            nullableOutputTable.createTable(conn);
        }

        String sourceTable = "nullable_source";
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s ( "
                                + "  id INT PRIMARY KEY NOT ENFORCED, "
                                + "  ts1 TIMESTAMP(3), "
                                + "  ts2 TIMESTAMP(3) "
                                + ") WITH ( "
                                + " 'connector' = 'values', "
                                + " 'data-id' = '%s', "
                                + " 'changelog-mode' = 'I' "
                                + ");",
                        sourceTable, dataId));

        String sinkTable = "nullable_sink";
        tEnv.executeSql(
                nullableOutputTable.getCreateQueryForFlink(
                        getMetadata(),
                        sinkTable,
                        Arrays.asList(
                                "'sink.buffer-flush.max-rows' = '10'",
                                "'sink.buffer-flush.interval' = '0'")));

        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM %s", sinkTable, sourceTable))
                .await();

        assertThat(nullableOutputTable.selectAllTable(getMetadata()))
                .containsExactlyInAnyOrderElementsOf(expectedNullableRows());
    }
}
