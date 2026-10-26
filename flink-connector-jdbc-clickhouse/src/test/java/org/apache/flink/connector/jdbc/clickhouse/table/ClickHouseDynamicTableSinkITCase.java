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
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase.tableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;

/** The Table Sink ITCase for {@link ClickHouseDialect}. */
class ClickHouseDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements ClickHouseTestBase {

    private final TableRow complexOutputTable =
            tableRow(
                    "MAP_ARRAY_TABLE",
                    pkField("id", dbType("Int32"), DataTypes.INT().notNull()),
                    field(
                            "map_col",
                            dbType("Map(Int, String)"),
                            DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())),
                    field(
                            "array_col",
                            dbType("Array(String)"),
                            DataTypes.ARRAY(DataTypes.STRING())));

    @Override
    protected TableRow createUpsertOutputTable() {
        return tableRow(
                "dynamicSinkForUpsert",
                pkField("cnt", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("lencnt", dbType("Int64"), DataTypes.BIGINT().notNull()),
                pkField("cTag", dbType("Int32"), DataTypes.INT().notNull()),
                field("ts", dbType("DateTime64(6)"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createAppendOutputTable() {
        return tableRow(
                "dynamicSinkForAppend",
                pkField("id", dbType("Int32"), DataTypes.INT().notNull()),
                field("num", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("ts", dbType("DateTime64(6)"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createBatchOutputTable() {
        return tableRow(
                "dynamicSinkForBatch",
                field("NAME", dbType("String"), DataTypes.VARCHAR(20).notNull()),
                field("SCORE", dbType("Int64"), DataTypes.BIGINT().notNull()));
    }

    @Override
    protected TableRow createRealOutputTable() {
        return tableRow("REAL_TABLE", field("real_data", dbType("Float32"), DataTypes.FLOAT()));
    }

    @Override
    protected TableRow createUserOutputTable() {
        return tableRow(
                "USER_TABLE",
                pkField("user_id", dbType("String"), DataTypes.VARCHAR(20).notNull()),
                pkField("user_name", dbType("String"), DataTypes.VARCHAR(20).notNull()),
                field("email", dbType("String"), DataTypes.VARCHAR(255)),
                field("balance", dbType("Decimal(18, 2)"), DataTypes.DECIMAL(18, 2)),
                field("balance2", dbType("Decimal(18, 2)"), DataTypes.DECIMAL(18, 2)));
    }

    @Override
    protected TableRow createCheckpointOutputTable() {
        return tableRow(
                "checkpointTable", field("id", dbType("Int64"), DataTypes.BIGINT().notNull()));
    }

    private static List<Row> expectedRows() {
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

    private static List<Row> insertRows() {
        return Arrays.asList(
                Row.ofKind(
                        RowKind.INSERT,
                        1,
                        new HashMap<Integer, String>() {
                            {
                                put(1, "one");
                                put(2, "two");
                            }
                        },
                        new String[] {"a", "b"}),
                Row.ofKind(
                        RowKind.INSERT,
                        2,
                        new HashMap<Integer, String>() {
                            {
                                put(3, "three");
                            }
                        },
                        new String[] {"c"}),
                Row.ofKind(RowKind.INSERT, 3, new HashMap<Integer, String>(), new String[] {}));
    }

    @Test
    void testMapAndArrayRoundTrip() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String dataId = TestValuesTableFactory.registerData(insertRows());

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
                                "'sink.buffer-flush.max-rows' = '2'",
                                "'sink.buffer-flush.interval' = '0'")));

        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM %s", sinkTable, sourceTable))
                .await();

        assertThat(complexOutputTable.selectAllTable(getMetadata()))
                .containsExactlyInAnyOrderElementsOf(expectedRows());
    }
}
