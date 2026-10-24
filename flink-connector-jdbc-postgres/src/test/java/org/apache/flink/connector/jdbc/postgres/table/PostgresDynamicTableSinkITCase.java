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

package org.apache.flink.connector.jdbc.postgres.table;

import org.apache.flink.connector.jdbc.core.table.sink.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.postgres.database.dialect.PostgresDialect;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.assertj.core.api.Assertions.assertThat;

/** The Table Sink ITCase for {@link PostgresDialect}. */
class PostgresDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements PostgresTestBase {

    private final TableRow arrayOutputTable =
            tableRow(
                    "dynamicSinkForArray",
                    field("id", dbType("INTEGER NOT NULL"), DataTypes.INT().notNull()),
                    field("int_arr", dbType("INTEGER[]"), DataTypes.ARRAY(DataTypes.INT())),
                    field(
                            "tinyint_arr",
                            dbType("SMALLINT[]"),
                            DataTypes.ARRAY(DataTypes.TINYINT())),
                    field("text_arr", dbType("TEXT[]"), DataTypes.ARRAY(DataTypes.STRING())),
                    field("time_arr", dbType("TIME(3)[]"), DataTypes.ARRAY(DataTypes.TIME(3))),
                    field(
                            "ts_arr",
                            dbType("TIMESTAMP(3)[]"),
                            DataTypes.ARRAY(DataTypes.TIMESTAMP(3))));

    @Override
    public List<TableManaged> getManagedTables() {
        List<TableManaged> tables = new ArrayList<>(super.getManagedTables());
        tables.add(arrayOutputTable);
        return tables;
    }

    @Test
    void testArray() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String tableName = "arraySink";
        tEnv.executeSql(arrayOutputTable.getCreateQueryForFlink(getMetadata(), tableName));
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s VALUES "
                                        + "(1, ARRAY[1, CAST(NULL AS INT), 3], "
                                        + "ARRAY[CAST(-128 AS TINYINT), CAST(127 AS TINYINT)], "
                                        + "ARRAY['a', CAST(NULL AS STRING)], "
                                        + "ARRAY[TIME '12:34:56.789'], "
                                        + "ARRAY[TIMESTAMP '2025-01-01 00:00:00.123']), "
                                        + "(2, CAST(NULL AS ARRAY<INT>), "
                                        + "CAST(NULL AS ARRAY<TINYINT>), "
                                        + "CAST(NULL AS ARRAY<STRING>), "
                                        + "CAST(NULL AS ARRAY<TIME(3)>), "
                                        + "CAST(NULL AS ARRAY<TIMESTAMP(3)>)), "
                                        + "(3, ARRAY_REMOVE(ARRAY[0], 0), "
                                        + "ARRAY_REMOVE(ARRAY[CAST(0 AS TINYINT)], CAST(0 AS TINYINT)), "
                                        + "ARRAY_REMOVE(ARRAY['x'], 'x'), "
                                        + "ARRAY_REMOVE(ARRAY[TIME '00:00:00'], TIME '00:00:00'), "
                                        + "ARRAY_REMOVE(ARRAY[TIMESTAMP '2025-01-01 00:00:00'], "
                                        + "TIMESTAMP '2025-01-01 00:00:00'))",
                                tableName))
                .await();

        // Compare the text representation in PostgreSQL to check the stored values directly.
        assertThat(selectArraysAsText())
                .containsExactly(
                        Arrays.asList(
                                "{1,NULL,3}",
                                "{-128,127}",
                                "{a,NULL}",
                                "{12:34:56.789}",
                                "{\"2025-01-01 00:00:00.123\"}"),
                        Arrays.asList(null, null, null, null, null),
                        Arrays.asList("{}", "{}", "{}", "{}", "{}"));
    }

    private List<List<String>> selectArraysAsText() throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        try (Connection conn = getMetadata().getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                String.format(
                                        "SELECT int_arr::text, tinyint_arr::text, text_arr::text, "
                                                + "time_arr::text, ts_arr::text "
                                                + "FROM %s ORDER BY id",
                                        arrayOutputTable.getTableName()))) {
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= 5; i++) {
                    row.add(rs.getString(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }
}
