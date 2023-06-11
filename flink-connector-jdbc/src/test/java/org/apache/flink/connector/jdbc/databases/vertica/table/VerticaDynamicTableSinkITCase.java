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

package org.apache.flink.connector.jdbc.databases.vertica.table;

import org.apache.flink.connector.jdbc.databases.vertica.VerticaTestBase;
import org.apache.flink.connector.jdbc.databases.vertica.dialect.VerticaDialect;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.assertj.core.api.Assertions.assertThat;

/** The Table Sink ITCase for {@link VerticaDialect}. */
public class VerticaDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements VerticaTestBase {

    private final TableRow appendOutputTable = createAppendOutputTable();
    private final TableRow realOutputTable = createRealOutputTable();

    @Override
    protected TableRow createAppendOutputTable() {
        return tableRow(
                "dynamicSinkForAppend",
                field("id", DataTypes.BIGINT().notNull()),
                field("num", DataTypes.BIGINT().notNull()),
                field("ts", dbType("TIMESTAMP"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createRealOutputTable() {
        return tableRow("REAL_TABLE", field("real_data", dbType("REAL"), DataTypes.DOUBLE()));
    }

    @Test
    void testAppend() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String viewName = "testData";
        createTestDataTempView(tEnv, viewName);

        String tableName = "appendSink";
        tEnv.executeSql(appendOutputTable.getCreateQueryForFlink(getMetadata(), tableName));

        Set<Integer> searchIds = new HashSet<>(Arrays.asList(2, 10, 20));
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s SELECT id, num, ts FROM %s WHERE id IN (%s)",
                                tableName,
                                viewName,
                                searchIds.stream()
                                        .map(Object::toString)
                                        .collect(Collectors.joining(","))))
                .await();

        List<Row> tableRows = appendOutputTable.selectAllTable(getMetadata());
        assertThat(tableRows.size()).isEqualTo(3);

        Map<Integer, Row> mapTestData = testDataMap();
        assertThat(tableRows)
                .containsExactlyInAnyOrderElementsOf(
                        searchIds.stream()
                                .map(mapTestData::get)
                                .map(
                                        d ->
                                                Row.of(
                                                        ((Number) d.getField(0)).longValue(),
                                                        d.getField(1),
                                                        d.getField(3)))
                                .collect(Collectors.toList()));
    }

    @Test
    void testReal() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        String tableName = "realSink";
        tEnv.executeSql(realOutputTable.getCreateQueryForFlink(getMetadata(), tableName));

        tEnv.executeSql(String.format("INSERT INTO %s SELECT CAST(1.0 as DOUBLE)", tableName))
                .await();

        assertThat(realOutputTable.selectAllTable(getMetadata())).containsExactly(Row.of(1.0d));
    }

    @Test
    @Disabled("Upsert statement is not yet supported in Vertica dialect.")
    void testReadingFromChangelogSource() throws Exception {}

    @Test
    @Disabled("Upsert statement is not yet supported in Vertica dialect.")
    void testUpsert() throws Exception {}

    private void createTestDataTempView(StreamTableEnvironment tEnv, String viewName) {
        Table table = tEnv.fromValues(testData()).as("id", "num", "text", "ts");

        tEnv.createTemporaryView(viewName, table);
    }
}
