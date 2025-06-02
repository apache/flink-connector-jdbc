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

package org.apache.flink.connector.jdbc.backward.compatibility;

import org.apache.flink.connector.jdbc.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for backward compatibility. */
public class DynamicTableSinkTest implements PostgresTestBase {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    protected final TableRow upsertOutputTable = createUpsertOutputTable();
    protected final TableRow appendOutputTable = createAppendOutputTable();
    protected final TableRow batchOutputTable = createBatchOutputTable();
    protected final TableRow userOutputTable = createUserOutputTable();

    protected TableRow createUpsertOutputTable() {
        return tableRow(
                "dynamicSinkForUpsert",
                pkField("cnt", DataTypes.BIGINT().notNull()),
                field("lencnt", DataTypes.BIGINT().notNull()),
                pkField("cTag", DataTypes.INT().notNull()),
                field("ts", dbType("TIMESTAMP"), DataTypes.TIMESTAMP()));
    }

    protected TableRow createAppendOutputTable() {
        return tableRow(
                "dynamicSinkForAppend",
                field("id", DataTypes.INT().notNull()),
                field("num", DataTypes.BIGINT().notNull()),
                field("ts", dbType("TIMESTAMP"), DataTypes.TIMESTAMP()));
    }

    protected TableRow createBatchOutputTable() {
        return tableRow(
                "dynamicSinkForBatch",
                field("NAME", DataTypes.VARCHAR(20).notNull()),
                field("SCORE", DataTypes.BIGINT().notNull()));
    }

    protected TableRow createUserOutputTable() {
        return tableRow(
                "USER_TABLE",
                pkField("user_id", DataTypes.VARCHAR(20).notNull()),
                field("user_name", DataTypes.VARCHAR(20).notNull()),
                field("email", DataTypes.VARCHAR(255)),
                field("balance", DataTypes.DECIMAL(18, 2)),
                field("balance2", DataTypes.DECIMAL(18, 2)));
    }

    @Override
    public List<TableManaged> getManagedTables() {
        return Arrays.asList(
                upsertOutputTable, appendOutputTable, batchOutputTable, userOutputTable);
    }

    @AfterEach
    void afterEach() {
        TestValuesTableFactory.clearAllData();
    }

    protected List<Row> testUserData() {
        return Arrays.asList(
                Row.of(
                        "user1",
                        "Tom",
                        "tom123@gmail.com",
                        new BigDecimal("8.10"),
                        new BigDecimal("16.20")),
                Row.of(
                        "user3",
                        "Bailey",
                        "bailey@qq.com",
                        new BigDecimal("9.99"),
                        new BigDecimal("19.98")),
                Row.of(
                        "user4",
                        "Tina",
                        "tina@gmail.com",
                        new BigDecimal("11.30"),
                        new BigDecimal("22.60")));
    }

    protected List<Row> testData() {
        return Arrays.asList(
                Row.of(1, 1L, "Hi", Timestamp.valueOf("1970-01-01 00:00:00.001")),
                Row.of(2, 2L, "Hello", Timestamp.valueOf("1970-01-01 00:00:00.002")),
                Row.of(3, 2L, "Hello world", Timestamp.valueOf("1970-01-01 00:00:00.003")),
                Row.of(
                        4,
                        3L,
                        "Hello world, how are you?",
                        Timestamp.valueOf("1970-01-01 00:00:00.004")),
                Row.of(5, 3L, "I am fine.", Timestamp.valueOf("1970-01-01 00:00:00.005")),
                Row.of(6, 3L, "Luke Skywalker", Timestamp.valueOf("1970-01-01 00:00:00.006")),
                Row.of(7, 4L, "Comment#1", Timestamp.valueOf("1970-01-01 00:00:00.007")),
                Row.of(8, 4L, "Comment#2", Timestamp.valueOf("1970-01-01 00:00:00.008")),
                Row.of(9, 4L, "Comment#3", Timestamp.valueOf("1970-01-01 00:00:00.009")),
                Row.of(10, 4L, "Comment#4", Timestamp.valueOf("1970-01-01 00:00:00.010")),
                Row.of(11, 5L, "Comment#5", Timestamp.valueOf("1970-01-01 00:00:00.011")),
                Row.of(12, 5L, "Comment#6", Timestamp.valueOf("1970-01-01 00:00:00.012")),
                Row.of(13, 5L, "Comment#7", Timestamp.valueOf("1970-01-01 00:00:00.013")),
                Row.of(14, 5L, "Comment#8", Timestamp.valueOf("1970-01-01 00:00:00.014")),
                Row.of(15, 5L, "Comment#9", Timestamp.valueOf("1970-01-01 00:00:00.015")),
                Row.of(16, 6L, "Comment#10", Timestamp.valueOf("1970-01-01 00:00:00.016")),
                Row.of(17, 6L, "Comment#11", Timestamp.valueOf("1970-01-01 00:00:00.017")),
                Row.of(18, 6L, "Comment#12", Timestamp.valueOf("1970-01-01 00:00:00.018")),
                Row.of(19, 6L, "Comment#13", Timestamp.valueOf("1970-01-01 00:00:00.019")),
                Row.of(20, 6L, "Comment#14", Timestamp.valueOf("1970-01-01 00:00:00.020")),
                Row.of(21, 6L, "Comment#15", Timestamp.valueOf("1970-01-01 00:00:00.021")));
    }

    protected Map<Integer, Row> testDataMap() {
        return testData().stream()
                .collect(Collectors.toMap(r -> r.getFieldAs(0), Function.identity()));
    }

    private void createTestDataTempView(StreamTableEnvironment tEnv, String viewName) {
        Table table = tEnv.fromValues(testData()).as("id", "num", "text", "ts");

        tEnv.createTemporaryView(viewName, table);
    }

    @Test
    protected void testUpsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String viewName = "testData";
        createTestDataTempView(tEnv, viewName);

        String tableName = "upsertSink";
        tEnv.executeSql(
                upsertOutputTable.getCreateQueryForFlink(
                        getMetadata(),
                        tableName,
                        Arrays.asList(
                                "'sink.buffer-flush.max-rows' = '2'",
                                "'sink.buffer-flush.interval' = '0'",
                                "'sink.max-retries' = '0'")));

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s "
                                        + " SELECT cnt, COUNT(len) AS lencnt, cTag, MAX(ts) AS ts "
                                        + " FROM ( "
                                        + "  SELECT len, COUNT(id) as cnt, cTag, MAX(ts) AS ts "
                                        + "  FROM (SELECT id, CHAR_LENGTH(text) AS len, (CASE WHEN id > 0 THEN 1 ELSE 0 END) cTag, ts FROM %s) "
                                        + "  GROUP BY len, cTag "
                                        + " ) "
                                        + " GROUP BY cnt, cTag",
                                tableName, viewName))
                .await();

        Map<Integer, Row> mapTestData = testDataMap();
        assertThat(upsertOutputTable.selectAllTable(getMetadata()))
                .containsExactlyInAnyOrder(
                        Row.of(1L, 5L, 1, mapTestData.get(6).getField(3)),
                        Row.of(7L, 1L, 1, mapTestData.get(21).getField(3)),
                        Row.of(9L, 1L, 1, mapTestData.get(15).getField(3)));
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
                                .map(d -> Row.of(d.getField(0), d.getField(1), d.getField(3)))
                                .collect(Collectors.toList()));
    }

    @Test
    void testBatchSink() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        String tableName = "batchSink";
        tEnv.executeSql(
                batchOutputTable.getCreateQueryForFlink(
                        getMetadata(),
                        tableName,
                        Arrays.asList(
                                "'sink.buffer-flush.max-rows' = '2'",
                                "'sink.buffer-flush.interval' = '300ms'",
                                "'sink.max-retries' = '4'")));

        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s "
                                        + " SELECT user_name, score "
                                        + " FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), (42, 'Kim'), (1, 'Bob')) "
                                        + " AS UserCountTable(score, user_name) ",
                                tableName));
        tableResult.await();

        assertThat(batchOutputTable.selectAllTable(getMetadata()))
                .containsExactlyInAnyOrder(
                        Row.of("Bob", 1L),
                        Row.of("Tom", 22L),
                        Row.of("Kim", 42L),
                        Row.of("Kim", 42L),
                        Row.of("Bob", 1L));
    }

    @Test
    protected void testReadingFromChangelogSource() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String dataId = TestValuesTableFactory.registerData(TestData.userChangelog());

        String userTableLogs = "user_logs";
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s ( "
                                + "  user_id STRING, "
                                + "  user_name STRING, "
                                + "  email STRING, "
                                + "  balance DECIMAL(18,2), "
                                + "  balance2 AS balance * 2 "
                                + ") WITH ( "
                                + " 'connector' = 'values', "
                                + " 'data-id' = '%s', "
                                + " 'changelog-mode' = 'I,UA,UB,D' "
                                + ")",
                        userTableLogs, dataId));

        String userTableSink = "user_sink";
        tEnv.executeSql(
                userOutputTable.getCreateQueryForFlink(
                        getMetadata(),
                        userTableSink,
                        Arrays.asList(
                                "'sink.buffer-flush.max-rows' = '2'",
                                "'sink.buffer-flush.interval' = '0'")));

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s SELECT * FROM %s", userTableSink, userTableLogs))
                .await();

        assertThat(userOutputTable.selectAllTable(getMetadata()))
                .containsExactlyInAnyOrderElementsOf(testUserData());
    }
}
