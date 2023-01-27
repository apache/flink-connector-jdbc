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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.templates.round2.TableManaged;
import org.apache.flink.connector.jdbc.templates.round2.TableRow;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.jdbc.templates.round2.TableBuilder2.field;
import static org.apache.flink.connector.jdbc.templates.round2.TableBuilder2.pkField;
import static org.apache.flink.connector.jdbc.templates.round2.TableBuilder2.tableRow;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;

/** The ITCase for {@link JdbcDynamicTableSink}. */
public class JdbcDynamicTableSinkITCase extends AbstractTestBase implements JdbcTestBase {

    private static final TableRow OUTPUT_TABLE1 =
            tableRow(
                    "dynamicSinkForUpsert",
                    pkField("cnt", DataTypes.BIGINT().notNull()), // DEFAULT 0
                    field("lencnt", DataTypes.BIGINT().notNull()), // DEFAULT 0
                    pkField("cTag", DataTypes.INT().notNull()), // DEFAULT 0
                    field("ts", "TIMESTAMP", DataTypes.TIMESTAMP()));

    private static final TableRow OUTPUT_TABLE2 =
            tableRow(
                    "dynamicSinkForAppend",
                    field("id", DataTypes.INT().notNull()), // DEFAULT 0
                    field("num", DataTypes.BIGINT().notNull()), // DEFAULT 0
                    field("ts", "TIMESTAMP", DataTypes.TIMESTAMP()));

    private static final TableRow OUTPUT_TABLE3 =
            tableRow(
                    "dynamicSinkForBatch",
                    field("NAME", DataTypes.VARCHAR(20).notNull()),
                    field("SCORE", DataTypes.BIGINT().notNull()) // DEFAULT 0
                    );

    private static final TableRow OUTPUT_TABLE4 =
            tableRow("REAL_TABLE", field("real_data", "REAL", DataTypes.FLOAT()));

    private static final TableRow OUTPUT_TABLE5 =
            tableRow("checkpointTable", field("id", DataTypes.BIGINT().notNull()));

    private static final TableRow USER_TABLE =
            tableRow(
                    "USER_TABLE",
                    pkField("user_id", DataTypes.VARCHAR(20).notNull()),
                    field("user_name", DataTypes.VARCHAR(20).notNull()),
                    field("email", DataTypes.VARCHAR(255)),
                    field("balance", DataTypes.DECIMAL(18, 2)),
                    field("balance2", DataTypes.DECIMAL(18, 2)));

    private String getUrlWithCredentials() {
        return getDbMetadata().getUrlWithCredentials();
    }

    @Override
    public List<TableManaged> getManagedTables() {
        return Arrays.asList(
                OUTPUT_TABLE1,
                OUTPUT_TABLE2,
                OUTPUT_TABLE3,
                OUTPUT_TABLE4,
                OUTPUT_TABLE5,
                USER_TABLE);
    }

    @AfterAll
    static void afterAll() {
        TestValuesTableFactory.clearAllData();
    }

    protected Map<Integer, Row> testDataMap() {
        return testData().stream()
                .collect(Collectors.toMap(r -> r.getFieldAs(0), Function.identity()));
    }

    protected List<Row> testData() {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(1, 1L, "Hi", Timestamp.valueOf("1970-01-01 00:00:00.001")));
        data.add(Row.of(2, 2L, "Hello", Timestamp.valueOf("1970-01-01 00:00:00.002")));
        data.add(Row.of(3, 2L, "Hello world", Timestamp.valueOf("1970-01-01 00:00:00.003")));
        data.add(
                Row.of(
                        4,
                        3L,
                        "Hello world, how are you?",
                        Timestamp.valueOf("1970-01-01 00:00:00.004")));
        data.add(Row.of(5, 3L, "I am fine.", Timestamp.valueOf("1970-01-01 00:00:00.005")));
        data.add(Row.of(6, 3L, "Luke Skywalker", Timestamp.valueOf("1970-01-01 00:00:00.006")));
        data.add(Row.of(7, 4L, "Comment#1", Timestamp.valueOf("1970-01-01 00:00:00.007")));
        data.add(Row.of(8, 4L, "Comment#2", Timestamp.valueOf("1970-01-01 00:00:00.008")));
        data.add(Row.of(9, 4L, "Comment#3", Timestamp.valueOf("1970-01-01 00:00:00.009")));
        data.add(Row.of(10, 4L, "Comment#4", Timestamp.valueOf("1970-01-01 00:00:00.010")));
        data.add(Row.of(11, 5L, "Comment#5", Timestamp.valueOf("1970-01-01 00:00:00.011")));
        data.add(Row.of(12, 5L, "Comment#6", Timestamp.valueOf("1970-01-01 00:00:00.012")));
        data.add(Row.of(13, 5L, "Comment#7", Timestamp.valueOf("1970-01-01 00:00:00.013")));
        data.add(Row.of(14, 5L, "Comment#8", Timestamp.valueOf("1970-01-01 00:00:00.014")));
        data.add(Row.of(15, 5L, "Comment#9", Timestamp.valueOf("1970-01-01 00:00:00.015")));
        data.add(Row.of(16, 6L, "Comment#10", Timestamp.valueOf("1970-01-01 00:00:00.016")));
        data.add(Row.of(17, 6L, "Comment#11", Timestamp.valueOf("1970-01-01 00:00:00.017")));
        data.add(Row.of(18, 6L, "Comment#12", Timestamp.valueOf("1970-01-01 00:00:00.018")));
        data.add(Row.of(19, 6L, "Comment#13", Timestamp.valueOf("1970-01-01 00:00:00.019")));
        data.add(Row.of(20, 6L, "Comment#14", Timestamp.valueOf("1970-01-01 00:00:00.020")));
        data.add(Row.of(21, 6L, "Comment#15", Timestamp.valueOf("1970-01-01 00:00:00.021")));
        return data;
    }

    protected DataStream<Row> get4TupleDataStream(StreamExecutionEnvironment env) {
        List<Row> data = testData();
        Collections.shuffle(data);
        return env.fromCollection(data);
    }

    @Test
    void testReal() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                "CREATE TABLE upsertSink ("
                        + "  real_data float"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + getUrlWithCredentials()
                        + "',"
                        + "  'table-name'='"
                        + OUTPUT_TABLE4.getTableName()
                        + "'"
                        + ")");

        tEnv.executeSql("INSERT INTO upsertSink SELECT CAST(1.0 as FLOAT)").await();
        OUTPUT_TABLE4.checkContent(getDbMetadata(), Row.of(1.0f));
    }

    @Test
    void testUpsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table t =
                tEnv.fromDataStream(
                                get4TupleDataStream(env)
                                        .assignTimestampsAndWatermarks(
                                                new AscendingTimestampExtractor<Row>() {
                                                    @Override
                                                    public long extractAscendingTimestamp(
                                                            Row element) {
                                                        return Long.parseLong(
                                                                element.getField(0).toString());
                                                    }
                                                }))
                        .as("id", "num", "text", "ts");

        tEnv.createTemporaryView("T", t);
        tEnv.executeSql(
                "CREATE TABLE upsertSink ("
                        + "  cnt BIGINT,"
                        + "  lencnt BIGINT,"
                        + "  cTag INT,"
                        + "  ts TIMESTAMP(3),"
                        + "  PRIMARY KEY (cnt, cTag) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + getUrlWithCredentials()
                        + "',"
                        + "  'table-name'='"
                        + OUTPUT_TABLE1.getTableName()
                        + "',"
                        + "  'sink.buffer-flush.max-rows' = '2',"
                        + "  'sink.buffer-flush.interval' = '0',"
                        + "  'sink.max-retries' = '0'"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO upsertSink "
                                + "SELECT cnt, COUNT(len) AS lencnt, cTag, MAX(ts) AS ts "
                                + "FROM ( "
                                + "  SELECT len, COUNT(id) as cnt, cTag, MAX(ts) AS ts "
                                + "  FROM (SELECT id, CHAR_LENGTH(text) AS len, (CASE WHEN id > 0 THEN 1 ELSE 0 END) cTag, ts FROM T) "
                                + "  GROUP BY len, cTag "
                                + ") "
                                + "GROUP BY cnt, cTag")
                .await();
        OUTPUT_TABLE1.checkContent(
                getDbMetadata(),
                Row.of(1, 5, 1, testDataMap().get(6).getField(3)),
                Row.of(7, 1, 1, testDataMap().get(21).getField(3)),
                Row.of(9, 1, 1, testDataMap().get(15).getField(3)));
    }

    @Test
    void testAppend() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table t = tEnv.fromDataStream(get4TupleDataStream(env)).as("id", "num", "text", "ts");

        tEnv.registerTable("T", t);

        tEnv.executeSql(
                "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  num BIGINT,"
                        + "  ts TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + getUrlWithCredentials()
                        + "',"
                        + "  'table-name'='"
                        + OUTPUT_TABLE2.getTableName()
                        + "'"
                        + ")");

        tEnv.executeSql("INSERT INTO upsertSink SELECT id, num, ts FROM T WHERE id IN (2, 10, 20)")
                .await();
        OUTPUT_TABLE2.checkContent(
                getDbMetadata(),
                Stream.of(testDataMap().get(2), testDataMap().get(10), testDataMap().get(20))
                        .map(row -> Row.of(row.getField(0), row.getField(1), row.getField(3)))
                        .toArray(Row[]::new));
    }

    @Test
    void testBatchSink() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        tEnv.executeSql(
                "CREATE TABLE USER_RESULT("
                        + "NAME VARCHAR,"
                        + "SCORE BIGINT"
                        + ") WITH ( "
                        + "'connector' = 'jdbc',"
                        + "'url'='"
                        + getUrlWithCredentials()
                        + "',"
                        + "'table-name' = '"
                        + OUTPUT_TABLE3.getTableName()
                        + "',"
                        + "'sink.buffer-flush.max-rows' = '2',"
                        + "'sink.buffer-flush.interval' = '300ms',"
                        + "'sink.max-retries' = '4'"
                        + ")");

        TableResult tableResult =
                tEnv.executeSql(
                        "INSERT INTO USER_RESULT "
                                + "SELECT user_name, score "
                                + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                + "(42, 'Kim'), (1, 'Bob')) "
                                + "AS UserCountTable(score, user_name)");
        tableResult.await();

        OUTPUT_TABLE3.checkContent(
                getDbMetadata(),
                Row.of("Bob", 1),
                Row.of("Tom", 22),
                Row.of("Kim", 42),
                Row.of("Kim", 42),
                Row.of("Bob", 1));
    }

    @Test
    void testReadingFromChangelogSource() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String dataId = TestValuesTableFactory.registerData(TestData.userChangelog());
        tEnv.executeSql(
                "CREATE TABLE user_logs ("
                        + "  user_id STRING,"
                        + "  user_name STRING,"
                        + "  email STRING,"
                        + "  balance DECIMAL(18,2),"
                        + "  balance2 AS balance * 2"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'data-id' = '"
                        + dataId
                        + "',"
                        + " 'changelog-mode' = 'I,UA,UB,D'"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE user_sink ("
                        + "  user_id STRING PRIMARY KEY NOT ENFORCED,"
                        + "  user_name STRING,"
                        + "  email STRING,"
                        + "  balance DECIMAL(18,2),"
                        + "  balance2 DECIMAL(18,2)"
                        + ") WITH ("
                        + "  'connector' = 'jdbc',"
                        + "  'url'='"
                        + getUrlWithCredentials()
                        + "',"
                        + "  'table-name' = '"
                        + USER_TABLE.getTableName()
                        + "',"
                        + "  'sink.buffer-flush.max-rows' = '2',"
                        + "  'sink.buffer-flush.interval' = '0'"
                        + // disable async flush
                        ")");
        tEnv.executeSql("INSERT INTO user_sink SELECT * FROM user_logs").await();

        USER_TABLE.checkContent(
                getDbMetadata(),
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

    @Test
    void testFlushBufferWhenCheckpoint() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "jdbc");
        options.put("url", getUrlWithCredentials());
        options.put("table-name", OUTPUT_TABLE5.getTableName());
        options.put("sink.buffer-flush.interval", "0");

        ResolvedSchema schema =
                ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT().notNull()));

        DynamicTableSink tableSink = createTableSink(schema, options);

        SinkRuntimeProviderContext context = new SinkRuntimeProviderContext(false);
        SinkFunctionProvider sinkProvider =
                (SinkFunctionProvider) tableSink.getSinkRuntimeProvider(context);
        GenericJdbcSinkFunction<RowData> sinkFunction =
                (GenericJdbcSinkFunction<RowData>) sinkProvider.createSinkFunction();
        sinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(true, 1, 0));
        sinkFunction.open(new Configuration());
        sinkFunction.invoke(GenericRowData.of(1L), SinkContextUtil.forTimestamp(1));
        sinkFunction.invoke(GenericRowData.of(2L), SinkContextUtil.forTimestamp(1));

        OUTPUT_TABLE5.checkContent(getDbMetadata());
        sinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));
        OUTPUT_TABLE5.checkContent(getDbMetadata(), Row.of(1L), Row.of(2L));
        sinkFunction.close();
    }
}
