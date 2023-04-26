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

package org.apache.flink.connector.jdbc.dialect.vertica;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.databases.vertica.VerticaDatabase;
import org.apache.flink.connector.jdbc.databases.vertica.VerticaMetadata;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
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
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.jdbc.internal.JdbcTableOutputFormatTest.check;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;

/** The Table Sink ITCase for {@link VerticaDialect}. */
public class VerticaTableSinkITCase extends AbstractTestBase implements VerticaDatabase {

    public static final String BATCH_SINK_TABLE = "dynamicSinkForBatch";
    public static final String APPEND_TABLE = "dynamicSinkForAppend";
    public static final String REAL_TABLE = "realTable";
    public static final String CHECKPOINT_TABLE = "checkpointTable";

    @BeforeAll
    static void beforeAll() throws ClassNotFoundException, SQLException {
        try (Connection conn = new VerticaMetadata(CONTAINER).getConnection();
             Statement stat = conn.createStatement()) {
            stat.executeUpdate(
                    "CREATE TABLE "
                            + BATCH_SINK_TABLE
                            + " ("
                            + "NAME VARCHAR(20) NOT NULL,"
                            + "SCORE INT DEFAULT 0 NOT NULL)");

            stat.executeUpdate(
                    "CREATE TABLE "
                            + APPEND_TABLE
                            + " ("
                            + "id INT DEFAULT 0 NOT NULL,"
                            + "num INT DEFAULT 0 NOT NULL,"
                            + "ts DATETIME)");

            stat.executeUpdate("CREATE TABLE " + REAL_TABLE + " (real_data REAL)");

            stat.executeUpdate(
                    "CREATE TABLE " + CHECKPOINT_TABLE + " (" + "id BIGINT DEFAULT 0 NOT NULL)");
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        TestValuesTableFactory.clearAllData();
        try (Connection conn = new VerticaMetadata(CONTAINER).getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute("DROP TABLE " + BATCH_SINK_TABLE);
            stat.execute("DROP TABLE " + APPEND_TABLE);
            stat.execute("DROP TABLE " + REAL_TABLE);
            stat.execute("DROP TABLE " + CHECKPOINT_TABLE);
        }
    }

    public static DataStream<Tuple4<Integer, Long, String, Timestamp>> get4TupleDataStream(
            StreamExecutionEnvironment env) {
        List<Tuple4<Integer, Long, String, Timestamp>> data = new ArrayList<>();
        data.add(new Tuple4<>(1, 1L, "Hi", Timestamp.valueOf("1970-01-01 00:00:00.001")));
        data.add(new Tuple4<>(2, 2L, "Hello", Timestamp.valueOf("1970-01-01 00:00:00.002")));
        data.add(new Tuple4<>(3, 2L, "Hello world", Timestamp.valueOf("1970-01-01 00:00:00.003")));
        data.add(
                new Tuple4<>(
                        4,
                        3L,
                        "Hello world, how are you?",
                        Timestamp.valueOf("1970-01-01 00:00:00.004")));
        data.add(new Tuple4<>(5, 3L, "I am fine.", Timestamp.valueOf("1970-01-01 00:00:00.005")));
        data.add(
                new Tuple4<>(
                        6, 3L, "Luke Skywalker", Timestamp.valueOf("1970-01-01 00:00:00.006")));
        data.add(new Tuple4<>(7, 4L, "Comment#1", Timestamp.valueOf("1970-01-01 00:00:00.007")));
        data.add(new Tuple4<>(8, 4L, "Comment#2", Timestamp.valueOf("1970-01-01 00:00:00.008")));
        data.add(new Tuple4<>(9, 4L, "Comment#3", Timestamp.valueOf("1970-01-01 00:00:00.009")));
        data.add(new Tuple4<>(10, 4L, "Comment#4", Timestamp.valueOf("1970-01-01 00:00:00.010")));
        data.add(new Tuple4<>(11, 5L, "Comment#5", Timestamp.valueOf("1970-01-01 00:00:00.011")));
        data.add(new Tuple4<>(12, 5L, "Comment#6", Timestamp.valueOf("1970-01-01 00:00:00.012")));
        data.add(new Tuple4<>(13, 5L, "Comment#7", Timestamp.valueOf("1970-01-01 00:00:00.013")));
        data.add(new Tuple4<>(14, 5L, "Comment#8", Timestamp.valueOf("1970-01-01 00:00:00.014")));
        data.add(new Tuple4<>(15, 5L, "Comment#9", Timestamp.valueOf("1970-01-01 00:00:00.015")));
        data.add(new Tuple4<>(16, 6L, "Comment#10", Timestamp.valueOf("1970-01-01 00:00:00.016")));
        data.add(new Tuple4<>(17, 6L, "Comment#11", Timestamp.valueOf("1970-01-01 00:00:00.017")));
        data.add(new Tuple4<>(18, 6L, "Comment#12", Timestamp.valueOf("1970-01-01 00:00:00.018")));
        data.add(new Tuple4<>(19, 6L, "Comment#13", Timestamp.valueOf("1970-01-01 00:00:00.019")));
        data.add(new Tuple4<>(20, 6L, "Comment#14", Timestamp.valueOf("1970-01-01 00:00:00.020")));
        data.add(new Tuple4<>(21, 6L, "Comment#15", Timestamp.valueOf("1970-01-01 00:00:00.021")));

        Collections.shuffle(data);
        return env.fromCollection(data);
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
                        + getMetadata().getJdbcUrl()
                        + "',"
                        + "'table-name' = '"
                        + BATCH_SINK_TABLE
                        + "',"
                        + "  'username'='"
                        + getMetadata().getUsername()
                        + "',"
                        + "  'password'='"
                        + getMetadata().getPassword()
                        + "',"
                        + "'sink.buffer-flush.max-rows' = '2',"
                        + "'sink.buffer-flush.interval' = '300ms',"
                        + "'sink.max-retries' = '4'"
                        + ")");

        TableResult tableResult =
                tEnv.executeSql(
                        "INSERT INTO USER_RESULT\n"
                                + "SELECT user_name, score "
                                + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                + "(42, 'Kim'), (1, 'Bob')) "
                                + "AS UserCountTable(score, user_name)");
        tableResult.await();

        check(
                new Row[] {
                    Row.of("Bob", 1),
                    Row.of("Tom", 22),
                    Row.of("Kim", 42),
                    Row.of("Kim", 42),
                    Row.of("Bob", 1)
                },
                getMetadata().getJdbcUrlWithCredentials(),
                BATCH_SINK_TABLE,
                new String[] {"NAME", "SCORE"});
    }

    @Test
    void testAppend() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createTemporaryView(
                "T", tEnv.fromDataStream(get4TupleDataStream(env)).as("id", "num", "text", "ts"));

        tEnv.executeSql(
                "CREATE TABLE appendSink ("
                        + "  id BIGINT,"
                        + "  num BIGINT,"
                        + "  ts TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + getMetadata().getJdbcUrl()
                        + "',"
                        + "  'table-name'='"
                        + APPEND_TABLE
                        + "',"
                        + "  'username'='"
                        + getMetadata().getUsername()
                        + "',"
                        + "  'password'='"
                        + getMetadata().getPassword()
                        + "'"
                        + ")");

        tEnv.executeSql("INSERT INTO appendSink SELECT id, num, ts FROM T WHERE id IN (2, 10, 20)")
                .await();
        check(
                new Row[] {
                    Row.of(2, 2, Timestamp.valueOf("1970-01-01 00:00:00.002")),
                    Row.of(10, 4, Timestamp.valueOf("1970-01-01 00:00:00.01")),
                    Row.of(20, 6, Timestamp.valueOf("1970-01-01 00:00:00.02"))
                },
                getMetadata().getJdbcUrlWithCredentials(),
                APPEND_TABLE,
                new String[] {"id", "num", "ts"});
    }

    @Test
    void testReal() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tEnv.executeSql(
                "CREATE TABLE realSink ("
                        + "  real_data DOUBLE"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + getMetadata().getJdbcUrl()
                        + "',"
                        + "  'table-name'='"
                        + REAL_TABLE
                        + "',"
                        + "  'username'='"
                        + getMetadata().getUsername()
                        + "',"
                        + "  'password'='"
                        + getMetadata().getPassword()
                        + "'"
                        + ")");

        tEnv.executeSql("INSERT INTO realSink SELECT CAST(1.1 as FLOAT)").await();
        check(
                new Row[] {Row.of(1.1f)},
                getMetadata().getJdbcUrlWithCredentials(),
                REAL_TABLE,
                new String[] {"real_data"});
    }

    @Test
    void testFlushBufferWhenCheckpoint() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "jdbc");
        options.put("url", getMetadata().getJdbcUrl());
        options.put("table-name", CHECKPOINT_TABLE);
        options.put("sink.buffer-flush.interval", "0");
        options.put("username", getMetadata().getUsername());
        options.put("password", getMetadata().getPassword());

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

        check(
                new Row[] {},
                getMetadata().getJdbcUrlWithCredentials(),
                CHECKPOINT_TABLE,
                new String[] {"id"});
        sinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));
        check(
                new Row[] {Row.of(1L), Row.of(2L)},
                getMetadata().getJdbcUrlWithCredentials(),
                CHECKPOINT_TABLE,
                new String[] {"id"});
        sinkFunction.close();
    }
}
