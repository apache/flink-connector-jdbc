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

package org.apache.flink.connector.jdbc.spanner.table;

import org.apache.flink.connector.jdbc.core.table.sink.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.spanner.SpannerTestBase;
import org.apache.flink.connector.jdbc.spanner.database.dialect.SpannerDialect;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.spanner.testutils.tables.SpannerTableRow.spannerTableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;
import static org.assertj.core.api.Assertions.assertThat;

/** The Table Sink ITCase for {@link SpannerDialect}. */
class SpannerDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase implements SpannerTestBase {

    @Override
    protected TableRow createUpsertOutputTable() {
        return spannerTableRow(
                "dynamicSinkForUpsert",
                pkField("cnt", dbType("INT64 NOT NULL"), DataTypes.BIGINT().notNull()),
                field("lencnt", dbType("INT64 NOT NULL"), DataTypes.BIGINT().notNull()),
                pkField("cTag", dbType("INT64 NOT NULL"), DataTypes.INT().notNull()),
                field("ts", dbType("TIMESTAMP"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createAppendOutputTable() {
        return spannerTableRow(
                "dynamicSinkForAppend",
                // Spanner requires primary keys.
                pkField("id", dbType("INT64 NOT NULL"), DataTypes.INT().notNull()),
                field("num", dbType("INT64 NOT NULL"), DataTypes.BIGINT().notNull()),
                field("ts", dbType("TIMESTAMP"), DataTypes.TIMESTAMP()));
    }

    protected TableRow createBatchOutputTable() {
        return spannerTableRow(
                "dynamicSinkForBatch",
                // Spanner requires primary keys.
                pkField("UUID", dbType("STRING(36) NOT NULL"), DataTypes.STRING().notNull()),
                field("NAME", dbType("STRING(20)"), DataTypes.STRING()),
                field("SCORE", dbType("INT64 NOT NULL"), DataTypes.BIGINT().notNull()));
    }

    protected TableRow createRealOutputTable() {
        return spannerTableRow(
                "REAL_TABLE",
                // Spanner requires primary keys.
                pkField("uuid", dbType("STRING(36) NOT NULL"), DataTypes.STRING().notNull()),
                field("real_data", dbType("FLOAT32"), DataTypes.FLOAT()));
    }

    protected TableRow createCheckpointOutputTable() {
        return spannerTableRow(
                "checkpointTable",
                pkField("id", dbType("INT64 NOT NULL"), DataTypes.BIGINT().notNull()));
    }

    protected TableRow createUserOutputTable() {
        return spannerTableRow(
                "USER_TABLE",
                pkField("user_id", dbType("STRING(20) NOT NULL"), DataTypes.VARCHAR(20).notNull()),
                field("user_name", dbType("STRING(20) NOT NULL"), DataTypes.VARCHAR(20).notNull()),
                field("email", dbType("STRING(255)"), DataTypes.VARCHAR(255)),
                field("balance", dbType("NUMERIC"), DataTypes.DECIMAL(18, 2)),
                field("balance2", dbType("NUMERIC"), DataTypes.DECIMAL(18, 2)));
    }

    @Override
    protected List<Row> testUserData() {
        // NUMERIC in GoogleSQL is a fixed precision numeric type (precision=38 and scale=9)
        // and cannot be used to store arbitrary precision numeric data.
        // https://cloud.google.com/spanner/docs/storing-numeric-data
        return Arrays.asList(
                Row.of(
                        "user1",
                        "Tom",
                        "tom123@gmail.com",
                        new BigDecimal("8.1"),
                        new BigDecimal("16.2")),
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
                        new BigDecimal("11.3"),
                        new BigDecimal("22.6")));
    }

    @Override
    protected List<Row> testData() {
        // This is probably due to the implementation of the JDBC driver (it uses the Calendar class
        // internally),
        // but when using 1970-01-01, there is a possibility of a 1-second error occurring
        // if the time zone of the execution environment is not UTC.
        return Arrays.asList(
                Row.of(1, 1L, "Hi", Timestamp.valueOf("1970-01-02 00:00:00.001")),
                Row.of(2, 2L, "Hello", Timestamp.valueOf("1970-01-02 00:00:00.002")),
                Row.of(3, 2L, "Hello world", Timestamp.valueOf("1970-01-02 00:00:00.003")),
                Row.of(
                        4,
                        3L,
                        "Hello world, how are you?",
                        Timestamp.valueOf("1970-01-02 00:00:00.004")),
                Row.of(5, 3L, "I am fine.", Timestamp.valueOf("1970-01-02 00:00:00.005")),
                Row.of(6, 3L, "Luke Skywalker", Timestamp.valueOf("1970-01-02 00:00:00.006")),
                Row.of(7, 4L, "Comment#1", Timestamp.valueOf("1970-01-02 00:00:00.007")),
                Row.of(8, 4L, "Comment#2", Timestamp.valueOf("1970-01-02 00:00:00.008")),
                Row.of(9, 4L, "Comment#3", Timestamp.valueOf("1970-01-02 00:00:00.009")),
                Row.of(10, 4L, "Comment#4", Timestamp.valueOf("1970-01-02 00:00:00.010")),
                Row.of(11, 5L, "Comment#5", Timestamp.valueOf("1970-01-02 00:00:00.011")),
                Row.of(12, 5L, "Comment#6", Timestamp.valueOf("1970-01-02 00:00:00.012")),
                Row.of(13, 5L, "Comment#7", Timestamp.valueOf("1970-01-02 00:00:00.013")),
                Row.of(14, 5L, "Comment#8", Timestamp.valueOf("1970-01-02 00:00:00.014")),
                Row.of(15, 5L, "Comment#9", Timestamp.valueOf("1970-01-02 00:00:00.015")),
                Row.of(16, 6L, "Comment#10", Timestamp.valueOf("1970-01-02 00:00:00.016")),
                Row.of(17, 6L, "Comment#11", Timestamp.valueOf("1970-01-02 00:00:00.017")),
                Row.of(18, 6L, "Comment#12", Timestamp.valueOf("1970-01-02 00:00:00.018")),
                Row.of(19, 6L, "Comment#13", Timestamp.valueOf("1970-01-02 00:00:00.019")),
                Row.of(20, 6L, "Comment#14", Timestamp.valueOf("1970-01-02 00:00:00.020")),
                Row.of(21, 6L, "Comment#15", Timestamp.valueOf("1970-01-02 00:00:00.021")));
    }

    @Test
    @Override
    protected void testReal() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        String tableName = "realSink";
        tEnv.executeSql(realOutputTable.getCreateQueryForFlink(getMetadata(), tableName));

        tEnv.executeSql(String.format("INSERT INTO %s SELECT 'a', CAST(1.0 as FLOAT)", tableName))
                .await();

        assertThat(realOutputTable.selectAllTable(getMetadata()))
                .containsExactly(Row.of("a", 1.0f));
    }

    @Test
    @Override
    protected void testBatchSink() throws Exception {
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
                                        + " SELECT uuid, user_name, score "
                                        + " FROM (VALUES "
                                        + "('a', 1, 'Bob'), "
                                        + "('b', 22, 'Tom'), "
                                        + "('c', 42, 'Kim'), "
                                        + "('d', 42, 'Kim'), "
                                        + "('e', 1, 'Bob')) "
                                        + " AS UserCountTable(uuid, score, user_name) ",
                                tableName));
        tableResult.await();

        assertThat(batchOutputTable.selectAllTable(getMetadata()))
                .containsExactlyInAnyOrder(
                        Row.of("a", "Bob", 1L),
                        Row.of("b", "Tom", 22L),
                        Row.of("c", "Kim", 42L),
                        Row.of("d", "Kim", 42L),
                        Row.of("e", "Bob", 1L));
    }
}
