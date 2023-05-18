package org.apache.flink.connector.jdbc.dialect.clickhouse;

import org.apache.flink.connector.jdbc.databases.clickhouse.ClickHouseDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import static org.apache.flink.connector.jdbc.internal.JdbcTableOutputFormatTest.check;
import static org.apache.flink.table.api.Expressions.row;

/** The Table Sink ITCase for {@link ClickHouseDialect}. */
class ClickhouseTableSinkITCase extends AbstractTestBase implements ClickHouseDatabase {

    public static final String OUTPUT_TABLE1 = "dynamicSinkForInsert";
    public static final String OUTPUT_TABLE3 = "dynamicSinkForBatch";
    public static final String OUTPUT_TABLE4 = "REAL_TABLE";

    @BeforeAll
    static void beforeAll() throws ClassNotFoundException, SQLException {
        Class.forName(CONTAINER.getDriverClassName());
        try (Connection conn =
                        DriverManager.getConnection(
                                CONTAINER.getJdbcUrl(),
                                CONTAINER.getUsername(),
                                CONTAINER.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "CREATE TABLE "
                            + OUTPUT_TABLE1
                            + "(\n"
                            + "    user_id Int8,\n"
                            + "    user_id_int16 Int16,\n"
                            + "    user_id_int32 Int32,\n"
                            + "    user_id_int64 Int64,\n"
                            + "    price_float32   Float32,\n"
                            + "    price_float64    Float64,\n"
                            + "    user_date Date,\n"
                            + "    user_timestamp DateTime,\n"
                            + "    decimal_column Decimal(3,1),\n"
                            + "    decimal32_column Decimal32(4),\n"
                            + "    decimal64_column Decimal64(4),\n"
                            + "    bool_flag Bool,\n"
                            + "    message String\n"
                            + ")\n"
                            + "ENGINE = MergeTree\n"
                            + "PRIMARY KEY (user_id, user_timestamp)");
            stat.execute(
                    "CREATE TABLE "
                            + OUTPUT_TABLE3
                            + " (user_id Int8,"
                            + "message String,"
                            + "user_timestamp DateTime) ENGINE = MergeTree PRIMARY KEY (user_id, user_timestamp)");
            stat.execute(
                    "CREATE TABLE "
                            + OUTPUT_TABLE4
                            + " (user_id Int8,"
                            + "real_data Float32,"
                            + "user_timestamp DateTime) ENGINE = MergeTree PRIMARY KEY (user_id, user_timestamp)");
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        TestValuesTableFactory.clearAllData();
        Class.forName(CONTAINER.getDriverClassName());
        try (Connection conn =
                        DriverManager.getConnection(
                                CONTAINER.getJdbcUrl(),
                                CONTAINER.getUsername(),
                                CONTAINER.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute("DROP TABLE " + OUTPUT_TABLE1);
            stat.execute("DROP TABLE " + OUTPUT_TABLE3);
            stat.execute("DROP TABLE " + OUTPUT_TABLE4);
        }
    }

    @Test
    void testAllDataTypes() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());

        tEnv.createTemporaryView(
                "myTable",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("user_id", DataTypes.TINYINT()),
                                DataTypes.FIELD("user_id_int16", DataTypes.SMALLINT()),
                                DataTypes.FIELD("user_id_int32", DataTypes.INT()),
                                DataTypes.FIELD("user_id_int64", DataTypes.BIGINT()),
                                DataTypes.FIELD("price_float32", DataTypes.FLOAT()),
                                DataTypes.FIELD("price_float64", DataTypes.DOUBLE()),
                                DataTypes.FIELD("user_date", DataTypes.DATE()),
                                DataTypes.FIELD("user_timestamp", DataTypes.TIMESTAMP(3)),
                                DataTypes.FIELD("decimal_column", DataTypes.DECIMAL(3, 1)),
                                DataTypes.FIELD("decimal32_column", DataTypes.DECIMAL(9, 4)),
                                DataTypes.FIELD("bool_flag", DataTypes.BOOLEAN()),
                                DataTypes.FIELD("message", DataTypes.STRING())),
                        row(
                                -128,
                                -32768,
                                -2147483648,
                                -9223372036854775808L,
                                -3.4e+38f,
                                -1.7e+308d,
                                "2023-01-01",
                                Timestamp.valueOf("2023-01-01 15:35:12").toInstant(),
                                -99.9f,
                                -99999.9999d,
                                true,
                                "this is a test message"),
                        row(
                                127,
                                32767,
                                2147483647,
                                9223372036854775807L,
                                3.4e+38f,
                                1.7e+308d,
                                "2023-01-02",
                                Timestamp.valueOf("2023-01-01 16:35:23").toInstant(),
                                99.9f,
                                99999.9999d,
                                false,
                                "this is a test message")));

        tEnv.executeSql(
                "CREATE TABLE "
                        + OUTPUT_TABLE1
                        + " ("
                        + "user_id TINYINT NOT NULL,"
                        + "user_id_int16 SMALLINT NOT NULL,"
                        + "user_id_int32 INTEGER NOT NULL,"
                        + "user_id_int64 BIGINT NOT NULL,"
                        + "price_float32 FLOAT NOT NULL,"
                        + "price_float64 DOUBLE NOT NULL,"
                        + "user_date DATE NOT NULL,"
                        + "user_timestamp TIMESTAMP(6) NOT NULL,"
                        + "decimal_column DECIMAL(3,1) NOT NULL,"
                        + "decimal32_column DECIMAL(9,4) NOT NULL,"
                        + "bool_flag BOOLEAN NOT NULL,"
                        + "message VARCHAR NOT NULL"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + getMetadata().getJdbcUrl()
                        + "',"
                        + "  'table-name'='"
                        + OUTPUT_TABLE1
                        + "',"
                        + "  'username'='"
                        + getMetadata().getUsername()
                        + "',"
                        + "  'password'='"
                        + getMetadata().getPassword()
                        + "'"
                        + ")");

        tEnv.executeSql("INSERT INTO " + OUTPUT_TABLE1 + " select * from myTable").await();

        check(
                new Row[] {
                    Row.of(
                            -128,
                            -32768,
                            -2147483648,
                            -9223372036854775808L,
                            -3.4e+38f,
                            -1.7e+308d,
                            "2023-01-01",
                            Timestamp.valueOf("2023-01-01 15:35:12")
                                    .toInstant()
                                    .toString()
                                    .replace("Z", ""),
                            -99.9f,
                            -99999.9999d,
                            true,
                            "this is a test message"),
                    Row.of(
                            127,
                            32767,
                            2147483647,
                            9223372036854775807L,
                            3.4e+38f,
                            1.7e+308d,
                            "2023-01-02",
                            Timestamp.valueOf("2023-01-01 16:35:23")
                                    .toInstant()
                                    .toString()
                                    .replace("Z", ""),
                            99.9f,
                            99999.9999d,
                            false,
                            "this is a test message")
                },
                getMetadata().getJdbcUrlWithCredentials(),
                OUTPUT_TABLE1,
                new String[] {
                    "user_id",
                    "user_id_int16",
                    "user_id_int32",
                    "user_id_int64",
                    "price_float32",
                    "price_float64",
                    "user_date",
                    "user_timestamp",
                    "decimal_column",
                    "decimal32_column",
                    "bool_flag",
                    "message"
                });
    }

    @Test
    void testStreamSink() throws Exception {
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
                        + getMetadata().getJdbcUrlWithCredentials()
                        + "',"
                        + "  'table-name'='"
                        + OUTPUT_TABLE4
                        + "'"
                        + ")");

        tEnv.executeSql("INSERT INTO upsertSink SELECT CAST(1.1 as FLOAT)").await();
        check(
                new Row[] {Row.of(1.1f)},
                getMetadata().getJdbcUrlWithCredentials(),
                "REAL_TABLE",
                new String[] {"real_data"});
    }

    @Test
    void testBatchSink() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.executeSql(
                "CREATE TABLE USER_RESULT("
                        + "user_id BIGINT,"
                        + "message VARCHAR"
                        + ") WITH ( "
                        + "'connector' = 'jdbc',"
                        + "'url'='"
                        + getMetadata().getJdbcUrlWithCredentials()
                        + "',"
                        + "'table-name' = '"
                        + OUTPUT_TABLE3
                        + "',"
                        + "'sink.buffer-flush.max-rows' = '10',"
                        + "'sink.buffer-flush.interval' = '300ms',"
                        + "'sink.max-retries' = '4'"
                        + ")");

        TableResult tableResult =
                tEnv.executeSql(
                        "INSERT INTO USER_RESULT\n"
                                + "SELECT user_id, message "
                                + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                + "(42, 'Kim'), (1, 'Bob')) "
                                + "AS UserCountTable(user_id, message)");
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
                OUTPUT_TABLE3,
                new String[] {"message", "user_id"});
    }
}
