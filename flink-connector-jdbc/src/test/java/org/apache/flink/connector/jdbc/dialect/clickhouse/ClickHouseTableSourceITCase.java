package org.apache.flink.connector.jdbc.dialect.clickhouse;

import org.apache.flink.connector.jdbc.databases.clickhouse.ClickHouseDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** The Table Source ITCase for {@link ClickHouseDialect}. */
class ClickHouseTableSourceITCase extends AbstractTestBase implements ClickHouseDatabase {

    private static final String INPUT_TABLE = "clickhouse_test_table";

    private static StreamExecutionEnvironment env;
    private static TableEnvironment tEnv;

    @BeforeAll
    static void beforeAll() throws ClassNotFoundException, SQLException {
        Class.forName(CONTAINER.getDriverClassName());
        try (Connection conn =
                        DriverManager.getConnection(
                                CONTAINER.getJdbcUrl(),
                                CONTAINER.getUsername(),
                                CONTAINER.getPassword());
                Statement statement = conn.createStatement()) {
            statement.execute(
                    "CREATE TABLE "
                            + INPUT_TABLE
                            + "(\n"
                            + "    user_id Int8,\n"
                            + "    user_id_uint8 UInt8,\n"
                            + "    user_id_int16 Int16,\n"
                            + "    user_id_uint16 UInt16,\n"
                            + "    user_id_int32 Int32,\n"
                            + "    user_id_uint32 UInt32,\n"
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
            statement.execute(
                    "insert  into "
                            + INPUT_TABLE
                            + " values (-128,0,-32768,0, -2147483648,0,-9223372036854775808,-3.4e+38, -1.7e+308,'2023-01-01','2023-01-01 15:35:03', -99.9,-99999.9999,-99999999999999.9999,true,'this is a test message')");
            statement.execute(
                    "insert  into "
                            + INPUT_TABLE
                            + " values (127,255,32767,65535,2147483647,4294967295,9223372036854775807,3.4e+38,1.7e+308,'2023-01-02','2023-01-01 16:35:05', 99.9, 99999.9999,99999999999999.9999,false,'this is a test message')");
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        Class.forName(CONTAINER.getDriverClassName());
        try (Connection conn =
                        DriverManager.getConnection(
                                CONTAINER.getJdbcUrl(),
                                CONTAINER.getUsername(),
                                CONTAINER.getPassword());
                Statement statement = conn.createStatement()) {
            statement.executeUpdate("DROP TABLE " + INPUT_TABLE);
        }
    }

    @BeforeEach
    void before() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    void testJdbcSource() throws Exception {
        createFlinkTable();
        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected =
                Stream.of(
                                "+I[-128, 0, -32768, 0, -2147483648, 0, -9223372036854775808, -3.4E38, -1.7E308, 2023-01-01, 2023-01-01T15:35:03, -99.9, -99999.9999, -99999999999999.9999, true, this is a test message]",
                                "+I[127, 255, 32767, 65535, 2147483647, 4294967295, 9223372036854775807, 3.4E38, 1.7E308, 2023-01-02, 2023-01-01T16:35:05, 99.9, 99999.9999, 99999999999999.9999, false, this is a test message]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);

        assert result.size() == 2;
    }

    @Test
    void testProject() throws Exception {
        createFlinkTable();
        Iterator<Row> collected =
                tEnv.executeSql(
                                "SELECT user_id,user_id_uint8,user_id_int16,user_id_uint16,user_id_int32,user_id_uint32,user_id_int64,decimal_column,decimal32_column,decimal64_column,bool_flag FROM "
                                        + INPUT_TABLE)
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected =
                Stream.of(
                                "+I[-128, 0, -32768, 0, -2147483648, 0, -9223372036854775808, -99.9, -99999.9999, -99999999999999.9999, true]",
                                "+I[127, 255, 32767, 65535, 2147483647, 4294967295, 9223372036854775807, 99.9, 99999.9999, 99999999999999.9999, false]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);

        assert result.size() == 2;
    }

    private void createFlinkTable() {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + " ("
                        + "user_id TINYINT NOT NULL,"
                        + "user_id_uint8 SMALLINT NOT NULL,"
                        + "user_id_int16 SMALLINT NOT NULL,"
                        + "user_id_uint16 INTEGER NOT NULL,"
                        + "user_id_int32 INTEGER NOT NULL,"
                        + "user_id_uint32 BIGINT NOT NULL,"
                        + "user_id_int64 BIGINT NOT NULL,"
                        + "price_float32 FLOAT NOT NULL,"
                        + "price_float64 DOUBLE NOT NULL,"
                        + "user_date DATE NOT NULL,"
                        + "user_timestamp TIMESTAMP(2) NOT NULL,"
                        + "decimal_column DECIMAL(3,1) NOT NULL,"
                        + "decimal32_column DECIMAL(9,4) NOT NULL,"
                        + "decimal64_column DECIMAL(18,4) NOT NULL,"
                        + "bool_flag BOOLEAN NOT NULL,"
                        + "message VARCHAR NOT NULL"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + getMetadata().getJdbcUrl()
                        + "',"
                        + "  'table-name'='"
                        + INPUT_TABLE
                        + "',"
                        + "  'username'='"
                        + getMetadata().getUsername()
                        + "',"
                        + "  'password'='"
                        + getMetadata().getPassword()
                        + "'"
                        + ")");
    }
}
