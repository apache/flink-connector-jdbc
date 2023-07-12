package org.apache.flink.connector.jdbc.databases.clickhouse.table;

import org.apache.flink.connector.jdbc.databases.clickhouse.ClickHouseTestBase;
import org.apache.flink.connector.jdbc.databases.clickhouse.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSourceITCase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.ckTableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;

/** The Table Source ITCase for {@link ClickHouseDialect}. */
class ClickHouseTableSourceITCase extends JdbcDynamicTableSourceITCase
        implements ClickHouseTestBase {

    @Override
    protected ClickhouseTableRow createInputTable() {
        return ckTableRow(
                "jdbDynamicTableSource",
                pkField("id", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("user_id_int8", dbType("Int8"), DataTypes.TINYINT().notNull()),
                field("user_id_int16", dbType("Int16"), DataTypes.SMALLINT().notNull()),
                field("user_id_int32", dbType("Int32"), DataTypes.INT().notNull()),
                field("user_id_int64", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("price_float", dbType("Float32"), DataTypes.FLOAT()),
                field("price_double", dbType("Float64"), DataTypes.DOUBLE()),
                field("decimal_col", dbType("Decimal64(4)"), DataTypes.DECIMAL(10, 4)),
                field("user_date", dbType("Date"), DataTypes.DATE()),
                field("timestamp6_col", dbType("DateTime(6)"), DataTypes.TIMESTAMP(6)),
                field("decimal_column", dbType("Decimal(3,1)"), DataTypes.DECIMAL(3, 1)),
                field("bool_flag", dbType("Bool"), DataTypes.BOOLEAN()),
                field("message", dbType("String"), DataTypes.VARCHAR(100)),
                field(
                        "test_map",
                        dbType("Map(Int64,Int64)"),
                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT())));
    }

    @Override
    protected List<Row> getTestData() {
        TimeZone timeZone = TimeZone.getTimeZone("GTM+0");
        TimeZone.setDefault(timeZone);
        HashMap<Long, Long> map = new HashMap<>();
        map.put(1L, 2L);
        return Arrays.asList(
                Row.of(
                        1L,
                        (byte) 1,
                        (short) -32768,
                        -2147483648,
                        -9223372036854775808L,
                        -3.4e+38f,
                        -1.7e+308d,
                        BigDecimal.valueOf(100.1234),
                        LocalDate.parse("2023-01-01"),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                        BigDecimal.valueOf(-99.9),
                        true,
                        "this is a test message",
                        map),
                Row.of(
                        2L,
                        (byte) 2,
                        (short) 32767,
                        2147483647,
                        9223372036854775807L,
                        3.4e+38f,
                        1.7e+308d,
                        BigDecimal.valueOf(101.1234),
                        LocalDate.parse("2023-01-02"),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456"),
                        BigDecimal.valueOf(99.9),
                        false,
                        "this is a test message",
                        map));
    }
}
