package org.apache.flink.connector.jdbc.databases.clickhouse.table;

import org.apache.flink.connector.jdbc.databases.clickhouse.ClickHouseTestBase;
import org.apache.flink.connector.jdbc.databases.clickhouse.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.ckTableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;

/** The Table Sink ITCase for {@link ClickHouseDialect}. */
class ClickhouseTableSinkITCase extends JdbcDynamicTableSinkITCase implements ClickHouseTestBase {
    @Override
    protected TableRow createUpsertOutputTable() {
        return ckTableRow(
                "dynamicSinkForUpsert",
                pkField("cnt", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("lencnt", dbType("Int64"), DataTypes.BIGINT().notNull()),
                pkField("cTag", DataTypes.INT().notNull()),
                field("ts", dbType("DateTime"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createAppendOutputTable() {
        TimeZone timeZone = TimeZone.getTimeZone("GTM+0");
        TimeZone.setDefault(timeZone);
        return ckTableRow(
                "dynamicSinkForAppend",
                field("id", DataTypes.INT().notNull()),
                field("num", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("ts", dbType("DateTime64"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createBatchOutputTable() {
        return ckTableRow(
                "dynamicSinkForBatch",
                field("NAME", DataTypes.VARCHAR(20).notNull()),
                field("SCORE", dbType("Int64"), DataTypes.BIGINT().notNull()));
    }

    @Override
    protected TableRow createUserOutputTable() {
        return ckTableRow(
                "USER_TABLE",
                pkField("user_id", DataTypes.VARCHAR(20).notNull()),
                field("user_name", DataTypes.VARCHAR(20).notNull()),
                field("email", DataTypes.VARCHAR(255)),
                field("balance", DataTypes.DECIMAL(18, 2)),
                field("balance2", DataTypes.DECIMAL(18, 2)));
    }

    @Override
    protected TableRow createRealOutputTable() {
        return ckTableRow("REAL_TABLE", field("real_data", dbType("REAL"), DataTypes.FLOAT()));
    }

    @Override
    protected TableRow createCheckpointOutputTable() {
        return ckTableRow(
                "checkpointTable", field("id", dbType("Int64"), DataTypes.BIGINT().notNull()));
    }

    @Override
    protected List<Row> testUserData() {
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
}
