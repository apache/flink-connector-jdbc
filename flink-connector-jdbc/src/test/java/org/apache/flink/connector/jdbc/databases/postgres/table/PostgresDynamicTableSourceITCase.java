package org.apache.flink.connector.jdbc.databases.postgres.table;

import org.apache.flink.connector.jdbc.databases.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSourceITCase;
import org.apache.flink.connector.jdbc.templates.round2.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import static org.apache.flink.connector.jdbc.templates.round2.TableBuilder2.field;
import static org.apache.flink.connector.jdbc.templates.round2.TableBuilder2.tableRow;

/** The Table Source ITCase for Postgres. */
public class PostgresDynamicTableSourceITCase extends JdbcDynamicTableSourceITCase
        implements PostgresTestBase {

    @Override
    protected TableRow inputTable() {
        return tableRow(
                "jdbcDynamicTableSource",
                field("id", DataTypes.BIGINT().notNull()),
                field("timestamp6_col", DataTypes.TIMESTAMP(6)),
                field("timestamp9_col", DataTypes.TIMESTAMP(6)), // Timestamp only with precision 6
                field("time_col", "TIME", DataTypes.TIME()),
                field("real_col", "REAL", DataTypes.FLOAT()),
                field("double_col", "FLOAT", DataTypes.DOUBLE()), // No Double
                field("decimal_col", DataTypes.DECIMAL(10, 4)));
    }

    @Override
    protected String[] inputTableValues() {
        return new String[] {
            "1, TIMESTAMP '2020-01-01 15:35:00.123456', TIMESTAMP '2020-01-01 15:35:00.123456', TIME '15:35:00', 1.175E-37, 1.79769E+308, 100.1234",
            "2, TIMESTAMP '2020-01-01 15:36:01.123456', TIMESTAMP '2020-01-01 15:36:01.123456', TIME '15:36:01', -1.175E-37, -1.79769E+308, 101.1234"
        };
    }

    @Override
    protected Row[] rowTableValues() {
        return new Row[] {
            Row.of(
                    1,
                    "2020-01-01T15:35:00.123456",
                    "2020-01-01T15:35:00.123456",
                    "15:35",
                    1.175E-37,
                    1.79769E+308,
                    100.1234),
            Row.of(
                    2,
                    "2020-01-01T15:36:01.123456",
                    "2020-01-01T15:36:01.123456",
                    "15:36:01",
                    -1.175E-37,
                    -1.79769E+308,
                    101.1234)
        };
    }
}
