package org.apache.flink.connector.jdbc.databases.clickhouse.dialect;

import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeTest;

import java.util.Arrays;
import java.util.List;

/** The Clickhouse params for {@link JdbcDialectTypeTest}. */
public class ClickHouseDialectTypeTest extends JdbcDialectTypeTest {

    @Override
    protected String testDialect() {
        return "clickhouse";
    }

    @Override
    protected List<TestItem> testData() {
        return Arrays.asList(
                createTestItem("CHAR"),
                createTestItem("VARCHAR"),
                createTestItem("BOOLEAN"),
                createTestItem("TINYINT"),
                createTestItem("SMALLINT"),
                createTestItem("INTEGER"),
                createTestItem("BIGINT"),
                createTestItem("FLOAT"),
                createTestItem("DOUBLE"),
                createTestItem("DECIMAL(10, 4)"),
                createTestItem("DECIMAL(38, 18)"),
                createTestItem("DATE"),
                createTestItem("TIMESTAMP(3)"),
                createTestItem("TIMESTAMP WITHOUT TIME ZONE"),
                createTestItem("VARBINARY", "The ClickHouse dialect doesn't support type: BYTES"),

                // Not valid data
                createTestItem("BINARY", "The ClickHouse dialect doesn't support type: BINARY(1)."),
                createTestItem(
                        "VARBINARY(10)",
                        "The ClickHouse dialect doesn't support type: VARBINARY(10)."));
    }
}
