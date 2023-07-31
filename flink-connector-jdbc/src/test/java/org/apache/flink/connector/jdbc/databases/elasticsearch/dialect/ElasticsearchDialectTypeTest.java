package org.apache.flink.connector.jdbc.databases.elasticsearch.dialect;

import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeTest;

import java.util.Arrays;
import java.util.List;

/** The Elasticsearch params for {@link JdbcDialectTypeTest}. */
public class ElasticsearchDialectTypeTest extends JdbcDialectTypeTest {

    @Override
    protected String testDialect() {
        return "elasticsearch";
    }

    @Override
    protected List<TestItem> testData() {
        return Arrays.asList(
                createTestItem("VARCHAR"),
                createTestItem("BOOLEAN"),
                createTestItem("TINYINT"),
                createTestItem("SMALLINT"),
                createTestItem("INTEGER"),
                createTestItem("BIGINT"),
                createTestItem("FLOAT"),
                createTestItem("DOUBLE"),
                createTestItem("DATE"),
                createTestItem("TIMESTAMP(3)"),
                createTestItem("TIMESTAMP WITHOUT TIME ZONE"),
                createTestItem("VARBINARY"),

                // Not valid data
                createTestItem("CHAR", "The Elasticsearch dialect doesn't support type: CHAR(1)."),
                createTestItem(
                        "BINARY", "The Elasticsearch dialect doesn't support type: BINARY(1)."),
                createTestItem("TIME", "The Elasticsearch dialect doesn't support type: TIME(0)."),
                createTestItem(
                        "VARBINARY(10)",
                        "The Elasticsearch dialect doesn't support type: VARBINARY(10)."),
                createTestItem(
                        "DECIMAL(10, 4)",
                        "The Elasticsearch dialect doesn't support type: DECIMAL(10, 4)."));
    }
}
