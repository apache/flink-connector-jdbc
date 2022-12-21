package org.apache.flink.connector.jdbc.dialect.trino;

import org.apache.flink.connector.jdbc.internal.JdbcTableOutputFormatTest;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Basic class for testing Trino jdbc. */
@Testcontainers
public abstract class TrinoTestBase extends AbstractTestBase {

    private static final String TRINO_DOCKER_IMAGE = "trinodb/trino:403";

    @Container
    protected static final TrinoContainer TRINO_CONTAINER =
            new TrinoContainer(TRINO_DOCKER_IMAGE).withDatabaseName("memory/default");

    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName(TRINO_CONTAINER.getDriverClassName());
        return DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
    }

    public static String getJdbcUrl() {
        return TRINO_CONTAINER.getJdbcUrl();
    }

    public static String getUsername() {
        return TRINO_CONTAINER.getUsername();
    }

    public static String getPassword() {
        return TRINO_CONTAINER.getPassword();
    }

    public static void check(Row[] rows, String table, String[] fields)
            throws SQLException, ClassNotFoundException {
        JdbcTableOutputFormatTest.check(rows, getConnection(), table, fields);
    }
}
