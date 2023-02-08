package org.apache.flink.connector.jdbc.testutils;

import java.sql.Connection;
import java.sql.SQLException;

/** Table that can be manage by {@link DatabaseExtension}. */
public interface TableManaged {

    String getTableName();

    void createTable(Connection conn) throws SQLException;

    void deleteTable(Connection conn) throws SQLException;

    void dropTable(Connection conn) throws SQLException;
}
