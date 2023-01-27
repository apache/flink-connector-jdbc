package org.apache.flink.connector.jdbc.templates.round2;

import java.sql.Connection;
import java.sql.SQLException;

/** Table that manage itself. * */
public interface TableManaged {

    String getTableName();

    void createTable(Connection conn) throws SQLException;

    void deleteTable(Connection conn) throws SQLException;

    void dropTable(Connection conn) throws SQLException;
}
