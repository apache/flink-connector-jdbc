package org.apache.flink.connector.jdbc.databases.mysql;

import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for MySql testing. */
@ExtendWith(MySqlDatabase.class)
public interface MySqlTestBase extends JdbcTestBase {

    @Override
    default DatabaseMetadata getDbMetadata() {
        return MySqlDatabase.getMetadata();
    }
}
