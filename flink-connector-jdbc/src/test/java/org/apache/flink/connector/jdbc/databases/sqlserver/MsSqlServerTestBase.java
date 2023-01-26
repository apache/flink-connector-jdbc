package org.apache.flink.connector.jdbc.databases.sqlserver;

import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for MySqlServer testing. */
@ExtendWith(MsSqlServerDatabase.class)
public interface MsSqlServerTestBase extends JdbcTestBase {

    @Override
    default DatabaseMetadata getDbMetadata() {
        return MsSqlServerDatabase.getMetadata();
    }
}
