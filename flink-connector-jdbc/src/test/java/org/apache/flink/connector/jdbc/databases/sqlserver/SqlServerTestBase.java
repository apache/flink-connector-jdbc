package org.apache.flink.connector.jdbc.databases.sqlserver;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.databases.sqlserver.SqlServerDatabase;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for SqlServer testing. */
@ExtendWith(SqlServerDatabase.class)
public interface SqlServerTestBase extends DatabaseTest {

    @Override
    default DatabaseMetadata getMetadata() {
        return SqlServerDatabase.getMetadata();
    }
}
