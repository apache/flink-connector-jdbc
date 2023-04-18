package org.apache.flink.connector.jdbc.databases.mysql;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.databases.mysql.MySqlDatabase;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for MySql testing. */
@ExtendWith(MySqlDatabase.class)
public interface MySqlTestBase extends DatabaseTest {

    @Override
    default DatabaseMetadata getMetadata() {
        return MySqlDatabase.getMetadata();
    }
}
