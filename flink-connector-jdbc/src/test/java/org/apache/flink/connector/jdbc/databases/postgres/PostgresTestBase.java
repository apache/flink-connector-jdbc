package org.apache.flink.connector.jdbc.databases.postgres;

import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for Postgres testing. */
@ExtendWith(PostgresDatabase.class)
public interface PostgresTestBase extends JdbcTestBase {

    @Override
    default DatabaseMetadata getDbMetadata() {
        return PostgresDatabase.getMetadata();
    }
}
