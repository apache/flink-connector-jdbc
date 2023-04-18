package org.apache.flink.connector.jdbc.databases.postgres;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.databases.postgres.PostgresDatabase;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for Postgres testing. */
@ExtendWith(PostgresDatabase.class)
public interface PostgresTestBase extends DatabaseTest {

    @Override
    default DatabaseMetadata getMetadata() {
        return PostgresDatabase.getMetadata();
    }
}
