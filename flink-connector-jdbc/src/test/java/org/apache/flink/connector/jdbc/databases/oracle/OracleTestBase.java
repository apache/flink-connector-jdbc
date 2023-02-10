package org.apache.flink.connector.jdbc.databases.oracle;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.databases.oracle.OracleDatabase;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for Oracle testing. */
@ExtendWith(OracleDatabase.class)
public interface OracleTestBase extends DatabaseTest {

    @Override
    default DatabaseMetadata getMetadata() {
        return OracleDatabase.getMetadata();
    }
}
