package org.apache.flink.connector.jdbc.databases.oracle;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.databases.oracle.OracleXaDatabase;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for Oracle XA testing. */
@ExtendWith(OracleXaDatabase.class)
public interface OracleXaTestBase extends DatabaseTest {

    @Override
    default DatabaseMetadata getMetadata() {
        return OracleXaDatabase.getMetadata();
    }
}
