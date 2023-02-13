package org.apache.flink.connector.jdbc.databases.derby;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.databases.derby.DerbyDatabase;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for Derby testing. */
@ExtendWith(DerbyDatabase.class)
public interface DerbyTestBase extends DatabaseTest {

    @Override
    default DatabaseMetadata getMetadata() {
        return DerbyDatabase.getMetadata();
    }
}
