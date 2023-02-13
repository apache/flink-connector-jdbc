package org.apache.flink.connector.jdbc.databases.h2;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.databases.h2.H2XaDatabase;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for H2 Xa testing. */
@ExtendWith(H2XaDatabase.class)
public interface H2XaTestBase extends DatabaseTest {

    @Override
    default DatabaseMetadata getMetadata() {
        return H2XaDatabase.getMetadata();
    }
}
