package org.apache.flink.connector.jdbc.databases.oracle;

import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for Oracle testing. */
@ExtendWith(OracleDatabase.class)
public interface OracleTestBase extends JdbcTestBase {

    @Override
    default DatabaseMetadata getDbMetadata() {
        return OracleDatabase.getMetadata();
    }
}
