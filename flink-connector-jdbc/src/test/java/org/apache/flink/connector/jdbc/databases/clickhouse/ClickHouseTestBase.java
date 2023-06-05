package org.apache.flink.connector.jdbc.databases.clickhouse;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.databases.clickhouse.ClickHouseDatabase;

import org.junit.jupiter.api.extension.ExtendWith;

/** clickhouse database for testing. */
@ExtendWith(ClickHouseDatabase.class)
public interface ClickHouseTestBase extends DatabaseTest {

    @Override
    default DatabaseMetadata getMetadata() {
        return ClickHouseDatabase.getMetadata();
    }
}
