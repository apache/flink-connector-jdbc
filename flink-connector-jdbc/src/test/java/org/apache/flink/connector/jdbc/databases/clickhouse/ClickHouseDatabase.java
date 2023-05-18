package org.apache.flink.connector.jdbc.databases.clickhouse;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.DatabaseTest;

import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** clickhouse database for testing. */
@Testcontainers
public interface ClickHouseDatabase extends DatabaseTest, ClickHouseImages {

    @Container
    ClickHouseContainer CONTAINER =
            new ClickHouseContainer(
                    DockerImageName.parse("clickhouse/clickhouse-server:23.4.2")
                            .asCompatibleSubstituteFor("yandex/clickhouse-server"));

    @Override
    default DatabaseMetadata getMetadata() {
        return new ClickHouseMetadata(CONTAINER);
    }
}
