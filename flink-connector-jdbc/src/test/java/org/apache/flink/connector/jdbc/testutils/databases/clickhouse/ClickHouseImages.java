package org.apache.flink.connector.jdbc.testutils.databases.clickhouse;

import org.testcontainers.utility.DockerImageName;

/** clickhouse images. */
public interface ClickHouseImages {

    DockerImageName CLICKHOUSE_IMAGE_23 =
            DockerImageName.parse("clickhouse/clickhouse-server:23.4.2")
                    .asCompatibleSubstituteFor("yandex/clickhouse-server");
}
