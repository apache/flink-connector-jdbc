/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.clickhouse.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseResource;
import org.apache.flink.connector.jdbc.testutils.resources.DockerResource;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/** ClickHouse database for testing. */
public class ClickHouseDatabase extends DatabaseExtension implements ClickHouseImages {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseDatabase.class);

    private static final ClickHouseContainer CONTAINER =
            new ClickHouseContainer(CLICKHOUSE_LATEST)
                    .withUsername("flink_test_user")
                    .withPassword("flink_test_password")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    private static ClickHouseMetadata metadata;

    public static ClickHouseMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new ClickHouseMetadata(CONTAINER);
        }
        return metadata;
    }

    @Override
    protected DatabaseMetadata getMetadataDB() {
        return getMetadata();
    }

    @Override
    protected DatabaseResource getResource() {
        return new DockerResource(CONTAINER);
    }
}
