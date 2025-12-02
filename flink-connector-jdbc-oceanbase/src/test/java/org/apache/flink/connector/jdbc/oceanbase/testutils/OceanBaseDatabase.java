/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.oceanbase.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseResource;
import org.apache.flink.connector.jdbc.testutils.resources.DockerResource;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.oceanbase.OceanBaseCEContainer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** OceanBase database for testing. */
public class OceanBaseDatabase extends DatabaseExtension implements OceanBaseImages {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseDatabase.class);

    private static final String ZONE_OFFSET =
            DateTimeFormatter.ofPattern("xxx")
                    .format(ZoneId.systemDefault().getRules().getOffset(Instant.now()));

    private static final OceanBaseCEContainer CONTAINER =
            new OceanBaseContainer(OCEANBASE_CE_4)
                    .withPassword("123456")
                    // Resource Strategy: mini ensures lowest possible footprint
                    .withEnv("MODE", "mini")
                    // Memory Optimization: Lower limit manually
                    .withEnv("OB_MEMORY_LIMIT", "2G")
                    .withEnv("OB_SYSTEM_MEMORY", "1G")
                    // Storage Optimization: Minimize pre-allocated file sizes
                    .withEnv("OB_DATAFILE_SIZE", "1G")
                    .withEnv("OB_LOG_DISK_SIZE", "2G")
                    // Performance: Align with GitHub Runner's 2-core limit
                    .withEnv("OB_CPU_COUNT", "2")
                    .withUrlParam("useSSL", "false")
                    .withUrlParam("serverTimezone", ZONE_OFFSET)
                    .withCopyToContainer(
                            Transferable.of(
                                    String.format("SET GLOBAL time_zone = '%s';", ZONE_OFFSET)),
                            "/root/boot/init.d/init.sql")
                    .waitingFor(
                            Wait.forLogMessage(".*boot success!.*", 1)
                                    .withStartupTimeout(Duration.ofMinutes(2)))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    private static OceanBaseMetadata metadata;

    public static OceanBaseMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new OceanBaseMetadata(CONTAINER);
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
