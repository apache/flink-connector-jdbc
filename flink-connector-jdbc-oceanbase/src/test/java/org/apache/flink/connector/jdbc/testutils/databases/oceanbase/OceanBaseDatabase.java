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

package org.apache.flink.connector.jdbc.testutils.databases.oceanbase;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.Connection;
import java.sql.Statement;

/** OceanBase database for testing. */
public class OceanBaseDatabase extends DatabaseExtension implements OceanBaseImages {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseDatabase.class);

    private static final OceanBaseContainer CONTAINER =
            new OceanBaseContainer(OCEANBASE_CE_4)
                    .withEnv("MODE", "slim")
                    .withEnv("FASTBOOT", "true")
                    .withEnv("OB_DATAFILE_SIZE", "1G")
                    .withEnv("OB_LOG_DISK_SIZE", "4G")
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
    protected DatabaseMetadata startDatabase() throws Exception {
        CONTAINER.start();
        try (Connection connection = getMetadata().getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET GLOBAL time_zone = '+00:00'");
        }
        return getMetadata();
    }

    @Override
    protected void stopDatabase() throws Exception {
        CONTAINER.stop();
        metadata = null;
    }
}
