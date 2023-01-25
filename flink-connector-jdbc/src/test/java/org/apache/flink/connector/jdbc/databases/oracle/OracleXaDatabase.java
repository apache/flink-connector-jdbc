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

package org.apache.flink.connector.jdbc.databases.oracle;

import org.apache.flink.connector.jdbc.databases.DatabaseExtension;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.DockerImageVersions;

import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** A Oracle database with XA enable. * */
@DisabledOnOs(OS.MAC)
public class OracleXaDatabase extends DatabaseExtension {

    @Container
    private final OracleContainer container =
            new OracleXaContainer(DockerImageVersions.ORACLE)
                    .withLogConsumer(
                            new Slf4jLogConsumer(LoggerFactory.getLogger(OracleXaDatabase.class)));

    private static OracleMetadata metadata;

    public static OracleMetadata getMetadata() {
        return metadata;
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        container.start();
        metadata = new OracleMetadata(container);
        return metadata;
    }

    @Override
    protected void stopDatabase() throws Exception {
        container.stop();
    }

    /** {@link OracleContainer} with XA enabled. */
    class OracleXaContainer extends OracleContainer {
        public OracleXaContainer(String dockerImageName) {
            super(DockerImageName.parse(dockerImageName));
        }
    }
}