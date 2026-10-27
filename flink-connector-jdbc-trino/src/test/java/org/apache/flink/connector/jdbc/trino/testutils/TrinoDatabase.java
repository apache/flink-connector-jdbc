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

package org.apache.flink.connector.jdbc.trino.testutils;

import org.apache.flink.connector.jdbc.postgres.testutils.PostgresDatabase;
import org.apache.flink.connector.jdbc.postgres.testutils.PostgresImages;
import org.apache.flink.connector.jdbc.postgres.testutils.PostgresMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseResource;
import org.apache.flink.connector.jdbc.testutils.resources.DockerResource;
import org.apache.flink.util.FlinkRuntimeException;

import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.util.Arrays;

/** A Trino database for testing. */
public class TrinoDatabase extends DatabaseExtension implements TrinoImages, PostgresImages {

    private static final Network NETWORK = Network.newNetwork();
    private static final String CONTAINER_DB_ALIAS = "database";
    private static final Integer CONTAINER_DB_PORT = 5432;

    private static final PostgreSQLContainer<?> CONTAINER_DB =
            new PostgresDatabase.PostgresXaContainer(POSTGRES_16)
                    .withMaxConnections(10)
                    .withMaxTransactions(50)
                    .withExposedPorts(CONTAINER_DB_PORT)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(CONTAINER_DB_ALIAS);

    private static final TrinoContainer CONTAINER =
            new TrinoContainer(TRINO_IMAGE)
                    .withNetwork(NETWORK)
                    .withNetworkAliases("trino")
                    .dependsOn(CONTAINER_DB);

    private static TrinoMetadata metadata;

    public static TrinoMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new TrinoMetadata(CONTAINER);
        }
        return metadata;
    }

    public static PostgresMetadata getDatabaseMetadata() {
        if (!CONTAINER_DB.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        return new PostgresMetadata(CONTAINER_DB);
    }

    @Override
    protected DatabaseMetadata getMetadataDB() {
        return getMetadata();
    }

    @Override
    protected DatabaseResource getResource() {
        return new DatabaseResource() {
            @Override
            public void start() {
                try {
                    CONTAINER_DB.start();

                    String postgresContent =
                            "connector.name=postgresql\n"
                                    + String.format(
                                            "connection-url=jdbc:postgresql://%s:%s/test\n",
                                            CONTAINER_DB_ALIAS, CONTAINER_DB_PORT)
                                    + String.format(
                                            "connection-user=%s\n", CONTAINER_DB.getUsername())
                                    + String.format(
                                            "connection-password=%s\n", CONTAINER_DB.getPassword());

                    // Copied rather than bind mounted: Trino runs as uid 1000 and cannot read a
                    // 0600 temp file owned by another build user on Linux.
                    CONTAINER
                            .withDatabaseName("postgres/public")
                            .withCopyToContainer(
                                    Transferable.of(postgresContent),
                                    "/etc/trino/catalog/postgres.properties")
                            .waitingFor(Wait.forHttp("/ui/login.html").forStatusCode(200));
                    CONTAINER.start();
                } catch (Exception e) {
                    throw new FlinkRuntimeException(e);
                }
            }

            @Override
            public void stop() {
                Arrays.asList(CONTAINER, CONTAINER_DB)
                        .forEach(
                                container -> {
                                    container.stop();
                                    DockerResource.cleanContainers(container);
                                });
            }
        };
    }
}
