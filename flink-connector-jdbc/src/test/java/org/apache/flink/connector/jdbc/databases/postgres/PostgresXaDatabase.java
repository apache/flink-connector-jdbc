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

package org.apache.flink.connector.jdbc.databases.postgres;

import org.apache.flink.connector.jdbc.databases.DatabaseExtension;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.DockerImageVersions;

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A Postgres database with XA enable. * */
public class PostgresXaDatabase extends DatabaseExtension {

    private static final PostgreSQLContainer<?> container =
            new PostgresXaContainer(DockerImageVersions.POSTGRES)
                    .withLogConsumer(
                            new Slf4jLogConsumer(LoggerFactory.getLogger(PostgresXaDatabase.class)))
                    .withMaxConnections(8)
                    .withMaxTransactions(50);

    private static DatabaseMetadata metadata;

    public static DatabaseMetadata getMetadata() {
        return metadata;
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        container.start();
        metadata = new PostgresMetadata(container, true);
        return metadata;
    }

    @Override
    protected void stopDatabase() throws Exception {
        container.stop();
    }

    /** {@link PostgreSQLContainer} with XA enabled (by setting max_prepared_transactions). */
    static class PostgresXaContainer extends PostgreSQLContainer<PostgresXaContainer> {
        private static final int SUPERUSER_RESERVED_CONNECTIONS = 1;
        private int maxConnections = SUPERUSER_RESERVED_CONNECTIONS + 1;
        private int maxTransactions = 1;

        public PostgresXaContainer(String dockerImageName) {
            super(DockerImageName.parse(dockerImageName));
        }

        public PostgresXaContainer withMaxConnections(int maxConnections) {
            checkArgument(
                    maxConnections > SUPERUSER_RESERVED_CONNECTIONS,
                    "maxConnections should be greater than superuser_reserved_connections");
            this.maxConnections = maxConnections;
            return this.self();
        }

        public PostgresXaContainer withMaxTransactions(int maxTransactions) {
            checkArgument(maxTransactions > 1, "maxTransactions should be greater 1");
            this.maxTransactions = maxTransactions;
            return this.self();
        }

        @Override
        public void start() {
            setCommand(
                    "postgres",
                    "-c",
                    "superuser_reserved_connections=" + SUPERUSER_RESERVED_CONNECTIONS,
                    "-c",
                    "max_connections=" + maxConnections,
                    "-c",
                    "max_prepared_transactions=" + maxTransactions,
                    "-c",
                    "fsync=off");
            super.start();
        }
    }
}
