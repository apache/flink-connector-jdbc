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

package org.apache.flink.connector.jdbc.testutils.databases.postgres;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.util.FlinkRuntimeException;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A Postgres database for testing. */
public class PostgresDatabase extends DatabaseExtension implements PostgresImages {

    private static final PostgreSQLContainer<?> CONTAINER =
            new PostgresXaContainer(POSTGRES_15).withMaxConnections(10).withMaxTransactions(50);

    private static PostgresMetadata metadata;

    public static PostgresMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new PostgresMetadata(CONTAINER, true);
        }
        return metadata;
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        CONTAINER.start();
        return getMetadata();
    }

    @Override
    protected void stopDatabase() throws Exception {
        CONTAINER.stop();
        metadata = null;
    }

    /** {@link PostgreSQLContainer} with XA enabled (by setting max_prepared_transactions). */
    public static class PostgresXaContainer extends PostgreSQLContainer<PostgresXaContainer> {
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
