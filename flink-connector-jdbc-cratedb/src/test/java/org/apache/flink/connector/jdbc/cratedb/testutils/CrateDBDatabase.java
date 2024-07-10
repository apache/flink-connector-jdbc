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

package org.apache.flink.connector.jdbc.cratedb.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseResource;
import org.apache.flink.connector.jdbc.testutils.resources.DockerResource;
import org.apache.flink.util.FlinkRuntimeException;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

/** A CrateDB database for testing. */
public class CrateDBDatabase extends DatabaseExtension implements CrateDBImages {

    private static final int CRATEDB_PG_PORT = 5432;
    private static final int CRATEDB_HTTP_PORT = 4200;

    private static final DockerImageName CRATEDB_DOCKER_IMAGE =
            DockerImageName.parse(CRATEDB_5).asCompatibleSubstituteFor("postgres");
    private static final WaitStrategy WAIT_STRATEGY =
            Wait.forHttp("/")
                    .forPort(CRATEDB_HTTP_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.of(60, SECONDS));

    private static CrateDBMetadata metadata;

    public static final CrateDBContainer CONTAINER =
            new CrateDBContainer(CRATEDB_DOCKER_IMAGE)
                    .withDatabaseName("crate")
                    .withUsername("crate")
                    .withPassword("crate")
                    .withCommand("crate")
                    .withEnv("TZ", "UTC") // For deterministic timestamp field results
                    .waitingFor(WAIT_STRATEGY);

    public static CrateDBMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new CrateDBMetadata(CONTAINER);
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

    /**
     * Workaround to use testcontainers with <a
     * href="https://crate.io/docs/jdbc/en/latest/index.html">legacy CrateDB JDBC driver</a>.
     */
    public static class CrateDBContainer extends JdbcDatabaseContainer<CrateDBContainer> {

        public static final String IMAGE = "crate";

        private String databaseName = "crate";

        private String username = "crate";

        private String password = "crate";

        public CrateDBContainer(final DockerImageName dockerImageName) {
            super(dockerImageName);
            dockerImageName.assertCompatibleWith(DockerImageName.parse(IMAGE));

            this.waitStrategy = Wait.forHttp("/").forPort(CRATEDB_HTTP_PORT).forStatusCode(200);

            addExposedPort(CRATEDB_PG_PORT);
            addExposedPort(CRATEDB_HTTP_PORT);
        }

        @Override
        public String getDriverClassName() {
            return "io.crate.client.jdbc.CrateDriver";
        }

        @Override
        public String getJdbcUrl() {
            String additionalUrlParams = constructUrlParameters("?", "&");
            return ("jdbc:crate://"
                    + getHost()
                    + ":"
                    + getMappedPort(CRATEDB_PG_PORT)
                    + "/"
                    + databaseName
                    + additionalUrlParams);
        }

        @Override
        public String getDatabaseName() {
            return databaseName;
        }

        @Override
        public String getUsername() {
            return username;
        }

        @Override
        public String getPassword() {
            return password;
        }

        @Override
        public String getTestQueryString() {
            return "SELECT 1";
        }

        @Override
        public CrateDBContainer withDatabaseName(final String databaseName) {
            this.databaseName = databaseName;
            return self();
        }

        @Override
        public CrateDBContainer withUsername(final String username) {
            this.username = username;
            return self();
        }

        @Override
        public CrateDBContainer withPassword(final String password) {
            this.password = password;
            return self();
        }

        @Override
        protected void waitUntilContainerStarted() {
            getWaitStrategy().waitUntilReady(this);
        }
    }
}
