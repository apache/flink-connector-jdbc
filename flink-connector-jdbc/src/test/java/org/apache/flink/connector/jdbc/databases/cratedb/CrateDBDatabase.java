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

package org.apache.flink.connector.jdbc.databases.cratedb;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.DatabaseTest;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

/** A CrateDB database for testing. */
@Testcontainers
public interface CrateDBDatabase extends DatabaseTest {

    String CRATEDB = "crate:latest";
    int CRATEDB_PG_PORT = 5432;
    int CRATEDB_HTTP_PORT = 4200;

    DockerImageName CRATEDB_DOCKER_IMAGE =
            DockerImageName.parse(CRATEDB).asCompatibleSubstituteFor("postgres");
    WaitStrategy WAIT_STRATEGY =
            Wait.forHttp("/")
                    .forPort(CRATEDB_HTTP_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.of(60, SECONDS));

    @Container
    CrateDBContainer<?> CONTAINER =
            new CrateDBContainer<>(CRATEDB_DOCKER_IMAGE)
                    .withDatabaseName("crate")
                    .withUsername("crate")
                    .withPassword("crate")
                    .withCommand("crate")
                    .waitingFor(WAIT_STRATEGY);

    @Override
    default DatabaseMetadata getMetadata() {
        return new CrateDBMetadata(CONTAINER);
    }

    /**
     * Workaround to use testcontainers until: <a
     * href="https://github.com/testcontainers/testcontainers-java/pull/6790"/>is merged.
     */
    class CrateDBContainer<SELF extends CrateDBContainer<SELF>>
            extends JdbcDatabaseContainer<SELF> {

        public static final String IMAGE = "crate";

        public static final String DEFAULT_TAG = "latest";

        protected static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse(IMAGE);

        private String databaseName = "crate";

        private String username = "crate";

        private String password = "crate";

        /**
         * @deprecated use {@link #CrateDBContainer(DockerImageName)} or {@link
         *     #CrateDBContainer(String)} instead
         */
        @Deprecated
        public CrateDBContainer() {
            this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
        }

        public CrateDBContainer(final String dockerImageName) {
            this(DockerImageName.parse(dockerImageName));
        }

        public CrateDBContainer(final DockerImageName dockerImageName) {
            super(dockerImageName);
            dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

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
        public SELF withDatabaseName(final String databaseName) {
            this.databaseName = databaseName;
            return self();
        }

        @Override
        public SELF withUsername(final String username) {
            this.username = username;
            return self();
        }

        @Override
        public SELF withPassword(final String password) {
            this.password = password;
            return self();
        }

        @Override
        protected void waitUntilContainerStarted() {
            getWaitStrategy().waitUntilReady(this);
        }
    }
}
