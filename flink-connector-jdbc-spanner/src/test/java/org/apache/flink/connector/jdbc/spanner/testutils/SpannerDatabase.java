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

package org.apache.flink.connector.jdbc.spanner.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseResource;
import org.apache.flink.connector.jdbc.testutils.resources.DockerResource;
import org.apache.flink.util.FlinkRuntimeException;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** A Spanner database for testing. */
public class SpannerDatabase extends DatabaseExtension implements SpannerImages {

    private static final SpannerEmulatorJdbcContainer CONTAINER =
            new SpannerEmulatorJdbcContainer(SPANNER_EMULATOR_1_5);

    private static SpannerMetadata metadata;

    public static SpannerMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new SpannerMetadata(CONTAINER);
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
     * Testcontainers implementation for Spanner.
     *
     * <p>Supported image: {@code gcr.io/cloud-spanner-emulator/emulator}
     *
     * <p>Exposed ports:
     *
     * <ul>
     *   <li>gRPC: 9010
     *   <li>HTTP: 9020
     * </ul>
     */
    public static class SpannerEmulatorJdbcContainer
            extends JdbcDatabaseContainer<SpannerEmulatorJdbcContainer> {

        private static final int GRPC_PORT = 9010;
        private static final int HTTP_PORT = 9020;
        private static final DockerImageName DEFAULT_IMAGE_NAME =
                DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator");

        private static final String DEFAULT_PROJECT = "test-project";
        private static final String DEFAULT_INSTANCE = "test-instance";
        private static final String DEFAULT_DATABASE = "test-database";

        private String project;
        private String instance;
        private String database;

        public SpannerEmulatorJdbcContainer(String image) {
            this(DockerImageName.parse(image));
        }

        public SpannerEmulatorJdbcContainer(final DockerImageName dockerImageName) {
            super(dockerImageName);
            this.project = DEFAULT_PROJECT;
            this.instance = DEFAULT_INSTANCE;
            this.database = DEFAULT_DATABASE;
            dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
            addExposedPorts(GRPC_PORT, HTTP_PORT);
            setWaitStrategy(Wait.forLogMessage(".*Cloud Spanner emulator running\\..*", 1));
        }

        public String getEmulatorGrpcEndpoint() {
            return getHost() + ":" + getMappedPort(GRPC_PORT);
        }

        public String getEmulatorHttpEndpoint() {
            return getHost() + ":" + getMappedPort(HTTP_PORT);
        }

        @Override
        public String getDriverClassName() {
            return "com.google.cloud.spanner.jdbc.JdbcDriver";
        }

        @Override
        public String getJdbcUrl() {
            return "jdbc:cloudspanner://"
                    + getEmulatorGrpcEndpoint()
                    + "/projects/"
                    + getProject()
                    + "/instances/"
                    + getInstance()
                    + "/databases/"
                    + getDatabaseName()
                    + ";autoConfigEmulator=true";
        }

        public String getProject() {
            return project;
        }

        public SpannerEmulatorJdbcContainer withProject(String project) {
            this.project = project;
            return self();
        }

        public String getInstance() {
            return instance;
        }

        public SpannerEmulatorJdbcContainer withInstance(String instance) {
            this.instance = instance;
            return self();
        }

        @Override
        public String getDatabaseName() {
            return database;
        }

        @Override
        public SpannerEmulatorJdbcContainer withDatabaseName(String dbName) {
            this.database = dbName;
            return self();
        }

        @Override
        public String getUsername() {
            // Spanner does not support password authentication.
            return "";
        }

        @Override
        public String getPassword() {
            // Spanner does not support password authentication.
            return "";
        }

        @Override
        protected String getTestQueryString() {
            return "SELECT 1";
        }
    }
}
