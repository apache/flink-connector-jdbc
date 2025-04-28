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

package org.apache.flink.connector.jdbc.gaussdb.testutils;

import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Testcontainers implementation for GaussDB. Supported images: {@code
 * opengauss/opengauss:7.0.0-RC1.B023}, {@code pgvector/pgvector} Exposed ports: 8000
 *
 * <p>Notes: The source code is based on PostgresContainer.
 */
public class GaussDBContainer<SELF extends GaussDBContainer<SELF>>
        extends JdbcDatabaseContainer<SELF> {

    public static final String NAME = "gaussdb";

    public static final String IMAGE = "opengauss/opengauss:7.0.0-RC1.B023";

    public static final String DEFAULT_TAG = "7.0.0-RC1.B023";

    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("opengauss/opengauss:7.0.0-RC1.B023")
                    .asCompatibleSubstituteFor("gaussdb");

    public static final Integer GAUSSDB_PORT = 8000;

    public static final String DEFAULT_USER_NAME = "flink_jdbc_test";

    public static final String DEFAULT_PASSWORD = "Flink_jdbc_test@123";

    private String databaseName = "postgres";

    private String username = DEFAULT_USER_NAME;

    private String password = DEFAULT_PASSWORD;

    /**
     * @deprecated use {@link #GaussDBContainer(DockerImageName)} or {@link
     *     #GaussDBContainer(String)} instead
     */
    @Deprecated
    public GaussDBContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    public GaussDBContainer(final String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public GaussDBContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        setWaitStrategy(
                new WaitStrategy() {
                    @Override
                    public void waitUntilReady(WaitStrategyTarget waitStrategyTarget) {
                        Wait.forListeningPort().waitUntilReady(waitStrategyTarget);
                        try {
                            // Open Gauss will set up users and password when ports are ready.
                            Wait.forLogMessage(".*gs_ctl stopped.*", 1)
                                    .waitUntilReady(waitStrategyTarget);
                            // Not enough and no idea
                            TimeUnit.SECONDS.sleep(3);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public WaitStrategy withStartupTimeout(Duration duration) {
                        return waitStrategy.withStartupTimeout(duration);
                    }
                });
        addExposedPort(GAUSSDB_PORT);
    }

    /**
     * @return the ports on which to check if the container is ready
     * @deprecated use {@link #getLivenessCheckPortNumbers()} instead
     */
    @NotNull
    @Override
    @Deprecated
    protected Set<Integer> getLivenessCheckPorts() {
        return super.getLivenessCheckPorts();
    }

    @Override
    protected void configure() {
        // Disable Postgres driver use of java.util.logging to reduce noise at startup time
        withUrlParam("loggerLevel", "OFF");
        withDatabaseName(databaseName);
        addEnv("GS_PORT", String.valueOf(GAUSSDB_PORT));
        addEnv("GS_USERNAME", username);
        addEnv("GS_PASSWORD", password);
    }

    @Override
    public String getDriverClassName() {
        return "com.huawei.gaussdb.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return ("jdbc:gaussdb://"
                + getHost()
                + ":"
                + getMappedPort(GAUSSDB_PORT)
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
