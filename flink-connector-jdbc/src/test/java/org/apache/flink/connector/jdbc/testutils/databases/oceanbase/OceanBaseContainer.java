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

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** {@link JdbcDatabaseContainer} for OceanBase. */
public class OceanBaseContainer extends JdbcDatabaseContainer<OceanBaseContainer> {

    private static final int SQL_PORT = 2881;
    private static final String DEFAULT_PASSWORD = "";

    private String password = DEFAULT_PASSWORD;

    public OceanBaseContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public OceanBaseContainer(DockerImageName dockerImageName) {
        super(dockerImageName);

        addExposedPort(SQL_PORT);
        setWaitStrategy(Wait.forLogMessage(".*boot success!.*", 1));
    }

    @Override
    protected void configure() {
        if (!DEFAULT_PASSWORD.equals(password)) {
            addEnv("OB_TENANT_PASSWORD", password);
        }
    }

    protected void waitUntilContainerStarted() {
        this.getWaitStrategy().waitUntilReady(this);
    }

    @Override
    public String getDriverClassName() {
        return "com.oceanbase.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        return getJdbcUrl("test");
    }

    public String getJdbcUrl(String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:oceanbase://"
                + getHost()
                + ":"
                + getMappedPort(SQL_PORT)
                + "/"
                + databaseName
                + additionalUrlParams;
    }

    @Override
    public String getUsername() {
        return "root@test";
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    public OceanBaseContainer withPassword(String password) {
        this.password = password;
        return this;
    }
}
