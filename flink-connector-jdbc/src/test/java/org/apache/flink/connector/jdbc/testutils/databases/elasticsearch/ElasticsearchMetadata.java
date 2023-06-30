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

package org.apache.flink.connector.jdbc.testutils.databases.elasticsearch;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;

import org.testcontainers.elasticsearch.ElasticsearchContainer;

import javax.sql.XADataSource;

/** Elasticsearch metadata. */
public class ElasticsearchMetadata implements DatabaseMetadata {

    static final int ELASTIC_PORT = 9200;
    static final String USERNAME = "elastic";
    static final String PASSWORD = "password";

    private final String username;
    private final String password;
    private final String jdbcUrl;
    private final String driver;
    private final String version;
    private final String containerHost;
    private final int containerPort;

    public ElasticsearchMetadata(ElasticsearchContainer container) {
        this.containerHost = container.getHost();
        this.containerPort = container.getMappedPort(ELASTIC_PORT);
        this.username = USERNAME;
        this.password = PASSWORD;
        this.jdbcUrl = "jdbc:elasticsearch://" + containerHost + ":" + containerPort;
        this.driver = "org.elasticsearch.xpack.sql.jdbc.EsDriver";
        this.version = container.getDockerImageName();
    }

    @Override
    public String getJdbcUrl() {
        return this.jdbcUrl;
    }

    @Override
    public String getJdbcUrlWithCredentials() {
        return String.format("%s&user=%s&password=%s", getJdbcUrl(), getUsername(), getPassword());
    }

    @Override
    public String getUsername() {
        return this.username;
    }

    @Override
    public String getPassword() {
        return this.password;
    }

    @Override
    public XADataSource buildXaDataSource() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDriverClass() {
        return this.driver;
    }

    @Override
    public String getVersion() {
        return this.version;
    }

    public String getContainerHost() {
        return containerHost;
    }

    public int getContainerPort() {
        return containerPort;
    }
}
