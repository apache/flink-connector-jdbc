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

package org.apache.flink.connector.jdbc.databases.oracle;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import oracle.jdbc.xa.client.OracleXADataSource;
import org.testcontainers.containers.OracleContainer;

import javax.sql.XADataSource;

import java.sql.SQLException;

/** Postgres Metadata. */
public class OracleMetadata implements DatabaseMetadata {

    private final String username;
    private final String password;
    private final String url;
    private final String driver;
    private final String version;
    private final boolean xaEnabled;

    protected OracleMetadata(OracleContainer container) {
        this(container, false);
    }

    protected OracleMetadata(OracleContainer container, boolean hasXaEnabled) {
        this.username = container.getUsername();
        this.password = container.getPassword();
        this.url = container.getJdbcUrl();
        this.driver = container.getDriverClassName();
        this.version = container.getDockerImageName();
        this.xaEnabled = hasXaEnabled;
    }

    @Override
    public String getUrl() {
        return this.url;
    }

    @Override
    public String getUrlWithCredentials() {
        return String.format("%s?user=%s&password=%s", this.url, this.username, this.password);
    }

    @Override
    public String getSchema() {
        return "public";
    }

    @Override
    public String getVersion() {
        return this.version;
    }

    @Override
    public XADataSource buildXaDataSource() {
        if (!xaEnabled) {
            throw new UnsupportedOperationException();
        }
        try {
            OracleXADataSource xaDataSource = new OracleXADataSource();
            xaDataSource.setURL(getUrl());
            xaDataSource.setUser(getUser());
            xaDataSource.setPassword(getPassword());
            return xaDataSource;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDriverClass() {
        return this.driver;
    }

    @Override
    public String getUser() {
        return this.username;
    }

    @Override
    public String getPassword() {
        return this.password;
    }
}
