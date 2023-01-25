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

package org.apache.flink.connector.jdbc.databases.mysql;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.testcontainers.containers.MySQLContainer;

import javax.sql.XADataSource;

/** Postgres Metadata. */
public class MySqlMetadata implements DatabaseMetadata {

    private final String username;
    private final String password;
    private final String url;
    private final String driver;
    private final String version;
    private final boolean xaEnabled;

    protected MySqlMetadata(MySQLContainer<?> container) {
        this(container, false);
    }

    protected MySqlMetadata(MySQLContainer<?> container, boolean hasXaEnabled) {
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
    public String getSchema() {
        return "";
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

        MysqlXADataSource xaDataSource = new MysqlXADataSource();
        xaDataSource.setUrl(getUrl());
        xaDataSource.setUser(getUser());
        xaDataSource.setPassword(getPassword());
        return xaDataSource;
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
