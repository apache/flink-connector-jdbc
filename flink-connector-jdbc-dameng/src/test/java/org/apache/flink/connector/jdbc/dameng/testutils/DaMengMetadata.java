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

package org.apache.flink.connector.jdbc.dameng.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;

import javax.sql.XADataSource;

/** DaMeng Metadata. */
public class DaMengMetadata implements DatabaseMetadata {

    private final String username;
    private final String password;
    private final String url;
    private final String driver;
    private final String version;
    private final boolean xaEnabled;

    public DaMengMetadata(DaMengContainer container) {
        this(container, false);
    }

    public DaMengMetadata(DaMengContainer container, boolean hasXaEnabled) {
        this.username = container.getUsername();
        this.password = container.getPassword();
        this.url = container.getJdbcUrl();
        this.driver = container.getDriverClassName();
        this.version = container.getDockerImageName();
        this.xaEnabled = hasXaEnabled;
    }

    @Override
    public String getJdbcUrl() {
        return this.url;
    }

    @Override
    public String getJdbcUrlWithCredentials() {
        return String.format("%s?user=%s&password=%s", getJdbcUrl(), getUsername(), getPassword());
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
        if (!xaEnabled) {
            throw new UnsupportedOperationException();
        }

        try {
            // Use reflection to create DmXADataSource to avoid direct dependency
            Class<?> xaDataSourceClass = Class.forName("dm.jdbc.driver.DmXADataSource");
            XADataSource xaDataSource = (XADataSource) xaDataSourceClass.newInstance();

            // Set properties using reflection
            xaDataSourceClass.getMethod("setURL", String.class).invoke(xaDataSource, getJdbcUrl());
            xaDataSourceClass.getMethod("setUser", String.class).invoke(xaDataSource, getUsername());
            xaDataSourceClass.getMethod("setPassword", String.class).invoke(xaDataSource, getPassword());

            return xaDataSource;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create DaMeng XA DataSource", e);
        }
    }

    @Override
    public String getDriverClass() {
        return this.driver;
    }

    @Override
    public String getVersion() {
        return this.version;
    }
}
