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

package org.apache.flink.connector.jdbc.databases.vertica;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import org.testcontainers.containers.GenericContainer;

import javax.sql.XADataSource;

/** Vertica Metadata. */
public class VerticaMetadata implements DatabaseMetadata {

    private final String url;
    private final String version;

    public VerticaMetadata(GenericContainer<?> container) {
        String sampleDatabase = "VMart";
        this.url =
                "jdbc:vertica://"
                        + container.getHost()
                        + ":"
                        + container.getMappedPort(5433)
                        + "/"
                        + sampleDatabase;
        this.version = container.getDockerImageName();
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
        return "dbadmin";
    }

    @Override
    public String getPassword() {
        return "";
    }

    @Override
    public XADataSource buildXaDataSource() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDriverClass() {
        return "com.vertica.jdbc.Driver";
    }

    @Override
    public String getVersion() {
        return this.version;
    }
}
