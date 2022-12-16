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

package org.apache.flink.connector.jdbc.testutils.databases.db2;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;

import com.ibm.db2.jcc.DB2XADataSource;
import org.testcontainers.containers.Db2Container;

import javax.sql.XADataSource;

/** Db2 Metadata. */
public class Db2Metadata implements DatabaseMetadata {
    private final String host;
    private final Integer port;
    private final String dataBaseName;
    private final String username;
    private final String password;
    private final String url;
    private final String driver;
    private final String version;

    public Db2Metadata(Db2Container container) {
        this.username = container.getUsername();
        this.password = container.getPassword();
        this.host = container.getHost();
        this.port = container.getMappedPort(50000);
        this.dataBaseName = container.getDatabaseName();
        this.url = container.getJdbcUrl();
        this.driver = container.getDriverClassName();
        this.version = container.getDockerImageName();
    }

    @Override
    public String getJdbcUrl() {
        return this.url;
    }

    @Override
    public String getJdbcUrlWithCredentials() {
        return String.format(
                "%s;username=%s;password=%s", getJdbcUrl(), getUsername(), getPassword());
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
        DB2XADataSource xaDataSource = new DB2XADataSource();
        xaDataSource.setDatabaseName(dataBaseName);
        xaDataSource.setUser(getUsername());
        xaDataSource.setPassword(getPassword());
        xaDataSource.setServerName(host);
        xaDataSource.setPortNumber(port);
        xaDataSource.setDriverType(4);
        return xaDataSource;
    }

    @Override
    public String getDriverClass() {
        return this.driver;
    }

    @Override
    public String getVersion() {
        return version;
    }
}
