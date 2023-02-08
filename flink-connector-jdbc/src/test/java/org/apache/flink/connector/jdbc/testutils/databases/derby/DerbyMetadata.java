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

package org.apache.flink.connector.jdbc.testutils.databases.derby;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;

import org.apache.derby.jdbc.EmbeddedXADataSource;

import javax.sql.XADataSource;

/** Derby Metadata. */
public class DerbyMetadata implements DatabaseMetadata {
    private final String dbName;

    public DerbyMetadata(String schemaName) {
        dbName = "memory:" + schemaName;
    }

    public String getDbName() {
        return dbName;
    }

    @Override
    public String getJdbcUrl() {
        return String.format("jdbc:derby:%s", dbName);
    }

    @Override
    public String getJdbcUrlWithCredentials() {
        return getJdbcUrl();
    }

    @Override
    public String getUsername() {
        return "";
    }

    @Override
    public String getPassword() {
        return "";
    }

    @Override
    public XADataSource buildXaDataSource() {
        EmbeddedXADataSource ds = new EmbeddedXADataSource();
        ds.setDatabaseName(dbName);
        return ds;
    }

    @Override
    public String getDriverClass() {
        return "org.apache.derby.jdbc.EmbeddedDriver";
    }

    @Override
    public String getVersion() {
        return "derby:memory";
    }
}
