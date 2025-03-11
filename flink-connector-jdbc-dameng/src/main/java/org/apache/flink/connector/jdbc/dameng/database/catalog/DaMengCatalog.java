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

package org.apache.flink.connector.jdbc.dameng.database.catalog;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.database.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;

/** Catalog for DaMeng. */
@Internal
public class DaMengCatalog
        extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(DaMengCatalog.class);

    private final JdbcCatalogTypeMapper dialectTypeMapper;

    // DaMeng system schemas that shouldn't be exposed to users
    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("SYSOBJECTS");
                    add("SYSAUDIT");
                }
            };

    @VisibleForTesting
    public DaMengCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        this(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                getBriefAuthProperties(username, pwd));
    }

    public DaMengCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            Properties connectionProperties) {
        super(userClassLoader, catalogName, defaultDatabase, baseUrl, connectionProperties);

        String driverVersion =
                Preconditions.checkNotNull(getDriverVersion(), "Driver version must not be null.");
        String databaseVersion =
                Preconditions.checkNotNull(
                        getDatabaseVersion(), "Database version must not be null.");
        LOG.info("Driver version: {}, database version: {}", driverVersion, databaseVersion);
        this.dialectTypeMapper = new DaMengTypeMapper(databaseVersion, driverVersion);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return extractColumnValuesBySQL(
                defaultUrl,
                "SELECT NAME AS SCHEMA_NAME FROM SYS.SYSOBJECTS",
                1,
                dbName -> !builtinDatabases.contains(dbName.toUpperCase()));
    }

    // ------ tables ------

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return extractColumnValuesBySQL(
                getDatabaseUrl(databaseName),
                "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = ?",
                1,
                null,
                databaseName.toUpperCase());
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return !extractColumnValuesBySQL(
                baseUrl,
                "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = ? AND TABLE_NAME = ?",
                1,
                null,
                tablePath.getDatabaseName().toUpperCase(),
                tablePath.getObjectName().toUpperCase())
                .isEmpty();
    }

    private String getDatabaseVersion() {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userClassLoader)) {
            try (Connection conn = DriverManager.getConnection(defaultUrl, connectionProperties)) {
                return conn.getMetaData().getDatabaseProductVersion();
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed in getting DaMeng version by %s.", defaultUrl), e);
            }
        }
    }

    private String getDriverVersion() {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userClassLoader)) {
            try (Connection conn = DriverManager.getConnection(defaultUrl, connectionProperties)) {
                String driverVersion = conn.getMetaData().getDriverVersion();
                Pattern regexp = Pattern.compile("\\d+?\\.\\d+?\\.\\d+");
                Matcher matcher = regexp.matcher(driverVersion);
                return matcher.find() ? matcher.group(0) : null;
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed in getting DaMeng driver version by %s.", defaultUrl),
                        e);
            }
        }
    }

    /** Converts DaMeng type to Flink {@link DataType}. */
    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    @Override
    protected String getTableName(ObjectPath tablePath) {
        return tablePath.getObjectName().toUpperCase();
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return tablePath.getDatabaseName().toUpperCase();
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return tablePath.getObjectName().toUpperCase();
    }
}
