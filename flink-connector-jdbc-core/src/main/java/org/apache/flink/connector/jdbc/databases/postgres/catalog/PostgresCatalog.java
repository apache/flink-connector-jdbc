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

package org.apache.flink.connector.jdbc.databases.postgres.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.table.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.core.table.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;

/** Catalog for PostgreSQL. */
@Internal
public class PostgresCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresCatalog.class);

    public static final String DEFAULT_DATABASE = "postgres";

    // ------ Postgres default objects that shouldn't be exposed to users ------

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("template0");
                    add("template1");
                }
            };

    private static final Set<String> builtinSchemas =
            new HashSet<String>() {
                {
                    add("pg_toast");
                    add("pg_temp_1");
                    add("pg_toast_temp_1");
                    add("pg_catalog");
                    add("information_schema");
                }
            };

    protected final JdbcCatalogTypeMapper dialectTypeMapper;

    @VisibleForTesting
    public PostgresCatalog(
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

    public PostgresCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            Properties connectProperties) {
        this(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                new PostgresTypeMapper(),
                connectProperties);
    }

    @Deprecated
    protected PostgresCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl,
            JdbcCatalogTypeMapper dialectTypeMapper) {
        this(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                dialectTypeMapper,
                getBriefAuthProperties(username, pwd));
    }

    protected PostgresCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            JdbcCatalogTypeMapper dialectTypeMapper,
            Properties connectProperties) {
        super(userClassLoader, catalogName, defaultDatabase, baseUrl, connectProperties);
        this.dialectTypeMapper = dialectTypeMapper;
    }

    // ------ databases ------

    @Override
    public List<String> listDatabases() throws CatalogException {

        return extractColumnValuesBySQL(
                defaultUrl,
                "SELECT datname FROM pg_database;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    // ------ schemas ------

    protected Set<String> getBuiltinSchemas() {
        return builtinSchemas;
    }

    // ------ tables ------

    protected List<String> getPureTables(Connection conn, List<String> schemas)
            throws SQLException {
        List<String> tables = Lists.newArrayList();

        // position 1 is database name, position 2 is schema name, position 3 is table name
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "SELECT * FROM information_schema.tables "
                                + "WHERE table_type = 'BASE TABLE' "
                                + "AND table_schema = ? "
                                + "ORDER BY table_type, table_name;")) {
            for (String schema : schemas) {
                // Column index 1 is database name, 2 is schema name, 3 is table name
                extractColumnValuesByStatement(ps, 3, null, schema).stream()
                        .map(pureTable -> schema + "." + pureTable)
                        .forEach(tables::add);
            }
            return tables;
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {

        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        final String url = baseUrl + databaseName;
        try (Connection conn = DriverManager.getConnection(url, connectionProperties)) {
            // get all schemas
            List<String> schemas;
            try (PreparedStatement ps =
                    conn.prepareStatement("SELECT schema_name FROM information_schema.schemata;")) {
                schemas =
                        extractColumnValuesByStatement(
                                ps, 1, pgSchema -> !getBuiltinSchemas().contains(pgSchema));
            }

            // get all tables
            return getPureTables(conn, schemas);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list tables for database %s", databaseName), e);
        }
    }

    /**
     * Converts Postgres type to Flink {@link DataType}.
     *
     * @see org.postgresql.jdbc.TypeInfoCache
     */
    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {

        List<String> tables = null;
        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }

        return tables.contains(getSchemaTableName(tablePath));
    }

    @Override
    protected String getTableName(ObjectPath tablePath) {
        return PostgresTablePath.fromFlinkTableName(tablePath.getObjectName()).getPgTableName();
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return PostgresTablePath.fromFlinkTableName(tablePath.getObjectName()).getPgSchemaName();
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return PostgresTablePath.fromFlinkTableName(tablePath.getObjectName()).getFullPath();
    }
}
