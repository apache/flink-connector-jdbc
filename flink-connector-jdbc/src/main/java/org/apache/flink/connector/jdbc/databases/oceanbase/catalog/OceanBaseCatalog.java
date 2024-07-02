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

package org.apache.flink.connector.jdbc.databases.oceanbase.catalog;

import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeMapper;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.COMPATIBLE_MODE;

/** Catalog for OceanBase. */
public class OceanBaseCatalog extends AbstractJdbcCatalog {

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("__public");
                    add("information_schema");
                    add("mysql");
                    add("oceanbase");
                    add("LBACSYS");
                    add("ORAAUDITOR");
                }
            };

    private final String compatibleMode;
    private final JdbcDialectTypeMapper dialectTypeMapper;

    public OceanBaseCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String compatibleMode,
            String defaultDatabase,
            String baseUrl,
            Properties connectionProperties) {
        super(userClassLoader, catalogName, defaultDatabase, baseUrl, connectionProperties);
        this.compatibleMode = compatibleMode;
        this.dialectTypeMapper = new OceanBaseTypeMapper(compatibleMode);
    }

    private boolean isMySQLMode() {
        return "mysql".equalsIgnoreCase(compatibleMode);
    }

    private String getConnUrl() {
        return isMySQLMode() ? baseUrl : defaultUrl;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        String query =
                isMySQLMode()
                        ? "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`"
                        : "SELECT USERNAME FROM ALL_USERS";
        return extractColumnValuesBySQL(
                getConnUrl(), query, 1, dbName -> !builtinDatabases.contains(dbName));
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        String sql =
                isMySQLMode()
                        ? "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?"
                        : "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = ?";
        return extractColumnValuesBySQL(getConnUrl(), sql, 1, null, databaseName);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        String query =
                isMySQLMode()
                        ? "SELECT TABLE_NAME FROM information_schema.`TABLES` "
                                + "WHERE TABLE_SCHEMA = ? and TABLE_NAME = ?"
                        : "SELECT TABLE_NAME FROM ALL_TABLES "
                                + "WHERE OWNER = ? and TABLE_NAME = ?";
        return !extractColumnValuesBySQL(
                        getConnUrl(),
                        query,
                        1,
                        null,
                        tablePath.getDatabaseName(),
                        tablePath.getObjectName())
                .isEmpty();
    }

    @Override
    protected Optional<UniqueConstraint> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        if (isMySQLMode()) {
            return super.getPrimaryKey(metaData, database, null, table);
        } else {
            return super.getPrimaryKey(metaData, null, database, table);
        }
    }

    @Override
    protected Map<String, String> getOptions(ObjectPath tablePath) {
        Map<String, String> options = super.getOptions(tablePath);
        options.put(COMPATIBLE_MODE.key(), compatibleMode);
        return options;
    }

    /** Converts OceanBase type to Flink {@link DataType}. */
    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    @Override
    protected String getTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return null;
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }
}
