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

package org.apache.flink.connector.jdbc.cratedb.database.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.postgres.database.catalog.PostgresCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;

/** Catalog for CrateDB. */
@Internal
public class CrateDBCatalog extends PostgresCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(CrateDBCatalog.class);

    public static final String DEFAULT_DATABASE = "crate";

    private static final Set<String> builtinSchemas =
            new HashSet<String>() {
                {
                    add("pg_catalog");
                    add("information_schema");
                    add("sys");
                }
            };

    @VisibleForTesting
    public CrateDBCatalog(
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

    public CrateDBCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            Properties connecProperties) {
        super(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                new CrateDBTypeMapper(),
                connecProperties);
    }

    // ------ databases ------

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Collections.singletonList(DEFAULT_DATABASE);
    }

    // ------ schemas ------

    protected Set<String> getBuiltinSchemas() {
        return builtinSchemas;
    }

    // ------ tables ------

    @Override
    protected List<String> getPureTables(Connection conn, List<String> schemas)
            throws SQLException {
        List<String> tables = Lists.newArrayList();

        // position 1 is database name, position 2 is schema name, position 3 is table name
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "SELECT table_name FROM information_schema.tables "
                                + "WHERE table_schema = ? "
                                + "ORDER BY table_type, table_name")) {
            for (String schema : schemas) {
                // Column index 1 is database name, 2 is schema name, 3 is table name
                extractColumnValuesByStatement(ps, 1, null, schema).stream()
                        .map(pureTable -> schema + "." + pureTable)
                        .forEach(tables::add);
            }
            return tables;
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List<String> tables;
        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }

        String searchPath =
                extractColumnValuesBySQL(
                                getDatabaseUrl(DEFAULT_DATABASE), "show search_path", 1, null)
                        .get(0);
        String[] schemas = searchPath.split("\\s*,\\s*");

        if (tables.contains(getSchemaTableName(tablePath))) {
            return true;
        }
        for (String schema : schemas) {
            if (tables.contains(schema + "." + tablePath.getObjectName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected String getTableName(ObjectPath tablePath) {
        return CrateDBTablePath.fromFlinkTableName(tablePath.getObjectName()).getPgTableName();
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return CrateDBTablePath.fromFlinkTableName(tablePath.getObjectName()).getPgSchemaName();
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return CrateDBTablePath.fromFlinkTableName(tablePath.getObjectName()).getFullPath();
    }
}
