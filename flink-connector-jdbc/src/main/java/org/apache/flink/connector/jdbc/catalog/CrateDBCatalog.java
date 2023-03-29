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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.cratedb.CrateDBTypeMapper;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    protected CrateDBCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(userClassLoader, catalogName, defaultDatabase, username, pwd, baseUrl, new CrateDBTypeMapper());
    }

    // ------ databases ------

    @Override
    public List<String> listDatabases() throws CatalogException {
        return ImmutableList.of(DEFAULT_DATABASE);
    }

    // ------ schemas ------

    protected Set<String> getBuiltinSchemas() {
        return builtinSchemas;
    }

    // ------ tables ------

    @Override
    protected List<String> getPureTables(String databaseName, String schema) {
        return extractColumnValuesBySQL(
                baseUrl + databaseName,
                "SELECT table_name FROM information_schema.tables "
                        + "WHERE table_schema = ? "
                        + "ORDER BY table_type, table_name",
                1,
                null,
                schema);
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
                extractColumnValuesBySQL(baseUrl + DEFAULT_DATABASE, "show search_path", 1, null)
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
