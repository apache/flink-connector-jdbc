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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.database.catalog.AbstractJdbcCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.util.List;
import java.util.Properties;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;

/**
 * Catalogs for relational databases via JDBC.
 *
 * @deprecated Use {@link org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog}
 */
@Deprecated
@PublicEvolving
public class JdbcCatalog extends AbstractJdbcCatalog {

    private final AbstractJdbcCatalog internal;

    @Deprecated
    /**
     * Creates a JdbcCatalog.
     *
     * @deprecated please use {@link JdbcCatalog#JdbcCatalog(ClassLoader, String, String, String,
     *     String, Properties)} instead.
     */
    public JdbcCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        this(
                Thread.currentThread().getContextClassLoader(),
                catalogName,
                defaultDatabase,
                baseUrl,
                null,
                getBriefAuthProperties(username, pwd));
    }

    @VisibleForTesting
    /**
     * Creates a JdbcCatalog.
     *
     * @param userClassLoader the classloader used to load JDBC driver
     * @param catalogName the registered catalog name
     * @param defaultDatabase the default database name
     * @param username the username used to connect the database
     * @param pwd the password used to connect the database
     * @param baseUrl the base URL of the database, e.g. jdbc:mysql://localhost:3306
     * @param compatibleMode the compatible mode of the database
     */
    public JdbcCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl,
            String compatibleMode) {
        this(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                compatibleMode,
                getBriefAuthProperties(username, pwd));
    }

    /**
     * Creates a JdbcCatalog.
     *
     * @param userClassLoader the classloader used to load JDBC driver
     * @param catalogName the registered catalog name
     * @param defaultDatabase the default database name
     * @param connectProperties the properties used to connect the database
     * @param baseUrl the base URL of the database, e.g. jdbc:mysql://localhost:3306
     * @param compatibleMode the compatible mode of the database
     */
    public JdbcCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            String compatibleMode,
            Properties connectProperties) {
        super(userClassLoader, catalogName, defaultDatabase, baseUrl, connectProperties);

        internal =
                JdbcCatalogUtils.createCatalog(
                        userClassLoader,
                        catalogName,
                        defaultDatabase,
                        baseUrl,
                        compatibleMode,
                        connectProperties);
    }

    // ------ databases -----

    @Override
    public List<String> listDatabases() throws CatalogException {
        return internal.listDatabases();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return internal.getDatabase(databaseName);
    }

    // ------ tables and views ------

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return internal.listTables(databaseName);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return internal.getTable(tablePath);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    // ------ getters ------

    @VisibleForTesting
    @Internal
    public AbstractJdbcCatalog getInternal() {
        return internal;
    }
}
