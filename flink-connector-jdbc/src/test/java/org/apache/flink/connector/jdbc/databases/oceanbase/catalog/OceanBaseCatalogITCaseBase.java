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

import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test base for {@link OceanBaseCatalog}. */
public abstract class OceanBaseCatalogITCaseBase implements JdbcITCaseBase, DatabaseTest {

    private final String catalogName;
    private final String compatibleMode;
    private final String defaultDatabase;

    public OceanBaseCatalogITCaseBase(
            String catalogName, String compatibleMode, String defaultDatabase) {
        this.catalogName = catalogName;
        this.compatibleMode = compatibleMode;
        this.defaultDatabase = defaultDatabase;
    }

    protected OceanBaseCatalog catalog;
    protected TableEnvironment tEnv;

    protected abstract TableRow allTypesSourceTable();

    protected abstract TableRow allTypesSinkTable();

    protected abstract List<Row> allTypesTableRows();

    protected void before() {}

    protected void after() {}

    @BeforeEach
    void setup() {
        before();

        try (Connection conn = getMetadata().getConnection()) {
            allTypesSourceTable().insertIntoTableValues(conn, allTypesTableRows());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        catalog =
                new OceanBaseCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        catalogName,
                        compatibleMode,
                        defaultDatabase,
                        getMetadata()
                                .getJdbcUrl()
                                .substring(0, getMetadata().getJdbcUrl().lastIndexOf("/")),
                        getBriefAuthProperties(
                                getMetadata().getUsername(), getMetadata().getPassword()));

        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.registerCatalog(catalogName, catalog);
        tEnv.useCatalog(catalogName);
    }

    @AfterEach
    void cleanup() {
        after();
    }

    @Test
    void testGetDb_DatabaseNotExistException() {
        String databaseNotExist = "nonexistent";
        assertThatThrownBy(() -> catalog.getDatabase(databaseNotExist))
                .satisfies(
                        anyCauseMatches(
                                DatabaseNotExistException.class,
                                String.format(
                                        "Database %s does not exist in Catalog",
                                        databaseNotExist)));
    }

    @Test
    void testDbExists() {
        String databaseNotExist = "nonexistent";
        assertThat(catalog.databaseExists(databaseNotExist)).isFalse();
        assertThat(catalog.databaseExists(defaultDatabase)).isTrue();
    }

    // ------ tables ------

    @Test
    void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(defaultDatabase);
        assertThat(actual)
                .containsAll(
                        getManagedTables().stream()
                                .map(TableManaged::getTableName)
                                .collect(Collectors.toList()));
    }

    @Test
    void testListTables_DatabaseNotExistException() {
        String anyDatabase = "anyDatabase";
        assertThatThrownBy(() -> catalog.listTables(anyDatabase))
                .satisfies(
                        anyCauseMatches(
                                DatabaseNotExistException.class,
                                String.format(
                                        "Database %s does not exist in Catalog", anyDatabase)));
    }

    @Test
    void testTableExists() {
        String tableNotExist = "nonexist";
        assertThat(catalog.tableExists(new ObjectPath(defaultDatabase, tableNotExist))).isFalse();
        assertThat(
                        catalog.tableExists(
                                new ObjectPath(
                                        defaultDatabase, allTypesSourceTable().getTableName())))
                .isTrue();
    }

    @Test
    void testGetTables_TableNotExistException() {
        String anyTableNotExist = "anyTable";
        assertThatThrownBy(
                        () -> catalog.getTable(new ObjectPath(defaultDatabase, anyTableNotExist)))
                .satisfies(
                        anyCauseMatches(
                                TableNotExistException.class,
                                String.format(
                                        "Table (or view) %s.%s does not exist in Catalog",
                                        defaultDatabase, anyTableNotExist)));
    }

    @Test
    void testGetTables_TableNotExistException_NoDb() {
        String databaseNotExist = "nonexistdb";
        String tableNotExist = "anyTable";
        assertThatThrownBy(() -> catalog.getTable(new ObjectPath(databaseNotExist, tableNotExist)))
                .satisfies(
                        anyCauseMatches(
                                TableNotExistException.class,
                                String.format(
                                        "Table (or view) %s.%s does not exist in Catalog",
                                        databaseNotExist, tableNotExist)));
    }

    @Test
    void testGetTable() throws TableNotExistException {
        CatalogBaseTable table =
                catalog.getTable(
                        new ObjectPath(defaultDatabase, allTypesSourceTable().getTableName()));
        assertThat(table.getUnresolvedSchema().getColumns())
                .isEqualTo(allTypesSourceTable().getTableSchema().getColumns());
    }

    // ------ test select query. ------

    @Test
    void testWithoutCatalogDB() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s",
                                                allTypesSourceTable().getTableName()))
                                .execute()
                                .collect());

        assertThat(results).isEqualTo(allTypesTableRows());
    }

    @Test
    void testWithoutCatalog() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`.`%s`",
                                                defaultDatabase,
                                                allTypesSourceTable().getTableName()))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(allTypesTableRows());
    }

    @Test
    void testFullPath() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s.%s.`%s`",
                                                catalogName,
                                                defaultDatabase,
                                                allTypesSourceTable().getTableName()))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(allTypesTableRows());
    }

    @Test
    void testSelectToInsert() throws Exception {
        String sql =
                String.format(
                        "insert into `%s` select * from `%s`",
                        allTypesSinkTable().getTableName(), allTypesSourceTable().getTableName());
        tEnv.executeSql(sql).await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s",
                                                allTypesSinkTable().getTableName()))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(allTypesTableRows());
    }
}
