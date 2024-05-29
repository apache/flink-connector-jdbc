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

package org.apache.flink.connector.jdbc.databases.cratedb.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CrateDBCatalog}. */
class CrateDBCatalogTest extends CrateDBCatalogTestBase {

    // ------ databases ------

    @Test
    void testGetDb_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.getDatabase("nonexistent"))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessageContaining("Database nonexistent does not exist in Catalog");
    }

    @Test
    void testListDatabases() {
        assertThat(catalog.listDatabases()).containsExactly("crate");
    }

    @Test
    void testDbExists() {
        assertThat(catalog.databaseExists("nonexistent")).isFalse();
        assertThat(catalog.databaseExists(CrateDBCatalog.DEFAULT_DATABASE)).isTrue();
    }

    // ------ tables ------

    @Test
    void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(CrateDBCatalog.DEFAULT_DATABASE);

        assertThat(actual)
                .containsExactly(
                        "doc.array_table",
                        "doc.primitive_table",
                        "doc.t1",
                        "doc.t2",
                        "doc.target_primitive_table",
                        "test_schema.t3");
    }

    @Test
    void testListTables_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.listTables("CrateDB"))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    void testTableExists() {
        assertThat(catalog.tableExists(new ObjectPath(CrateDBCatalog.DEFAULT_DATABASE, "nonexist")))
                .isFalse();

        assertThat(catalog.tableExists(new ObjectPath(CrateDBCatalog.DEFAULT_DATABASE, TABLE1)))
                .isTrue();
        assertThat(
                        catalog.tableExists(
                                new ObjectPath(CrateDBCatalog.DEFAULT_DATABASE, "test_schema.t3")))
                .isTrue();
    }

    @Test
    void testGetTables_TableNotExistException() {
        assertThatThrownBy(
                        () ->
                                catalog.getTable(
                                        new ObjectPath(
                                                CrateDBCatalog.DEFAULT_DATABASE,
                                                CrateDBTablePath.toFlinkTableName(
                                                        TEST_SCHEMA, "anytable"))))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testGetTables_TableNotExistException_NoSchema() {
        assertThatThrownBy(
                        () ->
                                catalog.getTable(
                                        new ObjectPath(
                                                CrateDBCatalog.DEFAULT_DATABASE,
                                                CrateDBTablePath.toFlinkTableName(
                                                        "nonexistschema", "anytable"))))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testGetTables_TableNotExistException_NoDb() {
        assertThatThrownBy(
                        () ->
                                catalog.getTable(
                                        new ObjectPath(
                                                "nonexistdb",
                                                CrateDBTablePath.toFlinkTableName(
                                                        TEST_SCHEMA, "anytable"))))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testGetTable() throws TableNotExistException {
        // test crate.doc.t1
        Schema schema = getSimpleTable().schema;
        CatalogBaseTable table =
                catalog.getTable(new ObjectPath(CrateDBCatalog.DEFAULT_DATABASE, TABLE1));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        table = catalog.getTable(new ObjectPath(CrateDBCatalog.DEFAULT_DATABASE, "doc.t1"));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        table = catalog.getTable(new ObjectPath(CrateDBCatalog.DEFAULT_DATABASE, TABLE2));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        table = catalog.getTable(new ObjectPath(CrateDBCatalog.DEFAULT_DATABASE, "doc.t2"));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        // test crate.test_schema.t2
        table =
                catalog.getTable(
                        new ObjectPath(CrateDBCatalog.DEFAULT_DATABASE, TEST_SCHEMA + ".t3"));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);
    }

    @Test
    void testPrimitiveDataTypes() throws TableNotExistException {
        CatalogBaseTable table =
                catalog.getTable(
                        new ObjectPath(CrateDBCatalog.DEFAULT_DATABASE, TABLE_PRIMITIVE_TYPE));

        assertThat(table.getUnresolvedSchema())
                .hasToString(sanitizeSchemaSQL(getPrimitiveTable().schema.toString()));
    }

    @Test
    void testArrayDataTypes() throws TableNotExistException {
        CatalogBaseTable table =
                catalog.getTable(new ObjectPath(CrateDBCatalog.DEFAULT_DATABASE, TABLE_ARRAY_TYPE));

        assertThat(table.getUnresolvedSchema())
                .hasToString(sanitizeSchemaSQL(getArrayTable().schema.toString()));
    }
}
