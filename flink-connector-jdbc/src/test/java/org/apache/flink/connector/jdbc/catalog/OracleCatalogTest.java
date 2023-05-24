package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class OracleCatalogTest extends OracleCatalogTestBase {

    @Test
    void testGetDb_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.getDatabase("nonexistent"))
            .isInstanceOf(DatabaseNotExistException.class)
            .hasMessageContaining("Database nonexistent does not exist in Catalog");
    }

    @Test
    void testListDatabases() {
        List<String> actual = catalog.listDatabases();

        assertThat(actual).isEqualTo(Arrays.asList("postgres", "test"));
    }

    @Test
    void testDbExists() {
        assertThat(catalog.databaseExists("nonexistent")).isFalse();

        assertThat(catalog.databaseExists(OracleCatalog.DEFAULT_DATABASE)).isTrue();
    }

    // ------ tables ------

    @Test
    void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(OracleCatalog.DEFAULT_DATABASE);

        assertThat(actual)
            .isEqualTo(
                Arrays.asList(
                    "public.array_table",
                    "public.primitive_table",
                    "public.primitive_table2",
                    "public.serial_table",
                    "public.t1",
                    "public.t4",
                    "public.t5"));

        actual = catalog.listTables(TEST_DB);

        assertThat(actual).isEqualTo(Arrays.asList("public.t2", "test_schema.t3"));
    }

    @Test
    void testListTables_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.listTables(OracleCatalog.DEFAULT_DATABASE))
            .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    void testTableExists() {
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, "nonexist"))).isFalse();

        assertThat(catalog.tableExists(new ObjectPath(OracleCatalog.DEFAULT_DATABASE, TABLE1)))
            .isTrue();
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, TABLE2))).isTrue();
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, "test_schema.t3"))).isTrue();
    }

    @Test
    void testGetTables_TableNotExistException() {
        assertThatThrownBy(
            () ->
                catalog.getTable(
                    new ObjectPath(
                        TEST_DB,
                        PostgresTablePath.toFlinkTableName(
                            TEST_SCHEMA, "anytable"))))
            .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testGetTables_TableNotExistException_NoSchema() {
        assertThatThrownBy(
            () ->
                catalog.getTable(
                    new ObjectPath(
                        TEST_DB,
                        PostgresTablePath.toFlinkTableName(
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
                        PostgresTablePath.toFlinkTableName(
                            TEST_SCHEMA, "anytable"))))
            .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testGetTable() throws org.apache.flink.table.catalog.exceptions.TableNotExistException {
        // test postgres.public.user1
        Schema schema = getSimpleTable().schema;

        CatalogBaseTable table = catalog.getTable(new ObjectPath("postgres", TABLE1));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        table = catalog.getTable(new ObjectPath("postgres", "public.t1"));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        // test testdb.public.user2
        table = catalog.getTable(new ObjectPath(TEST_DB, TABLE2));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        table = catalog.getTable(new ObjectPath(TEST_DB, "public.t2"));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        // test testdb.testschema.user2
        table = catalog.getTable(new ObjectPath(TEST_DB, TEST_SCHEMA + ".t3"));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);
    }

    @Test
    void testPrimitiveDataTypes() throws TableNotExistException {
        CatalogBaseTable table =
            catalog.getTable(
                new ObjectPath(OracleCatalog.DEFAULT_DATABASE, TABLE_PRIMITIVE_TYPE));

        assertThat(table.getUnresolvedSchema()).isEqualTo(getPrimitiveTable().schema);
    }

    @Test
    void testArrayDataTypes() throws TableNotExistException {
        CatalogBaseTable table =
            catalog.getTable(
                new ObjectPath(OracleCatalog.DEFAULT_DATABASE, TABLE_ARRAY_TYPE));

        assertThat(table.getUnresolvedSchema()).isEqualTo(getArrayTable().schema);
    }

    @Test
    public void testSerialDataTypes() throws TableNotExistException {
        CatalogBaseTable table =
            catalog.getTable(
                new ObjectPath(OracleCatalog.DEFAULT_DATABASE, TABLE_SERIAL_TYPE));

        assertThat(table.getUnresolvedSchema()).isEqualTo(getSerialTable().schema);
    }
}
