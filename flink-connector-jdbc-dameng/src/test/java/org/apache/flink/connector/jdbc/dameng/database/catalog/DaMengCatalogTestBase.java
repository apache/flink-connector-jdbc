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

import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test base for {@link DaMengCatalog}. */
abstract class DaMengCatalogTestBase implements JdbcITCaseBase, DatabaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(DaMengCatalogTestBase.class);
    private static final String TEST_CATALOG_NAME = "dameng_catalog";
    private static final String TEST_DB = "SYSDBA";
    private static final String TEST_DB2 = "TEST2";

    private static final TableRow TABLE_ALL_TYPES = createTableAllTypeTable("T_ALL_TYPES");
    private static final TableRow TABLE_ALL_TYPES_SINK =
            createTableAllTypeTable("T_ALL_TYPES_SINK");
    private static final TableRow TABLE_GROUPED_BY_SINK = createGroupedTable("T_GROUPED_BY_SINK");
    private static final TableRow TABLE_PK = createGroupedTable("T_PK");
    private static final TableRow TABLE_PK2 =
            tableRow(
                    "T_PK",
                    pkField(
                            "PID",
                            dbType("INT IDENTITY"),
                            DataTypes.BIGINT().notNull()),
                    field("COL_VARCHAR", dbType("VARCHAR(255)"), DataTypes.BIGINT()));

    private static final List<Row> TABLE_ALL_TYPES_ROWS =
            Arrays.asList(
                    Row.ofKind(
                            RowKind.INSERT,
                            1L,
                            -1L,
                            new BigDecimal(1),
                            null,
                            true,
                            null,
                            "hello",
                            Date.valueOf("2021-08-04").toLocalDate(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            new BigDecimal(-1),
                            new BigDecimal(1),
                            -1.0d,
                            1.0d,
                            "enum2",
                            -9.1f,
                            9.1f,
                            -1,
                            1L,
                            -1,
                            1L,
                            null,
                            "col_longtext",
                            null,
                            -1,
                            1,
                            "col_mediumtext",
                            new BigDecimal(-99),
                            new BigDecimal(99),
                            -1.0d,
                            1.0d,
                            "set_ele1",
                            Short.parseShort("-1"),
                            1,
                            "col_text",
                            Time.valueOf("10:32:34").toLocalTime(),
                            Timestamp.valueOf("2021-08-04 01:54:16").toLocalDateTime(),
                            "col_tinytext",
                            Byte.parseByte("-1"),
                            Short.parseShort("1"),
                            null,
                            "col_varchar",
                            Timestamp.valueOf("2021-08-04 01:54:16.463").toLocalDateTime(),
                            Time.valueOf("09:33:43").toLocalTime(),
                            Timestamp.valueOf("2021-08-04 01:54:16.463").toLocalDateTime(),
                            null),
                    Row.ofKind(
                            RowKind.INSERT,
                            2L,
                            -1L,
                            new BigDecimal(1),
                            null,
                            true,
                            null,
                            "hello",
                            Date.valueOf("2021-08-04").toLocalDate(),
                            Timestamp.valueOf("2021-08-04 01:53:19").toLocalDateTime(),
                            new BigDecimal(-1),
                            new BigDecimal(1),
                            -1.0d,
                            1.0d,
                            "enum2",
                            -9.1f,
                            9.1f,
                            -1,
                            1L,
                            -1,
                            1L,
                            null,
                            "col_longtext",
                            null,
                            -1,
                            1,
                            "col_mediumtext",
                            new BigDecimal(-99),
                            new BigDecimal(99),
                            -1.0d,
                            1.0d,
                            "set_ele1,set_ele12",
                            Short.parseShort("-1"),
                            1,
                            "col_text",
                            Time.valueOf("10:32:34").toLocalTime(),
                            Timestamp.valueOf("2021-08-04 01:53:19").toLocalDateTime(),
                            "col_tinytext",
                            Byte.parseByte("-1"),
                            Short.parseShort("1"),
                            null,
                            "col_varchar",
                            Timestamp.valueOf("2021-08-04 01:53:19.098").toLocalDateTime(),
                            Time.valueOf("09:33:43").toLocalTime(),
                            Timestamp.valueOf("2021-08-04 01:53:19.098").toLocalDateTime(),
                            null));
    private DaMengCatalog catalog;
    private TableEnvironment tEnv;

    @Override
    public List<TableManaged> getManagedTables() {
        return Arrays.asList(
                TABLE_ALL_TYPES, TABLE_ALL_TYPES_SINK, TABLE_GROUPED_BY_SINK, TABLE_PK);
    }

    private static TableRow createTableAllTypeTable(String tableName) {
        return tableRow(
                tableName,
                pkField(
                        "PID",
                        dbType("BIGINT IDENTITY"),
                        DataTypes.BIGINT().notNull()),
                field("COL_BIGINT", dbType("BIGINT"), DataTypes.BIGINT()),
                field(
                        "COL_BIGINT_UNSIGNED",
                        dbType("DECIMAL(20,0)"),
                        DataTypes.DECIMAL(20, 0)),
                field("COL_BINARY", dbType("BINARY(100)"), DataTypes.BYTES()),
                field("COL_BIT", dbType("BIT"), DataTypes.BOOLEAN()),
                field("COL_BLOB", dbType("BLOB"), DataTypes.BYTES()),
                field("COL_CHAR", dbType("CHAR(10)"), DataTypes.CHAR(10)),
                field("COL_DATE", dbType("DATE"), DataTypes.DATE()),
                field(
                        "COL_DATETIME",
                        dbType("TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                        DataTypes.TIMESTAMP(0)),
                field("COL_DECIMAL", dbType("DECIMAL(10,0)"), DataTypes.DECIMAL(10, 0)),
                field(
                        "COL_DECIMAL_UNSIGNED",
                        dbType("DECIMAL(11,0)"),
                        DataTypes.DECIMAL(11, 0)),
                field("COL_DOUBLE", dbType("DOUBLE"), DataTypes.DOUBLE()),
                field("COL_DOUBLE_UNSIGNED", dbType("DOUBLE"), DataTypes.DOUBLE()),
                field("COL_ENUM", dbType("CHAR(6)"), DataTypes.CHAR(6)),
                field("COL_FLOAT", dbType("FLOAT"), DataTypes.FLOAT()),
                field("COL_FLOAT_UNSIGNED", dbType("FLOAT"), DataTypes.FLOAT()),
                field("COL_INT", dbType("INT"), DataTypes.INT()),
                field("COL_INT_UNSIGNED", dbType("BIGINT"), DataTypes.BIGINT()),
                field("COL_INTEGER", dbType("INT"), DataTypes.INT()),
                field("COL_INTEGER_UNSIGNED", dbType("BIGINT"), DataTypes.BIGINT()),
                field("COL_LONGBLOB", dbType("BLOB"), DataTypes.BYTES()),
                field(
                        "COL_LONGTEXT",
                        dbType("CLOB"),
                        DataTypes.STRING()),
                field("COL_MEDIUMBLOB", dbType("BLOB"), DataTypes.BYTES()),
                field("COL_MEDIUMINT", dbType("INT"), DataTypes.INT()),
                field("COL_MEDIUMINT_UNSIGNED", dbType("INT"), DataTypes.INT()),
                field("COL_MEDIUMTEXT", dbType("VARCHAR(32000)"), DataTypes.VARCHAR(32000)),
                field("COL_NUMERIC", dbType("DECIMAL(10,0)"), DataTypes.DECIMAL(10, 0)),
                field(
                        "COL_NUMERIC_UNSIGNED",
                        dbType("DECIMAL(11,0)"),
                        DataTypes.DECIMAL(11, 0)),
                field("COL_REAL", dbType("DOUBLE"), DataTypes.DOUBLE()),
                field("COL_REAL_UNSIGNED", dbType("DOUBLE"), DataTypes.DOUBLE()),
                field("COL_SET", dbType("CHAR(18)"), DataTypes.CHAR(18)),
                field("COL_SMALLINT", dbType("SMALLINT"), DataTypes.SMALLINT()),
                field("COL_SMALLINT_UNSIGNED", dbType("INT"), DataTypes.INT()),
                field("COL_TEXT", dbType("VARCHAR(21845)"), DataTypes.VARCHAR(21845)),
                field("COL_TIME", dbType("TIME"), DataTypes.TIME(0)),
                field(
                        "COL_TIMESTAMP",
                        dbType("TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                        DataTypes.TIMESTAMP(0)),
                field("COL_TINYTEXT", dbType("VARCHAR(85)"), DataTypes.VARCHAR(85)),
                field("COL_TINYINT", dbType("TINYINT"), DataTypes.TINYINT()),
                field(
                        "COL_TINYINT_UNSINGED",
                        dbType("SMALLINT"),
                        DataTypes.SMALLINT()),
                field("COL_TINYBLOB", dbType("BLOB"), DataTypes.BYTES()),
                field("COL_VARCHAR", dbType("VARCHAR(255)"), DataTypes.VARCHAR(255)),
                field(
                        "COL_DATETIME_P3",
                        dbType("TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP"),
                        DataTypes.TIMESTAMP(3).notNull()),
                field("COL_TIME_P3", dbType("TIME(3)"), DataTypes.TIME(3)),
                field(
                        "COL_TIMESTAMP_P3",
                        dbType("TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP"),
                        DataTypes.TIMESTAMP(3)),
                field("COL_VARBINARY", dbType("VARBINARY(255)"), DataTypes.BYTES()));
    }

    private static TableRow createGroupedTable(String tableName) {
        return tableRow(
                tableName,
                pkField(
                        "PID",
                        dbType("BIGINT IDENTITY"),
                        DataTypes.BIGINT().notNull()),
                field("COL_BIGINT", dbType("BIGINT"), DataTypes.BIGINT()));
    }

    @BeforeEach
    void setup() {
        try (Connection conn = getMetadata().getConnection();
                Statement st = conn.createStatement()) {
            st.execute(String.format("SET IDENTITY_INSERT \"%s\" ON", TABLE_ALL_TYPES.getTableName()));

            TABLE_ALL_TYPES.insertIntoTableValues(
                    conn,
                    "1, -1, 1, null, 1, null, 'hello', '2021-08-04', '2021-08-04 01:54:16', -1, 1, -1, 1, 'enum2', -9.1, 9.1, -1, 1, -1, 1, null, 'col_longtext', null, -1, 1, 'col_mediumtext', -99, 99, -1, 1, 'set_ele1', -1, 1, 'col_text', '10:32:34', '2021-08-04 01:54:16', 'col_tinytext', -1, 1, null, 'col_varchar', '2021-08-04 01:54:16.463', '09:33:43.000', '2021-08-04 01:54:16.463', null",
                    "2, -1, 1, null, 1, null, 'hello', '2021-08-04', '2021-08-04 01:53:19', -1, 1, -1, 1, 'enum2', -9.1, 9.1, -1, 1, -1, 1, null, 'col_longtext', null, -1, 1, 'col_mediumtext', -99, 99, -1, 1, 'set_ele1,set_ele12', -1, 1, 'col_text', '10:32:34', '2021-08-04 01:53:19', 'col_tinytext', -1, 1, null, 'col_varchar', '2021-08-04 01:53:19.098', '09:33:43.000', '2021-08-04 01:53:19.098', null");

            st.execute(String.format("SET IDENTITY_INSERT \"%s\" OFF", TABLE_ALL_TYPES.getTableName()));
            st.execute(String.format("CREATE SCHEMA \"%s\"", TEST_DB2));
            st.execute(String.format("SET SCHEMA \"%s\"", TEST_DB2));
            st.execute(TABLE_PK2.getCreateQuery());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        catalog =
                new DaMengCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        TEST_DB,
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        getMetadata()
                                .getJdbcUrl()
                                .substring(0, getMetadata().getJdbcUrl().lastIndexOf("/")));

        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        // Use DaMeng catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    @AfterEach
    void afterEach() {
        try (Connection conn = getMetadata().getConnection();
                Statement st = conn.createStatement()) {
            st.execute(String.format("DROP SCHEMA \"%s\" CASCADE", TEST_DB2));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testGetDb_DatabaseNotExistException() {
        String databaseNotExist = "NONEXISTENT";
        assertThatThrownBy(() -> catalog.getDatabase(databaseNotExist))
                .satisfies(
                        anyCauseMatches(
                                DatabaseNotExistException.class,
                                String.format(
                                        "Database %s does not exist in Catalog",
                                        databaseNotExist)));
    }

    @Test
    void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertThat(actual).contains(TEST_DB, TEST_DB2);
    }

    @Test
    void testDbExists() {
        String databaseNotExist = "NONEXISTENT";
        assertThat(catalog.databaseExists(databaseNotExist)).isFalse();
        assertThat(catalog.databaseExists(TEST_DB)).isTrue();
    }

    // ------ tables ------

    @Test
    void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(TEST_DB);
        List<String> expectedTables = getManagedTables().stream()
                .map(TableManaged::getTableName)
                .collect(Collectors.toList());

        assertThat(actual).containsAll(expectedTables);
    }

    @Test
    void testListTables_DatabaseNotExistException() {
        String anyDatabase = "ANYDATABASE";
        assertThatThrownBy(() -> catalog.listTables(anyDatabase))
                .satisfies(
                        anyCauseMatches(
                                DatabaseNotExistException.class,
                                String.format(
                                        "Database %s does not exist in Catalog", anyDatabase)));
    }

    @Test
    void testTableExists() {
        String tableNotExist = "NONEXIST";
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, tableNotExist))).isFalse();
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, TABLE_ALL_TYPES.getTableName())))
                .isTrue();
    }

    @Test
    void testGetTables_TableNotExistException() {
        String anyTableNotExist = "ANYTABLE";
        assertThatThrownBy(() -> catalog.getTable(new ObjectPath(TEST_DB, anyTableNotExist)))
                .satisfies(
                        anyCauseMatches(
                                TableNotExistException.class,
                                String.format(
                                        "Table (or view) %s.%s does not exist in Catalog",
                                        TEST_DB, anyTableNotExist)));
    }

    @Test
    void testGetTables_TableNotExistException_NoDb() {
        String databaseNotExist = "NONEXISTDB";
        String tableNotExist = "ANYTABLE";
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
                catalog.getTable(new ObjectPath(TEST_DB, TABLE_ALL_TYPES.getTableName()));
        assertThat(table.getUnresolvedSchema()).isEqualTo(TABLE_ALL_TYPES.getTableSchema());
    }

    @Test
    void testGetTablePrimaryKey() throws TableNotExistException {
        // test the PK of test.t_user
        Schema tableSchemaTestPK1 = TABLE_PK.getTableSchema();
        CatalogBaseTable tablePK1 =
                catalog.getTable(new ObjectPath(TEST_DB, TABLE_PK.getTableName()));
        assertThat(tableSchemaTestPK1.getPrimaryKey())
                .isEqualTo(tablePK1.getUnresolvedSchema().getPrimaryKey());

        // test the PK of TEST_DB2.t_user
        Schema tableSchemaTestPK2 = TABLE_PK2.getTableSchema();
        CatalogBaseTable tablePK2 =
                catalog.getTable(new ObjectPath(TEST_DB2, TABLE_PK2.getTableName()));
        assertThat(tableSchemaTestPK2.getPrimaryKey())
                .isEqualTo(tablePK2.getUnresolvedSchema().getPrimaryKey());
    }

    // ------ test select query. ------

    @Test
    void testSelectField() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "SELECT PID FROM %s",
                                                TABLE_ALL_TYPES.getTableName()))
                                .execute()
                                .collect());
        assertThat(results)
                .isEqualTo(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, 1L), Row.ofKind(RowKind.INSERT, 2L)));
    }

    @Test
    void testWithoutCatalogDB() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "SELECT * FROM %s", TABLE_ALL_TYPES.getTableName()))
                                .execute()
                                .collect());

        assertThat(results).isEqualTo(TABLE_ALL_TYPES_ROWS);
    }

    @Test
    void testWithoutCatalog() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "SELECT * FROM %s.%s",
                                                TEST_DB, TABLE_ALL_TYPES.getTableName()))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(TABLE_ALL_TYPES_ROWS);
    }

    @Test
    void testFullPath() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "SELECT * FROM %s.%s.%s",
                                                TEST_CATALOG_NAME,
                                                catalog.getDefaultDatabase(),
                                                TABLE_ALL_TYPES.getTableName()))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(TABLE_ALL_TYPES_ROWS);
    }

    @Test
    void testSelectToInsert() throws Exception {
        String sql =
                String.format(
                        "INSERT INTO %s SELECT * FROM %s",
                        TABLE_ALL_TYPES_SINK.getTableName(), TABLE_ALL_TYPES.getTableName());
        tEnv.executeSql(sql).await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "SELECT * FROM %s",
                                                TABLE_ALL_TYPES_SINK.getTableName()))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(TABLE_ALL_TYPES_ROWS);
    }

    @Test
    void testGroupByInsert() throws Exception {
        // Changes primary key for the next record.
        tEnv.executeSql(
                        String.format(
                                "insert into %s select max(PID) PID, COL_BIGINT from %s "
                                        + "group by COL_BIGINT ",
                                TABLE_GROUPED_BY_SINK.getTableName(),
                                TABLE_ALL_TYPES.getTableName()))
                .await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from \"%s\"",
                                                TABLE_GROUPED_BY_SINK.getTableName()))
                                .execute()
                                .collect());
        assertThat(results)
                .isEqualTo(Collections.singletonList(Row.ofKind(RowKind.INSERT, 2L, -1L)));
    }
}
