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

package org.apache.flink.connector.jdbc.databases.mysql.catalog;

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

/** Test base for {@link MySqlCatalog}. */
public abstract class MySqlCatalogTestBase implements JdbcITCaseBase, DatabaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCatalogTestBase.class);
    private static final String TEST_CATALOG_NAME = "mysql_catalog";
    private static final String TEST_DB = "test";
    private static final String TEST_DB2 = "test2";

    private static final TableRow TABLE_ALL_TYPES = createTableAllTypeTable("t_all_types");
    private static final TableRow TABLE_ALL_TYPES_SINK =
            createTableAllTypeTable("t_all_types_sink");
    private static final TableRow TABLE_GROUPED_BY_SINK = createGroupedTable("t_grouped_by_sink");
    private static final TableRow TABLE_PK = createGroupedTable("t_pk");
    private static final TableRow TABLE_PK2 =
            tableRow(
                    "t_pk",
                    pkField(
                            "pid",
                            dbType("int(11) NOT NULL AUTO_INCREMENT"),
                            DataTypes.BIGINT().notNull()),
                    field("col_varchar", dbType("varchar(255)"), DataTypes.BIGINT()));

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
    private MySqlCatalog catalog;
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
                        "pid",
                        dbType("bigint(20) NOT NULL AUTO_INCREMENT"),
                        DataTypes.BIGINT().notNull()),
                field("col_bigint", dbType("bigint(20)"), DataTypes.BIGINT()),
                field(
                        "col_bigint_unsigned",
                        dbType("bigint(20) unsigned"),
                        DataTypes.DECIMAL(20, 0)),
                field("col_binary", dbType("binary(100)"), DataTypes.BYTES()),
                field("col_bit", dbType("bit(1)"), DataTypes.BOOLEAN()),
                field("col_blob", dbType("blob"), DataTypes.BYTES()),
                field("col_char", dbType("char(10)"), DataTypes.CHAR(10)),
                field("col_date", dbType("date"), DataTypes.DATE()),
                field(
                        "col_datetime",
                        dbType("datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
                        DataTypes.TIMESTAMP(0)),
                field("col_decimal", dbType("decimal(10,0)"), DataTypes.DECIMAL(10, 0)),
                field(
                        "col_decimal_unsigned",
                        dbType("decimal(10,0) unsigned"),
                        DataTypes.DECIMAL(11, 0)),
                field("col_double", dbType("double"), DataTypes.DOUBLE()),
                field("col_double_unsigned", dbType("double unsigned"), DataTypes.DOUBLE()),
                field("col_enum", dbType("enum('enum1','enum2','enum11')"), DataTypes.CHAR(6)),
                field("col_float", dbType("float"), DataTypes.FLOAT()),
                field("col_float_unsigned", dbType("float unsigned"), DataTypes.FLOAT()),
                field("col_int", dbType("int(11)"), DataTypes.INT()),
                field("col_int_unsigned", dbType("int(10) unsigned"), DataTypes.BIGINT()),
                field("col_integer", dbType("int(11)"), DataTypes.INT()),
                field("col_integer_unsigned", dbType("int(10) unsigned"), DataTypes.BIGINT()),
                field("col_longblob", dbType("longblob"), DataTypes.BYTES()),
                field(
                        "col_longtext",
                        dbType("longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"),
                        DataTypes.STRING()),
                field("col_mediumblob", dbType("mediumblob"), DataTypes.BYTES()),
                field("col_mediumint", dbType("mediumint(9)"), DataTypes.INT()),
                field("col_mediumint_unsigned", dbType("mediumint(8) unsigned"), DataTypes.INT()),
                field("col_mediumtext", dbType("mediumtext"), DataTypes.VARCHAR(5592405)),
                field("col_numeric", dbType("decimal(10,0)"), DataTypes.DECIMAL(10, 0)),
                field(
                        "col_numeric_unsigned",
                        dbType("decimal(10,0) unsigned"),
                        DataTypes.DECIMAL(11, 0)),
                field("col_real", dbType("double"), DataTypes.DOUBLE()),
                field("col_real_unsigned", dbType("double unsigned"), DataTypes.DOUBLE()),
                field("col_set", dbType("set('set_ele1','set_ele12')"), DataTypes.CHAR(18)),
                field("col_smallint", dbType("smallint(6)"), DataTypes.SMALLINT()),
                field("col_smallint_unsigned", dbType("smallint(5) unsigned"), DataTypes.INT()),
                field("col_text", dbType("text"), DataTypes.VARCHAR(21845)),
                field("col_time", dbType("time"), DataTypes.TIME(0)),
                field(
                        "col_timestamp",
                        dbType(
                                "timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
                        DataTypes.TIMESTAMP(0)),
                field("col_tinytext", dbType("tinytext"), DataTypes.VARCHAR(85)),
                field("col_tinyint", dbType("tinyint"), DataTypes.TINYINT()),
                field(
                        "col_tinyint_unsinged",
                        dbType("tinyint(255) unsigned"),
                        DataTypes.SMALLINT()),
                field("col_tinyblob", dbType("tinyblob"), DataTypes.BYTES()),
                field("col_varchar", dbType("varchar(255)"), DataTypes.VARCHAR(255)),
                field(
                        "col_datetime_p3",
                        dbType(
                                "datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"),
                        DataTypes.TIMESTAMP(3).notNull()),
                field("col_time_p3", dbType("time(3)"), DataTypes.TIME(3)),
                field(
                        "col_timestamp_p3",
                        dbType(
                                "timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"),
                        DataTypes.TIMESTAMP(3)),
                field("col_varbinary", dbType("varbinary(255)"), DataTypes.BYTES()));
    }

    private static TableRow createGroupedTable(String tableName) {
        return tableRow(
                tableName,
                pkField(
                        "pid",
                        dbType("bigint(20) NOT NULL AUTO_INCREMENT"),
                        DataTypes.BIGINT().notNull()),
                field("col_bigint", dbType("bigint(20)"), DataTypes.BIGINT()));
    }

    @BeforeEach
    void setup() {
        try (Connection conn = getMetadata().getConnection();
                Statement st = conn.createStatement()) {
            TABLE_ALL_TYPES.insertIntoTableValues(
                    conn,
                    "1, -1, 1, null, b'1', null, 'hello', '2021-08-04', '2021-08-04 01:54:16', -1, 1, -1, 1, 'enum2', -9.1, 9.1, -1, 1, -1, 1, null, 'col_longtext', null, -1, 1, 'col_mediumtext', -99, 99, -1, 1, 'set_ele1', -1, 1, 'col_text', '10:32:34', '2021-08-04 01:54:16', 'col_tinytext', -1, 1, null, 'col_varchar', '2021-08-04 01:54:16.463', '09:33:43.000', '2021-08-04 01:54:16.463', null",
                    "2, -1, 1, null, b'1', null, 'hello', '2021-08-04', '2021-08-04 01:53:19', -1, 1, -1, 1, 'enum2', -9.1, 9.1, -1, 1, -1, 1, null, 'col_longtext', null, -1, 1, 'col_mediumtext', -99, 99, -1, 1, 'set_ele1,set_ele12', -1, 1, 'col_text', '10:32:34', '2021-08-04 01:53:19', 'col_tinytext', -1, 1, null, 'col_varchar', '2021-08-04 01:53:19.098', '09:33:43.000', '2021-08-04 01:53:19.098', null");

            st.execute(String.format("CREATE DATABASE `%s` CHARSET=utf8", TEST_DB2));
            st.execute(String.format("use `%s`", TEST_DB2));
            st.execute(TABLE_PK2.getCreateQuery());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        catalog =
                new MySqlCatalog(
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

        // Use mysql catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    @AfterEach
    void afterEach() {
        try (Connection conn = getMetadata().getConnection();
                Statement st = conn.createStatement()) {
            st.execute(String.format("DROP DATABASE IF EXISTS `%s`", TEST_DB2));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
    void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertThat(actual).containsExactly(TEST_DB, TEST_DB2);
    }

    @Test
    void testDbExists() {
        String databaseNotExist = "nonexistent";
        assertThat(catalog.databaseExists(databaseNotExist)).isFalse();
        assertThat(catalog.databaseExists(TEST_DB)).isTrue();
    }

    // ------ tables ------

    @Test
    void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(TEST_DB);
        assertThat(actual)
                .isEqualTo(
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
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, tableNotExist))).isFalse();
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, TABLE_ALL_TYPES.getTableName())))
                .isTrue();
    }

    @Test
    void testGetTables_TableNotExistException() {
        String anyTableNotExist = "anyTable";
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
                                                "select pid from %s",
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
                                                "select * from %s", TABLE_ALL_TYPES.getTableName()))
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
                                                "select * from `%s`.`%s`",
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
                                                "select * from %s.%s.`%s`",
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
                        "insert into `%s` select * from `%s`",
                        TABLE_ALL_TYPES_SINK.getTableName(), TABLE_ALL_TYPES.getTableName());
        tEnv.executeSql(sql).await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s",
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
                                "insert into `%s` select max(`pid`) `pid`, `col_bigint` from `%s` "
                                        + "group by `col_bigint` ",
                                TABLE_GROUPED_BY_SINK.getTableName(),
                                TABLE_ALL_TYPES.getTableName()))
                .await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`",
                                                TABLE_GROUPED_BY_SINK.getTableName()))
                                .execute()
                                .collect());
        assertThat(results)
                .isEqualTo(Collections.singletonList(Row.ofKind(RowKind.INSERT, 2L, -1L)));
    }
}
