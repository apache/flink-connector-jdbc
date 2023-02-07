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

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test base for {@link MySqlCatalog}. */
abstract class MySqlCatalogTestBase {

    public static final Logger LOG = LoggerFactory.getLogger(MySqlCatalogTestBase.class);
    protected static final String TEST_CATALOG_NAME = "mysql_catalog";
    protected static final String TEST_USERNAME = "mysql";
    protected static final String TEST_PWD = "mysql";
    protected static final String TEST_DB = "test";
    protected static final String TEST_DB2 = "test2";
    protected static final String TEST_TABLE_ALL_TYPES = "t_all_types";
    protected static final String TEST_SINK_TABLE_ALL_TYPES = "t_all_types_sink";
    protected static final String TEST_TABLE_SINK_FROM_GROUPED_BY = "t_grouped_by_sink";
    protected static final String TEST_TABLE_PK = "t_pk";
    protected static final String MYSQL_INIT_SCRIPT = "mysql-scripts/catalog-init-for-test.sql";
    protected static final Map<String, String> DEFAULT_CONTAINER_ENV_MAP =
            new HashMap<String, String>() {
                {
                    put("MYSQL_ROOT_HOST", "%");
                }
            };

    protected static final Schema TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("pid", DataTypes.BIGINT().notNull())
                    .column("col_bigint", DataTypes.BIGINT())
                    .column("col_bigint_unsigned", DataTypes.DECIMAL(20, 0))
                    .column("col_binary", DataTypes.BYTES())
                    .column("col_bit", DataTypes.BOOLEAN())
                    .column("col_blob", DataTypes.BYTES())
                    .column("col_char", DataTypes.CHAR(10))
                    .column("col_date", DataTypes.DATE())
                    .column("col_datetime", DataTypes.TIMESTAMP(0))
                    .column("col_decimal", DataTypes.DECIMAL(10, 0))
                    .column("col_decimal_unsigned", DataTypes.DECIMAL(11, 0))
                    .column("col_double", DataTypes.DOUBLE())
                    .column("col_double_unsigned", DataTypes.DOUBLE())
                    .column("col_enum", DataTypes.CHAR(6))
                    .column("col_float", DataTypes.FLOAT())
                    .column("col_float_unsigned", DataTypes.FLOAT())
                    .column("col_int", DataTypes.INT())
                    .column("col_int_unsigned", DataTypes.BIGINT())
                    .column("col_integer", DataTypes.INT())
                    .column("col_integer_unsigned", DataTypes.BIGINT())
                    .column("col_longblob", DataTypes.BYTES())
                    .column("col_longtext", DataTypes.STRING())
                    .column("col_mediumblob", DataTypes.BYTES())
                    .column("col_mediumint", DataTypes.INT())
                    .column("col_mediumint_unsigned", DataTypes.INT())
                    .column("col_mediumtext", DataTypes.VARCHAR(5592405))
                    .column("col_numeric", DataTypes.DECIMAL(10, 0))
                    .column("col_numeric_unsigned", DataTypes.DECIMAL(11, 0))
                    .column("col_real", DataTypes.DOUBLE())
                    .column("col_real_unsigned", DataTypes.DOUBLE())
                    .column("col_set", DataTypes.CHAR(18))
                    .column("col_smallint", DataTypes.SMALLINT())
                    .column("col_smallint_unsigned", DataTypes.INT())
                    .column("col_text", DataTypes.VARCHAR(21845))
                    .column("col_time", DataTypes.TIME(0))
                    .column("col_timestamp", DataTypes.TIMESTAMP(0))
                    .column("col_tinytext", DataTypes.VARCHAR(85))
                    .column("col_tinyint", DataTypes.TINYINT())
                    .column("col_tinyint_unsinged", DataTypes.SMALLINT())
                    .column("col_tinyblob", DataTypes.BYTES())
                    .column("col_varchar", DataTypes.VARCHAR(255))
                    .column("col_datetime_p3", DataTypes.TIMESTAMP(3).notNull())
                    .column("col_time_p3", DataTypes.TIME(3))
                    .column("col_timestamp_p3", DataTypes.TIMESTAMP(3))
                    .column("col_varbinary", DataTypes.BYTES())
                    .primaryKeyNamed("PRIMARY", Lists.newArrayList("pid"))
                    .build();

    protected static final List<Row> TABLE_ROWS =
            Lists.newArrayList(
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

    protected static MySQLContainer<?> createContainer(String dockerImage) {
        return new MySQLContainer<>(DockerImageName.parse(dockerImage))
                .withUsername("root")
                .withPassword("")
                .withEnv(DEFAULT_CONTAINER_ENV_MAP)
                .withInitScript(MYSQL_INIT_SCRIPT)
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    protected abstract String getDatabaseUrl();

    @BeforeEach
    void setup() {
        catalog =
                new MySqlCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        TEST_DB,
                        TEST_USERNAME,
                        TEST_PWD,
                        getDatabaseUrl());

        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        // Use mysql catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
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
                        Arrays.asList(
                                TEST_TABLE_ALL_TYPES,
                                TEST_SINK_TABLE_ALL_TYPES,
                                TEST_TABLE_SINK_FROM_GROUPED_BY,
                                TEST_TABLE_PK));
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
        assertThat(catalog.tableExists(new ObjectPath(TEST_DB, TEST_TABLE_ALL_TYPES))).isTrue();
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
        CatalogBaseTable table = catalog.getTable(new ObjectPath(TEST_DB, TEST_TABLE_ALL_TYPES));
        assertThat(table.getUnresolvedSchema()).isEqualTo(TABLE_SCHEMA);
    }

    @Test
    void testGetTablePrimaryKey() throws TableNotExistException {
        // test the PK of test.t_user
        Schema tableSchemaTestPK1 =
                Schema.newBuilder()
                        .column("uid", DataTypes.BIGINT().notNull())
                        .column("col_bigint", DataTypes.BIGINT())
                        .primaryKeyNamed("PRIMARY", Collections.singletonList("uid"))
                        .build();
        CatalogBaseTable tablePK1 = catalog.getTable(new ObjectPath(TEST_DB, TEST_TABLE_PK));
        assertThat(tableSchemaTestPK1.getPrimaryKey())
                .contains(tablePK1.getUnresolvedSchema().getPrimaryKey().get());

        // test the PK of test2.t_user
        Schema tableSchemaTestPK2 =
                Schema.newBuilder()
                        .column("pid", DataTypes.INT().notNull())
                        .column("col_varchar", DataTypes.VARCHAR(255))
                        .primaryKeyNamed("PRIMARY", Collections.singletonList("pid"))
                        .build();
        CatalogBaseTable tablePK2 = catalog.getTable(new ObjectPath(TEST_DB2, TEST_TABLE_PK));
        assertThat(tableSchemaTestPK2.getPrimaryKey())
                .contains(tablePK2.getUnresolvedSchema().getPrimaryKey().get());
    }

    // ------ test select query. ------

    @Test
    void testSelectField() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select pid from %s", TEST_TABLE_ALL_TYPES))
                                .execute()
                                .collect());
        assertThat(results)
                .isEqualTo(
                        Lists.newArrayList(
                                Row.ofKind(RowKind.INSERT, 1L), Row.ofKind(RowKind.INSERT, 2L)));
    }

    @Test
    void testWithoutCatalogDB() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TEST_TABLE_ALL_TYPES))
                                .execute()
                                .collect());

        assertThat(results).isEqualTo(TABLE_ROWS);
    }

    @Test
    void testWithoutCatalog() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`.`%s`",
                                                TEST_DB, TEST_TABLE_ALL_TYPES))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(TABLE_ROWS);
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
                                                TEST_TABLE_ALL_TYPES))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(TABLE_ROWS);
    }

    @Test
    void testSelectToInsert() throws Exception {

        String sql =
                String.format(
                        "insert into `%s` select * from `%s`",
                        TEST_SINK_TABLE_ALL_TYPES, TEST_TABLE_ALL_TYPES);
        tEnv.executeSql(sql).await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TEST_SINK_TABLE_ALL_TYPES))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(TABLE_ROWS);
    }

    @Test
    void testGroupByInsert() throws Exception {
        // Changes primary key for the next record.
        tEnv.executeSql(
                        String.format(
                                "insert into `%s` select max(`pid`) `pid`, `col_bigint` from `%s` "
                                        + "group by `col_bigint` ",
                                TEST_TABLE_SINK_FROM_GROUPED_BY, TEST_TABLE_ALL_TYPES))
                .await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`",
                                                TEST_TABLE_SINK_FROM_GROUPED_BY))
                                .execute()
                                .collect());
        assertThat(results).isEqualTo(Lists.newArrayList(Row.ofKind(RowKind.INSERT, 2L, -1L)));
    }
}
