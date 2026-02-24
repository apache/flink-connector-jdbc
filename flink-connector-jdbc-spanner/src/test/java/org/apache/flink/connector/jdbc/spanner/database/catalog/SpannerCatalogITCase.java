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

package org.apache.flink.connector.jdbc.spanner.database.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** E2E test for {@link SpannerCatalog}. */
class SpannerCatalogITCase extends SpannerCatalogTestBase {

    // ------ databases ------

    @Test
    void testGetDatabase_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.getDatabase("nonexistent"))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessageContaining("Database nonexistent does not exist in Catalog");
    }

    @Test
    void testListDatabases() {
        List<String> actual = catalog.listDatabases();

        assertThat(actual).isEqualTo(Arrays.asList(metadata.getDatabaseName()));
    }

    @Test
    void testDatabaseExists() {
        assertThat(catalog.databaseExists("nonexistent")).isFalse();

        assertThat(catalog.databaseExists(metadata.getDatabaseName())).isTrue();
    }

    // ------ tables ------

    @Test
    void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(metadata.getDatabaseName());

        assertThat(actual)
                .containsExactlyInAnyOrder(
                        TABLE_SIMPLE.getTableName(),
                        TABLE_SIMPLE_WITH_SCHEMA.getTableName(),
                        TABLE_ALL_TYPES.getTableName(),
                        TABLE_ALL_TYPES_SINK.getTableName(),
                        TABLE_ARRAY_TYPES.getTableName(),
                        TABLE_GROUPED_BY_SINK.getTableName());
    }

    @Test
    void testListTables_DatabaseNotExistException() {
        assertThatThrownBy(() -> catalog.listTables("nonexistschema"))
                .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    void testTableExists() {
        assertThat(catalog.tableExists(new ObjectPath(metadata.getDatabaseName(), "nonexist")))
                .isFalse();

        assertThat(
                        catalog.tableExists(
                                new ObjectPath(
                                        metadata.getDatabaseName(), TABLE_SIMPLE.getTableName())))
                .isTrue();
        assertThat(
                        catalog.tableExists(
                                new ObjectPath(
                                        metadata.getDatabaseName(),
                                        TABLE_SIMPLE_WITH_SCHEMA.getTableName())))
                .isTrue();
        assertThat(
                        catalog.tableExists(
                                new ObjectPath(
                                        metadata.getDatabaseName(),
                                        TABLE_ALL_TYPES.getTableName())))
                .isTrue();
        assertThat(
                        catalog.tableExists(
                                new ObjectPath(
                                        metadata.getDatabaseName(),
                                        TABLE_ARRAY_TYPES.getTableName())))
                .isTrue();
    }

    @Test
    void testGetTables_TableNotExistException() {
        assertThatThrownBy(
                        () ->
                                catalog.getTable(
                                        new ObjectPath(
                                                metadata.getDatabaseName(),
                                                SpannerTablePath.toFlinkTableName(
                                                        TEST_SCHEMA, "anytable"))))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testGetTables_TableNotExistException_NoSchema() {
        assertThatThrownBy(
                        () ->
                                catalog.getTable(
                                        new ObjectPath(
                                                metadata.getDatabaseName(),
                                                SpannerTablePath.toFlinkTableName(
                                                        "nonexistschema", "anytable"))))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testGetTables_TableNotExistException_NoDatabase() {
        assertThatThrownBy(
                        () ->
                                catalog.getTable(
                                        new ObjectPath(
                                                "nonexistdb",
                                                SpannerTablePath.toFlinkTableName(
                                                        TEST_SCHEMA, "anytable"))))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testGetTable() throws TableNotExistException, CatalogException {
        Schema schema = TABLE_SIMPLE.getTableSchema("PRIMARY_KEY");
        CatalogBaseTable table =
                catalog.getTable(
                        new ObjectPath(metadata.getDatabaseName(), TABLE_SIMPLE.getTableName()));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        schema = TABLE_SIMPLE_WITH_SCHEMA.getTableSchema("PRIMARY_KEY");
        table =
                catalog.getTable(
                        new ObjectPath(
                                metadata.getDatabaseName(),
                                TABLE_SIMPLE_WITH_SCHEMA.getTableName()));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        schema = TABLE_ALL_TYPES.getTableSchema("PRIMARY_KEY");
        table =
                catalog.getTable(
                        new ObjectPath(metadata.getDatabaseName(), TABLE_ALL_TYPES.getTableName()));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);

        schema = TABLE_ARRAY_TYPES.getTableSchema("PRIMARY_KEY");
        table =
                catalog.getTable(
                        new ObjectPath(
                                metadata.getDatabaseName(), TABLE_ARRAY_TYPES.getTableName()));

        assertThat(table.getUnresolvedSchema()).isEqualTo(schema);
    }

    @Test
    void testGetTablePrimaryKey() throws TableNotExistException {
        Schema schemaAllTypes = TABLE_ALL_TYPES.getTableSchema("PRIMARY_KEY");
        CatalogBaseTable allTypes =
                catalog.getTable(
                        new ObjectPath(metadata.getDatabaseName(), TABLE_ALL_TYPES.getTableName()));
        assertThat(schemaAllTypes.getPrimaryKey())
                .isEqualTo(allTypes.getUnresolvedSchema().getPrimaryKey());
    }

    // ------ select queries ------

    @Test
    void testSelectField() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select id from `%s`",
                                                TABLE_ALL_TYPES.getTableName()))
                                .execute()
                                .collect());
        assertThat(results)
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "a"), Row.ofKind(RowKind.INSERT, "b"));
    }

    @Test
    void testWithoutSchema() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`", TABLE_SIMPLE.getTableName()))
                                .execute()
                                .collect());
        assertThat(results).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "12345-abcde"));
    }

    @Test
    void testWithSchema() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`",
                                                SpannerTablePath.fromFlinkTableName(
                                                        TABLE_SIMPLE_WITH_SCHEMA.getTableName())))
                                .execute()
                                .collect());
        assertThat(results).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "98765-fghij"));
    }

    @Test
    void testWithDatabaseWithoutSchema() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`.`%s`",
                                                metadata.getDatabaseName(),
                                                TABLE_SIMPLE.getTableName()))
                                .execute()
                                .collect());
        assertThat(results).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "12345-abcde"));
    }

    @Test
    void testWithDatabaseAndSchema() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`.`%s`",
                                                metadata.getDatabaseName(),
                                                SpannerTablePath.fromFlinkTableName(
                                                        TABLE_SIMPLE_WITH_SCHEMA.getTableName())))
                                .execute()
                                .collect());
        assertThat(results).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "98765-fghij"));
    }

    @Test
    void testFullPath() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`.`%s`.`%s`",
                                                TEST_CATALOG_NAME,
                                                metadata.getDatabaseName(),
                                                TABLE_SIMPLE.getTableName()))
                                .execute()
                                .collect());
        assertThat(results).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "12345-abcde"));
    }

    @Test
    void testFullPathWithSchema() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`.`%s`.`%s`",
                                                TEST_CATALOG_NAME,
                                                metadata.getDatabaseName(),
                                                SpannerTablePath.fromFlinkTableName(
                                                        TABLE_SIMPLE_WITH_SCHEMA.getTableName())))
                                .execute()
                                .collect());
        assertThat(results).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "98765-fghij"));
    }

    @Test
    void testInsert() throws Exception {
        String sql =
                String.format(
                        "insert into `%s` select id, col_bool, col_bytes, col_date, col_int64, "
                                + "col_numeric, col_float32, col_float64, col_string, col_timestamp from `%s`",
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
        assertThat(results)
                .containsExactlyInAnyOrder(
                        Row.ofKind(
                                RowKind.INSERT,
                                "a",
                                true,
                                "abc".getBytes(),
                                Date.valueOf("2025-01-01").toLocalDate(),
                                1L,
                                new BigDecimal("3.140000000"),
                                1.1f,
                                -1.1d,
                                "foo",
                                ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
                                        .withZoneSameInstant(ZoneId.systemDefault())
                                        .toLocalDateTime()),
                        Row.ofKind(
                                RowKind.INSERT,
                                "b",
                                false,
                                "123".getBytes(),
                                Date.valueOf("2025-12-31").toLocalDate(),
                                -1L,
                                new BigDecimal("-3.140000000"),
                                -1.1f,
                                1.1d,
                                "foo",
                                ZonedDateTime.of(2025, 12, 31, 0, 0, 0, 0, ZoneOffset.UTC)
                                        .withZoneSameInstant(ZoneId.systemDefault())
                                        .toLocalDateTime()));
    }

    @Test
    void testGroupByInsert() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "insert into `%s` select `col_string`, sum(`col_int64`) `col_int64_max` "
                                        + "from `%s` group by `col_string` ",
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
                .isEqualTo(Collections.singletonList(Row.ofKind(RowKind.INSERT, "foo", 0L)));
    }

    @Test
    void testAllTypes() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s", TABLE_ALL_TYPES.getTableName()))
                                .execute()
                                .collect());

        assertThat(results)
                .containsExactlyInAnyOrder(
                        Row.ofKind(
                                RowKind.INSERT,
                                "a",
                                true,
                                "abc".getBytes(),
                                Date.valueOf("2025-01-01").toLocalDate(),
                                "{\"key\":\"value1\"}",
                                1L,
                                new BigDecimal("3.140000000"),
                                1.1f,
                                -1.1d,
                                "foo",
                                ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
                                        .withZoneSameInstant(ZoneId.systemDefault())
                                        .toLocalDateTime()),
                        Row.ofKind(
                                RowKind.INSERT,
                                "b",
                                false,
                                "123".getBytes(),
                                Date.valueOf("2025-12-31").toLocalDate(),
                                "{\"key\":\"value2\"}",
                                -1L,
                                new BigDecimal("-3.140000000"),
                                -1.1f,
                                1.1d,
                                "foo",
                                ZonedDateTime.of(2025, 12, 31, 0, 0, 0, 0, ZoneOffset.UTC)
                                        .withZoneSameInstant(ZoneId.systemDefault())
                                        .toLocalDateTime()));
    }

    @Test
    void testArrayTypes() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s",
                                                TABLE_ARRAY_TYPES.getTableName()))
                                .execute()
                                .collect());

        assertThat(results)
                .containsExactlyInAnyOrder(
                        Row.ofKind(
                                RowKind.INSERT,
                                "a",
                                new Boolean[] {true, false},
                                new byte[][] {"abc".getBytes(), "123".getBytes()},
                                new LocalDate[] {
                                    Date.valueOf("2025-01-01").toLocalDate(),
                                    Date.valueOf("2025-12-31").toLocalDate()
                                },
                                new Long[] {1L, -1L},
                                new BigDecimal[] {
                                    new BigDecimal("3.140000000"), new BigDecimal("-3.140000000")
                                },
                                new Double[] {1.1d, -1.1d},
                                new String[] {"foo", "bar"},
                                new LocalDateTime[] {
                                    ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
                                            .withZoneSameInstant(ZoneId.systemDefault())
                                            .toLocalDateTime(),
                                    ZonedDateTime.of(2025, 12, 31, 0, 0, 0, 0, ZoneOffset.UTC)
                                            .withZoneSameInstant(ZoneId.systemDefault())
                                            .toLocalDateTime()
                                }));
    }
}
