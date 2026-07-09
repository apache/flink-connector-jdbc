/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot;

import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.SplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.Table;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatabaseSplitterEnumeratorTest {

    private static final TableId ORDERS = new TableId("catalog", "schema", "orders");
    private static final TableId CUSTOMERS = new TableId("catalog", "schema", "customers");

    @Test
    void testFanOutOverMultipleTables() {
        // Single-row tables each collapse to exactly one split, isolating the fan-out behavior
        // under test from the chunk-boundary counting already covered by
        // TableSplitterEnumeratorTest.
        FakeConnectionProvider connection =
                fakeConnectionWithTables(
                        tableEntry(ORDERS, values(1L)), tableEntry(CUSTOMERS, values(1L)));
        DatabaseSplitterEnumerator enumerator = databaseSplitterFor("catalog", "schema");

        enumerator.start(connection);
        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);

        assertThat(splits).hasSize(2);
        assertThat(enumerator.isAllSplitsFinished()).isTrue();
    }

    @Test
    void testTableFilterExcludesNonMatchingTables() {
        FakeConnectionProvider connection =
                fakeConnectionWithTables(
                        tableEntry(ORDERS, values(1L)), tableEntry(CUSTOMERS, values(1L)));
        DatabaseSplitterEnumerator enumerator =
                DatabaseSplitterEnumerator.builder()
                        .withCatalogName("catalog")
                        .withSchemaName("schema")
                        .withTableNames("orders")
                        .build();

        enumerator.start(connection);
        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getSqlTemplate()).contains(ORDERS.toString());
    }

    @Test
    void testTableFilterWithNoMatchThrows() {
        FakeConnectionProvider connection =
                fakeConnectionWithTables(tableEntry(ORDERS, values(1L)));
        DatabaseSplitterEnumerator enumerator =
                DatabaseSplitterEnumerator.builder()
                        .withCatalogName("catalog")
                        .withSchemaName("schema")
                        .withTableNames("does_not_exist")
                        .build();

        assertThatThrownBy(() -> enumerator.start(connection))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No tables found");
    }

    @Test
    void testNoTablesInDatabaseThrows() {
        FakeConnectionProvider connection =
                new FakeConnectionProvider(
                        Collections.emptySet(), new HashMap<>(), new HashMap<>());
        DatabaseSplitterEnumerator enumerator = databaseSplitterFor("catalog", "schema");

        assertThatThrownBy(() -> enumerator.start(connection))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No tables found");
    }

    @Test
    void testLineageQueriesAggregatesFromAllTableSplitters() {
        FakeConnectionProvider connection =
                fakeConnectionWithTables(
                        tableEntry(ORDERS, values(1L)), tableEntry(CUSTOMERS, values(1L)));
        DatabaseSplitterEnumerator enumerator = databaseSplitterFor("catalog", "schema");

        enumerator.start(connection);
        drainAllSplits(enumerator);

        assertThat(enumerator.lineageQueries()).hasSize(2);
    }

    @Test
    void testCloseClosesAllTableSplitterConnections() {
        FakeConnectionProvider connection =
                fakeConnectionWithTables(
                        tableEntry(ORDERS, values(1L)), tableEntry(CUSTOMERS, values(1L)));
        DatabaseSplitterEnumerator enumerator = databaseSplitterFor("catalog", "schema");

        enumerator.start(connection);
        drainAllSplits(enumerator);
        enumerator.close();

        // One closeConnection() call per table splitter (the parent's own connection is not
        // closed).
        assertThat(connection.closeCount).hasValue(2);
    }

    @Test
    void testCloseBeforeStartDoesNotThrow() {
        DatabaseSplitterEnumerator enumerator = databaseSplitterFor("catalog", "schema");

        assertThat(enumerator.isAllSplitsFinished()).isFalse();
        enumerator.close();
    }

    private static DatabaseSplitterEnumerator databaseSplitterFor(String catalog, String schema) {
        return DatabaseSplitterEnumerator.builder()
                .withCatalogName(catalog)
                .withSchemaName(schema)
                .build();
    }

    private static TableColumn idColumnPk() {
        return TableColumn.builder()
                .withColumnName("id")
                .withColumnType("int8")
                .withColumnPosition(1)
                .withColumnNullable(false)
                .withColumnPk(true)
                .build();
    }

    private static LinkedHashSet<Long> values(Long... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    private static Map.Entry<TableId, LinkedHashSet<Long>> tableEntry(
            TableId tableId, LinkedHashSet<Long> values) {
        return new java.util.AbstractMap.SimpleEntry<>(tableId, values);
    }

    @SafeVarargs
    private static FakeConnectionProvider fakeConnectionWithTables(
            Map.Entry<TableId, LinkedHashSet<Long>>... tableEntries) {
        Set<Table> tables = new HashSet<>();
        Map<TableId, Set<TableColumn>> columnsByTable = new HashMap<>();
        Map<TableId, LinkedHashSet<Long>> valuesByTable = new HashMap<>();
        for (Map.Entry<TableId, LinkedHashSet<Long>> entry : tableEntries) {
            tables.add(new Table(entry.getKey(), Collections.emptySet()));
            columnsByTable.put(entry.getKey(), Collections.singleton(idColumnPk()));
            valuesByTable.put(entry.getKey(), entry.getValue());
        }
        return new FakeConnectionProvider(tables, columnsByTable, valuesByTable);
    }

    private static List<JdbcSourceSplit> drainAllSplits(SplitterEnumerator enumerator) {
        List<JdbcSourceSplit> allSplits = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        do {
            allSplits.addAll(enumerator.enumerateSplits());
        } while (!enumerator.isAllSplitsFinished() && System.nanoTime() < deadline);
        return allSplits;
    }
}
