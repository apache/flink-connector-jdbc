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

import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionException;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.Table;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableBounds;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.SnapshotEnumeratorTestUtils.drainAllSplits;
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

        assertThatThrownBy(
                        () -> {
                            enumerator.start(connection);
                            enumerator.enumerateSplits();
                        })
                .isInstanceOf(IllegalStateException.class)
                .cause()
                .hasMessageContaining("No tables found");
    }

    @Test
    void testNoTablesInDatabaseThrows() {
        FakeConnectionProvider connection =
                new FakeConnectionProvider(
                        Collections.emptySet(), new HashMap<>(), new HashMap<>());
        DatabaseSplitterEnumerator enumerator = databaseSplitterFor("catalog", "schema");

        assertThatThrownBy(
                        () -> {
                            enumerator.start(connection);
                            enumerator.enumerateSplits();
                        })
                .isInstanceOf(IllegalStateException.class)
                .cause()
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

    @Test
    void testFinishedTablesSurviveLaterCheckpointsAndRestore() {
        // The bug this guards: a table splitter is closed and dropped from the live lists the
        // moment it finishes, so without persisting its terminal progress, any checkpoint taken
        // after that moment has no entry for the table — and a restore re-enumerates (and
        // duplicates) everything it had already emitted.
        FakeConnectionProvider connection =
                fakeConnectionWithTables(
                        tableEntry(ORDERS, values(1L)), tableEntry(CUSTOMERS, values(2L)));
        DatabaseSplitterEnumerator enumerator = databaseSplitterFor("catalog", "schema");

        enumerator.start(connection);
        assertThat(drainAllSplits(enumerator)).hasSize(2);

        DatabaseSplitProgress state = (DatabaseSplitProgress) enumerator.serializableState();
        assertThat(state.tableProgresses())
                .as("both finished tables must still be represented in later checkpoints")
                .hasSize(2)
                .allMatch(TableSplitProgress::finished);

        FakeConnectionProvider restoredConnection =
                fakeConnectionWithTables(
                        tableEntry(ORDERS, values(1L)), tableEntry(CUSTOMERS, values(2L)));
        DatabaseSplitterEnumerator restored = databaseSplitterFor("catalog", "schema");
        restored.restoreState(state);
        restored.start(restoredConnection);

        assertThat(drainAllSplits(restored))
                .as("finished tables must not be re-enumerated after restore")
                .isEmpty();
    }

    @Test
    void testCheckpointDuringRestoreWindowKeepsRestoredProgress() {
        // Between restoreState() and the background work applying the progress to freshly prepared
        // splitters, a checkpoint must re-persist the restored per-table progress instead of
        // capturing the (empty) not-yet-initialized live state.
        TableSplitProgress ordersProgress =
                new TableSplitProgress(ORDERS, true, false, TableBounds.of(1L, 5L), 1L, List.of());
        DatabaseSplitProgress state = new DatabaseSplitProgress(List.of(ordersProgress), List.of());

        DatabaseSplitterEnumerator restoring = databaseSplitterFor("catalog", "schema");
        restoring.restoreState(state);

        DatabaseSplitProgress rePersisted = (DatabaseSplitProgress) restoring.serializableState();
        assertThat(rePersisted.tableProgresses())
                .extracting(TableSplitProgress::tableId)
                .containsExactly(ORDERS);
    }

    @Test
    void testStartExportsGlobalSnapshotSynchronouslyBeforeAnySplit() throws Exception {
        // Restored splits are re-stamped with currentSnapshotId() at hand-off; that only protects
        // readers if the snapshot was already exported when start() returns.
        FakeConnectionProvider connection =
                fakeConnectionWithTables(tableEntry(ORDERS, values(1L, 2L)));
        DatabaseSplitterEnumerator enumerator = databaseSplitterFor("catalog", "schema");

        enumerator.start(connection);

        assertThat(connection.createGlobalSnapshotCount.get()).isEqualTo(1);
        assertThat(enumerator.currentSnapshotId()).isEqualTo("fake-exported-snapshot");
        enumerator.close();
    }

    @Test
    void testStartFailsFastWhenSnapshotExportFails() {
        FakeConnectionProvider connection =
                fakeConnectionWithTables(tableEntry(ORDERS, values(1L, 2L)));
        connection.failCreateGlobalSnapshot = true;
        DatabaseSplitterEnumerator enumerator = databaseSplitterFor("catalog", "schema");

        assertThatThrownBy(() -> enumerator.start(connection))
                .isInstanceOf(ConnectionException.class);
    }

    @Test
    void testNonPartitionedTableMetadataFetchedOncePerTable() throws InterruptedException {
        // The parent resolves each table's columns; non-partitioned children must reuse that result
        // instead of repeating the two metadata round trips per table (the preResolvedColumns
        // path).
        FakeConnectionProvider connection =
                fakeConnectionWithTables(tableEntry(ORDERS, values(1L, 2L)));
        DatabaseSplitterEnumerator enumerator = databaseSplitterFor("catalog", "schema");

        enumerator.start(connection);
        drainAllSplits(enumerator);

        assertThat(connection.tableColumnsCalls.get()).isEqualTo(1);
    }

    @Test
    void testRestoredUnfinishedTableResumesFromProgress() throws Exception {
        // A table that was mid-chunking at checkpoint time must resume from its cursor and pending
        // bounds — not restart enumeration from scratch, and not be dropped as finished. The
        // progress mirrors a producible capture: cursor == upper bound of the last pending chunk,
        // with chunkSize 2 over values 1..10 (bounds (1,3),(3,5),(5,7),(7,9),(9,null)).
        TableSplitProgress ordersProgress =
                new TableSplitProgress(
                        ORDERS,
                        true,
                        false,
                        TableBounds.of(1L, 10L),
                        9L,
                        List.of(TableBounds.of(5L, 7L), TableBounds.of(7L, 9L)));
        DatabaseSplitProgress state = new DatabaseSplitProgress(List.of(ordersProgress), List.of());

        // Customer table is single-row so it finishes immediately; orders resumes from the
        // restored pending bounds plus the trailing chunk it recomputes from its cursor.
        FakeConnectionProvider connection =
                fakeConnectionWithTables(
                        tableEntry(ORDERS, values(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)),
                        tableEntry(CUSTOMERS, values(1L)));
        DatabaseSplitterEnumerator restored =
                DatabaseSplitterEnumerator.builder()
                        .withCatalogName("catalog")
                        .withSchemaName("schema")
                        .withChunkSize(2)
                        .build();
        restored.restoreState(state);
        restored.start(connection);

        List<JdbcSourceSplit> splits = drainAllSplits(restored);
        List<JdbcSourceSplit> ordersSplits =
                splits.stream()
                        .filter(split -> split.getSqlTemplate().contains(ORDERS.toString()))
                        .collect(java.util.stream.Collectors.toList());
        assertThat(ordersSplits)
                .as("only the restored bounds and the post-cursor tail may be re-emitted")
                .hasSize(4);
        assertThat(ordersSplits.get(0).getParameters()).containsExactly(5L, 7L);
        assertThat(ordersSplits.get(1).getParameters()).containsExactly(7L, 9L);
        assertThat(ordersSplits.get(2).getParameters()).containsExactly(9L, 10L);
        assertThat(ordersSplits.get(3).getParameters()).containsExactly(10L);

        DatabaseSplitProgress finalState = (DatabaseSplitProgress) restored.serializableState();
        assertThat(finalState.tableProgresses())
                .as("both tables finished and their progress is checkpointable")
                .hasSize(2)
                .allMatch(TableSplitProgress::finished);
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
}
