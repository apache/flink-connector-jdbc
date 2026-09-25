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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableBounds;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.SnapshotEnumeratorTestUtils.drainAllSplits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableSplitterEnumeratorTest {

    private static final TableId TABLE_ID = new TableId("catalog", "schema", "orders");

    @Test
    void testStartRejectsNonConnectionProviderInstance() {
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);
        JdbcConnectionProvider notAConnectionProvider =
                new SnapshotEnumeratorTestUtils.NotAConnectionProvider();

        assertThatThrownBy(() -> enumerator.start(notAConnectionProvider))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ConnectionProvider");
    }

    @Test
    void testEmptyTableProducesSingleEmptySplit() throws InterruptedException {
        FakeConnectionProvider connection =
                fakeConnection(TABLE_ID, idColumn(), new LinkedHashSet<>());
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        enumerator.start(connection);
        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getParameters()).isNull();
        assertThat(enumerator.isAllSplitsFinished()).isTrue();
    }

    @Test
    void testSingleRowTableProducesSingleSplit() throws InterruptedException {
        FakeConnectionProvider connection = fakeConnection(TABLE_ID, idColumn(), values(5L));
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        enumerator.start(connection);
        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getParameters()).isNull();
    }

    @Test
    void testMultiChunkTableProducesBoundedAndUnboundedSplits() throws InterruptedException {
        LinkedHashSet<Long> pkValues = new LinkedHashSet<>();
        for (long i = 1; i <= 25; i++) {
            pkValues.add(i);
        }
        FakeConnectionProvider connection = fakeConnection(TABLE_ID, idColumn(), pkValues);
        TableSplitterEnumerator enumerator =
                TableSplitterEnumerator.builder()
                        .withCatalogName(TABLE_ID.catalogName())
                        .withSchemaName(TABLE_ID.schemaName())
                        .withTableName(TABLE_ID.tableName())
                        .withChunkSize(10)
                        .build();

        enumerator.start(connection);
        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);

        // Bounds: (null,1), (1,11), (11,21), (21,25), (25,null)
        assertThat(splits).hasSize(5);
        assertThat(splits.get(0).getParameters()).containsExactly(1L);
        assertThat(splits.get(1).getParameters()).containsExactly(1L, 11L);
        assertThat(splits.get(2).getParameters()).containsExactly(11L, 21L);
        assertThat(splits.get(3).getParameters()).containsExactly(21L, 25L);
        assertThat(splits.get(4).getParameters()).containsExactly(25L);
    }

    @Test
    void testMissingColumnNameThrows() throws InterruptedException {
        FakeConnectionProvider connection = fakeConnection(TABLE_ID, idColumn(), values(1L, 2L));
        TableSplitterEnumerator enumerator =
                TableSplitterEnumerator.builder()
                        .withCatalogName(TABLE_ID.catalogName())
                        .withSchemaName(TABLE_ID.schemaName())
                        .withTableName(TABLE_ID.tableName())
                        .withColumnNames("does_not_exist")
                        .build();

        enumerator.start(connection);

        assertThatThrownBy(() -> drainAllSplits(enumerator))
                .isInstanceOf(IllegalStateException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("does_not_exist");
    }

    @Test
    void testNoPrimaryKeyThrows() throws InterruptedException {
        TableColumn columnWithoutPk =
                TableColumn.builder()
                        .withColumnName("id")
                        .withColumnType("int8")
                        .withColumnPosition(1)
                        .withColumnNullable(false)
                        .withColumnPk(false)
                        .build();
        FakeConnectionProvider connection =
                fakeConnection(TABLE_ID, Collections.singleton(columnWithoutPk), values(1L));
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        enumerator.start(connection);

        assertThatThrownBy(() -> drainAllSplits(enumerator))
                .isInstanceOf(IllegalStateException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("does not have a primary key");
    }

    @Test
    void testCompositePrimaryKeyUsesFirstFieldByPosition() throws InterruptedException {
        TableColumn firstPk =
                TableColumn.builder()
                        .withColumnName("id")
                        .withColumnType("int8")
                        .withColumnPosition(1)
                        .withColumnNullable(false)
                        .withColumnPk(true)
                        .build();
        TableColumn secondPk =
                TableColumn.builder()
                        .withColumnName("id2")
                        .withColumnType("int8")
                        .withColumnPosition(2)
                        .withColumnNullable(false)
                        .withColumnPk(true)
                        .build();
        Set<TableColumn> columns = new LinkedHashSet<>(Arrays.asList(firstPk, secondPk));
        FakeConnectionProvider connection = fakeConnection(TABLE_ID, columns, values(1L, 2L, 3L));
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        enumerator.start(connection);
        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);

        // Doesn't throw, and chunks using the lower-positioned PK column ("id").
        assertThat(splits).isNotEmpty();
    }

    @Test
    void testLineageQueriesAggregatesGeneratedQueries() throws InterruptedException {
        FakeConnectionProvider connection =
                fakeConnection(TABLE_ID, idColumn(), values(1L, 2L, 3L));
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        enumerator.start(connection);
        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);

        // Multiple chunk splits, but only one representative lineage query per table.
        assertThat(splits).hasSizeGreaterThan(1);
        assertThat(enumerator.lineageQueries()).hasSize(1);
        assertThat(enumerator.lineageQueries().get(0)).contains(TABLE_ID.toString());
    }

    @Test
    void testCloseClosesConnection() {
        FakeConnectionProvider connection = fakeConnection(TABLE_ID, idColumn(), values(1L));
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        enumerator.start(connection);
        enumerator.close();

        assertThat(connection.closeCount).hasValue(1);
    }

    @Test
    void testCloseBeforeStartDoesNotThrow() {
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        assertThat(enumerator.isAllSplitsFinished()).isFalse();
        enumerator.close();
    }

    @Test
    void testBoundednessDefaultsToBoundedButIsConfigurable() {
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);
        assertThat(enumerator.getBoundedness()).isEqualTo(Boundedness.BOUNDED);

        TableSplitterEnumerator unbounded =
                TableSplitterEnumerator.builder()
                        .withCatalogName(TABLE_ID.catalogName())
                        .withSchemaName(TABLE_ID.schemaName())
                        .withTableName(TABLE_ID.tableName())
                        .withBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                        .build();
        assertThat(unbounded.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
    }

    @Test
    void testSerializableStateAndRestoreState() {
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        assertThat(enumerator.serializableState()).isNull();
        assertThat(enumerator.restoreState(null)).isSameAs(enumerator);
    }

    @Test
    void testRestoreFinishedProgressDoesNotReEmitSplits() throws InterruptedException {
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);
        enumerator.start(fakeConnection(TABLE_ID, idColumn(), values(1L, 2L, 3L)));

        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);
        assertThat(splits).hasSize(3);
        Serializable state = enumerator.serializableState();
        assertThat(state).isInstanceOf(TableSplitProgress.class);
        assertThat(((TableSplitProgress) state).finished()).isTrue();

        TableSplitterEnumerator restored = tableSplitterFor(TABLE_ID);
        restored.restoreState(state);
        restored.start(fakeConnection(TABLE_ID, idColumn(), values(1L, 2L, 3L)));

        assertThat(drainAllSplits(restored)).isEmpty();
    }

    @Test
    void testRestoreReEmitsPendingBoundsAfterFailover() throws Exception {
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);
        enumerator.start(fakeConnection(TABLE_ID, idColumn(), values(1L, 2L, 3L)));

        Serializable state = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            Serializable candidate = enumerator.serializableState();
            if (candidate instanceof TableSplitProgress
                    && ((TableSplitProgress) candidate).pendingBounds().size() == 3) {
                state = candidate;
                break;
            }
            Thread.sleep(20);
        }
        assertThat(state).as("background computation should have queued 3 bounds").isNotNull();

        // Survive a real checkpoint round trip through Java serialization.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(state);
        }
        Serializable deserialized;
        try (ObjectInputStream ois =
                new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            deserialized = (Serializable) ois.readObject();
        }

        TableSplitterEnumerator restored = tableSplitterFor(TABLE_ID);
        restored.restoreState(deserialized);
        restored.start(fakeConnection(TABLE_ID, idColumn(), values(1L, 2L, 3L)));

        assertThat(drainAllSplits(restored)).hasSize(3);
    }

    @Test
    void testSplitIdsAreHashedAndStableAcrossRuns() throws InterruptedException {
        // Split ids surface in the Web UI/REST/logs: they must embed the table identity but never
        // the raw primary-key boundary values, and must be deterministic across restarts (ids end
        // up in checkpointed split-state keyed by id).
        LinkedHashSet<Long> pkValues = new LinkedHashSet<>();
        for (long i = 1; i <= 25; i++) {
            pkValues.add(i);
        }
        TableSplitterEnumerator first = tableSplitterFor(TABLE_ID);
        first.start(fakeConnection(TABLE_ID, idColumn(), pkValues));
        List<JdbcSourceSplit> splits = drainAllSplits(first);
        assertThat(splits).hasSizeGreaterThan(1);
        List<String> ids =
                splits.stream().map(JdbcSourceSplit::splitId).collect(Collectors.toList());

        String expectedPrefix =
                TABLE_ID.catalogName()
                        + ":"
                        + TABLE_ID.schemaName()
                        + ":"
                        + TABLE_ID.tableName()
                        + ":";
        for (String id : ids) {
            assertThat(id).startsWith(expectedPrefix);
            String fingerprint = id.substring(expectedPrefix.length());
            assertThat(fingerprint).matches("[0-9a-f]{16}");
            assertThat(id).doesNotContain("TableBounds");
        }
        // Distinct bounds must produce distinct ids within the table.
        assertThat(ids).doesNotHaveDuplicates();

        // Determinism: an independent enumerator over the same data emits identical ids.
        TableSplitterEnumerator second = tableSplitterFor(TABLE_ID);
        second.start(fakeConnection(TABLE_ID, idColumn(), pkValues));
        List<String> secondIds =
                drainAllSplits(second).stream()
                        .map(JdbcSourceSplit::splitId)
                        .collect(Collectors.toList());
        assertThat(secondIds).isEqualTo(ids);
    }

    @Test
    void testRestoredBoundsAreNotConvertedBeforePrimaryKeyIsKnown() {
        // Restored pending bounds sit in the output queue before start() runs the background
        // validation that discovers the primary key. Converting early would dereference the null
        // primary key and NPE the coordinator; enumerateSplits() must report "transiently empty".
        TableSplitterEnumerator restored = tableSplitterFor(TABLE_ID);
        restored.restoreState(
                new TableSplitProgress(
                        TABLE_ID,
                        true,
                        false,
                        TableBounds.of(1L, 3L),
                        3L,
                        Collections.singletonList(TableBounds.of(1L, 2L))));

        assertThat(restored.enumerateSplits()).isEmpty();
    }

    private static TableSplitterEnumerator tableSplitterFor(TableId tableId) {
        return TableSplitterEnumerator.builder()
                .withCatalogName(tableId.catalogName())
                .withSchemaName(tableId.schemaName())
                .withTableName(tableId.tableName())
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

    private static Set<TableColumn> idColumn() {
        return Collections.singleton(idColumnPk());
    }

    private static LinkedHashSet<Long> values(Long... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    private static FakeConnectionProvider fakeConnection(
            TableId tableId, Set<TableColumn> columns, LinkedHashSet<Long> pkValues) {
        Map<TableId, Set<TableColumn>> columnsByTable = new HashMap<>();
        columnsByTable.put(tableId, columns);
        Map<TableId, LinkedHashSet<Long>> valuesByTable = new HashMap<>();
        valuesByTable.put(tableId, pkValues);
        return new FakeConnectionProvider(Collections.emptySet(), columnsByTable, valuesByTable);
    }
}
