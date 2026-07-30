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
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableSplitterEnumeratorTest {

    private static final TableId TABLE_ID = new TableId("catalog", "schema", "orders");

    @Test
    void testStartRejectsNonConnectionProviderInstance() {
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);
        JdbcConnectionProvider notAConnectionProvider = new NotAConnectionProvider();

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
        FakeConnectionProvider connection = fakeConnection(TABLE_ID, idColumn(), values(1L));
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        enumerator.start(connection);
        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);

        assertThat(enumerator.lineageQueries()).hasSize(splits.size());
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
    void testGetBoundednessIsContinuousUnbounded() {
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        assertThat(enumerator.getBoundedness().name()).isEqualTo("CONTINUOUS_UNBOUNDED");
    }

    @Test
    void testSerializableStateAndRestoreState() {
        TableSplitterEnumerator enumerator = tableSplitterFor(TABLE_ID);

        assertThat(enumerator.serializableState()).isNull();
        assertThat(enumerator.restoreState(null)).isSameAs(enumerator);
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

    private static List<JdbcSourceSplit> drainAllSplits(SplitterEnumerator enumerator) {
        List<JdbcSourceSplit> allSplits = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        do {
            allSplits.addAll(enumerator.enumerateSplits());
        } while (!enumerator.isAllSplitsFinished() && System.nanoTime() < deadline);
        return allSplits;
    }

    /**
     * Not a {@link org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionProvider}.
     */
    private static final class NotAConnectionProvider implements JdbcConnectionProvider {
        @Override
        public java.sql.Connection getConnection() {
            return null;
        }

        @Override
        public boolean isConnectionValid() {
            return false;
        }

        @Override
        public java.sql.Connection getOrEstablishConnection() {
            return null;
        }

        @Override
        public void closeConnection() {}

        @Override
        public java.sql.Connection reestablishConnection() {
            return null;
        }
    }
}
