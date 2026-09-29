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

package org.apache.flink.connector.jdbc.internal.executor;

import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.connector.jdbc.derby.database.dialect.DerbyDialect;
import org.apache.flink.connector.jdbc.statement.StatementFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link TableBufferReducedStatementExecutor}: which executor each {@link RowKind}
 * reaches, how the changes to one key are reduced, the order in which the two batches run, and what
 * a failed batch leaves behind for the retry in {@code JdbcOutputFormat#flush()}.
 */
class TableBufferReducedStatementExecutorTest {

    /**
     * Mirrors {@code JdbcOutputFormatBuilder#getPrimaryKey}: the key is a fresh {@link
     * GenericRowData} of the key fields, so it carries {@link RowKind#INSERT} whatever the record's
     * kind was. Reducing by key rests on that.
     */
    private static final Function<RowData, RowData> KEY_EXTRACTOR =
            row -> GenericRowData.of(row.getInt(0));

    private List<String> events;
    private RecordingExecutor upsert;
    private RecordingExecutor delete;
    private TableBufferReducedStatementExecutor executor;

    @BeforeEach
    void setUp() throws SQLException {
        events = new ArrayList<>();
        upsert = new RecordingExecutor("upsert", "UPSERT SQL", events);
        delete = new RecordingExecutor("delete", "DELETE SQL", events);
        executor = new TableBufferReducedStatementExecutor(upsert, delete, KEY_EXTRACTOR);
        executor.prepareStatements(null);
        events.clear();
    }

    @Test
    void insertAndUpdateAfterAreUpsertsDeleteAndUpdateBeforeAreDeletesByKey() throws SQLException {
        executor.addToBatch(row(RowKind.INSERT, 1, "a"));
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 2, "b"));
        executor.addToBatch(row(RowKind.DELETE, 3, "c"));
        executor.addToBatch(row(RowKind.UPDATE_BEFORE, 4, "d"));

        executor.executeBatch();

        assertThat(upsert.executed).hasSize(1);
        assertThat(upsert.executed.get(0))
                .containsExactlyInAnyOrder(
                        row(RowKind.INSERT, 1, "a"), row(RowKind.UPDATE_AFTER, 2, "b"));
        // deletes carry the extracted key, not the row, and the key's kind is always INSERT
        assertThat(delete.executed).hasSize(1);
        assertThat(delete.executed.get(0)).containsExactlyInAnyOrder(key(3), key(4));
        assertThat(delete.executed.get(0))
                .allSatisfy(
                        key -> {
                            assertThat(key.getArity()).isEqualTo(1);
                            assertThat(key.getRowKind()).isEqualTo(RowKind.INSERT);
                        });
    }

    @Test
    void lastChangePerKeyWinsWithinOneBatch() throws SQLException {
        executor.addToBatch(row(RowKind.INSERT, 1, "a"));
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 1, "b"));
        executor.addToBatch(row(RowKind.INSERT, 2, "a"));
        executor.addToBatch(row(RowKind.DELETE, 2, "a"));
        executor.addToBatch(row(RowKind.UPDATE_BEFORE, 3, "a"));
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 3, "b"));

        executor.executeBatch();

        assertThat(upsert.executed.get(0))
                .containsExactlyInAnyOrder(
                        row(RowKind.UPDATE_AFTER, 1, "b"), row(RowKind.UPDATE_AFTER, 3, "b"));
        assertThat(delete.executed.get(0)).containsExactly(key(2));
    }

    @Test
    void deleteThenInsertOfTheSameKeyCollapsesToTheInsert() throws SQLException {
        // the reduce key ignores the row kind, so the INSERT replaces the DELETE entry
        executor.addToBatch(row(RowKind.DELETE, 1, "a"));
        executor.addToBatch(row(RowKind.INSERT, 1, "c"));

        executor.executeBatch();

        assertThat(upsert.executed.get(0)).containsExactly(row(RowKind.INSERT, 1, "c"));
        assertThat(delete.executed.get(0)).isEmpty();
    }

    @Test
    void allUpsertsExecuteBeforeAllDeletes() throws SQLException {
        executor.addToBatch(row(RowKind.DELETE, 1, "a"));
        executor.addToBatch(row(RowKind.INSERT, 2, "b"));
        executor.addToBatch(row(RowKind.DELETE, 3, "c"));

        executor.executeBatch();

        int upsertExecute = events.indexOf("upsert.execute");
        int deleteExecute = events.indexOf("delete.execute");
        assertThat(upsertExecute).isPositive();
        assertThat(deleteExecute).isGreaterThan(upsertExecute);
        assertThat(events.subList(0, upsertExecute)).allMatch(event -> event.endsWith(".add"));
    }

    @Test
    void emptyBufferExecutesNothingAndTheBufferIsClearedAfterExecuting() throws SQLException {
        executor.executeBatch();
        assertThat(events).isEmpty();

        executor.addToBatch(row(RowKind.INSERT, 1, "a"));
        executor.executeBatch();
        assertThat(upsert.executed).hasSize(1);

        events.clear();
        executor.executeBatch();
        assertThat(events).isEmpty();
        assertThat(upsert.executed).hasSize(1);
    }

    @Test
    void aFailedDeleteBatchKeepsTheBufferAndReplaysItAfterRepreparing() throws SQLException {
        executor.addToBatch(row(RowKind.INSERT, 1, "a"));
        executor.addToBatch(row(RowKind.DELETE, 2, "b"));
        delete.failNextExecute(new SQLException("delete failed"));

        assertThatThrownBy(executor::executeBatch)
                .isInstanceOf(SQLException.class)
                .hasMessage("delete failed");

        // the upsert batch had already run when the delete failed
        assertThat(upsert.executed).containsExactly(list(row(RowKind.INSERT, 1, "a")));
        assertThat(delete.executed).isEmpty();

        // what JdbcOutputFormat#updateExecutor does before the retry
        executor.closeStatements();
        executor.prepareStatements(null);
        executor.executeBatch();

        // the upsert is replayed, so it has to be idempotent; the delete runs exactly once
        assertThat(upsert.executed)
                .containsExactly(
                        list(row(RowKind.INSERT, 1, "a")), list(row(RowKind.INSERT, 1, "a")));
        assertThat(delete.executed).containsExactly(list(key(2)));

        events.clear();
        executor.executeBatch();
        assertThat(events).isEmpty();
    }

    @Test
    void aFailedUpsertBatchLeavesTheDeletesForTheReplay() throws SQLException {
        executor.addToBatch(row(RowKind.INSERT, 1, "a"));
        executor.addToBatch(row(RowKind.DELETE, 2, "b"));
        upsert.failNextExecute(new SQLException("upsert failed"));

        assertThatThrownBy(executor::executeBatch).isInstanceOf(SQLException.class);
        assertThat(upsert.executed).isEmpty();
        assertThat(delete.executed).isEmpty();

        executor.closeStatements();
        executor.prepareStatements(null);
        executor.executeBatch();

        assertThat(upsert.executed).containsExactly(list(row(RowKind.INSERT, 1, "a")));
        // closeStatements discarded the half-built delete batch, so the key is not sent twice
        assertThat(delete.executed).containsExactly(list(key(2)));
    }

    @Test
    void prepareAndCloseReachBothExecutors() throws SQLException {
        assertThat(upsert.prepareCalls).isEqualTo(1);
        assertThat(delete.prepareCalls).isEqualTo(1);

        executor.closeStatements();

        assertThat(upsert.closeCalls).isEqualTo(1);
        assertThat(delete.closeCalls).isEqualTo(1);
    }

    @Test
    void insertSqlIsTheUpsertExecutorsSql() {
        assertThat(executor.insertSql()).isEqualTo("UPSERT SQL");
    }

    /**
     * The real executors read their SQL off the prepared statement, so {@code insertSql()} has
     * nothing to return before {@code prepareStatements}. Pinned so that a change to this contract
     * is a deliberate one.
     */
    @Test
    void realExecutorsHaveNoInsertSqlBeforePrepareStatements() {
        JdbcDialectConverter converter =
                new DerbyDialect().getRowConverter(RowType.of(new IntType(), new VarCharType()));
        StatementFactory neverPrepared =
                connection -> {
                    throw new AssertionError("prepareStatements was not expected");
                };

        TableSimpleStatementExecutor simple =
                new TableSimpleStatementExecutor(neverPrepared, converter);
        assertThatThrownBy(simple::insertSql).isInstanceOf(NullPointerException.class);

        TableInsertOrUpdateStatementExecutor insertOrUpdate =
                new TableInsertOrUpdateStatementExecutor(
                        neverPrepared,
                        neverPrepared,
                        neverPrepared,
                        converter,
                        converter,
                        converter,
                        KEY_EXTRACTOR);
        assertThatThrownBy(insertOrUpdate::insertSql).isInstanceOf(NullPointerException.class);
    }

    private static RowData row(RowKind kind, int id, String value) {
        return GenericRowData.ofKind(kind, id, StringData.fromString(value));
    }

    private static RowData key(int id) {
        return GenericRowData.of(id);
    }

    private static List<RowData> list(RowData... rows) {
        return Arrays.asList(rows);
    }

    /**
     * A {@link JdbcBatchStatementExecutor} that records what it is asked to do. Like the real
     * executors it has no statement until {@code prepareStatements} has run, and like a driver in
     * the worst case it keeps a failed batch until {@code closeStatements} discards it.
     */
    private static final class RecordingExecutor implements JdbcBatchStatementExecutor<RowData> {

        private final String name;
        private final String sql;
        private final List<String> events;
        private final List<RowData> batch = new ArrayList<>();
        private final Deque<SQLException> failures = new ArrayDeque<>();
        private boolean prepared;

        final List<List<RowData>> executed = new ArrayList<>();
        int prepareCalls;
        int closeCalls;

        RecordingExecutor(String name, String sql, List<String> events) {
            this.name = name;
            this.sql = sql;
            this.events = events;
        }

        void failNextExecute(SQLException failure) {
            failures.add(failure);
        }

        @Override
        public void prepareStatements(Connection connection) {
            prepared = true;
            prepareCalls++;
            events.add(name + ".prepare");
        }

        @Override
        public void addToBatch(RowData record) {
            requirePrepared();
            batch.add(record);
            events.add(name + ".add");
        }

        @Override
        public void executeBatch() throws SQLException {
            requirePrepared();
            SQLException failure = failures.poll();
            if (failure != null) {
                events.add(name + ".fail");
                throw failure;
            }
            executed.add(new ArrayList<>(batch));
            batch.clear();
            events.add(name + ".execute");
        }

        @Override
        public void closeStatements() {
            batch.clear();
            prepared = false;
            closeCalls++;
            events.add(name + ".close");
        }

        @Override
        public String insertSql() {
            requirePrepared();
            return sql;
        }

        private void requirePrepared() {
            if (!prepared) {
                throw new NullPointerException(name + " has no prepared statement");
            }
        }
    }
}
