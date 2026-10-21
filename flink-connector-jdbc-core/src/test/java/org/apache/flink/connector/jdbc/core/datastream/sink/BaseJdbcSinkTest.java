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

package org.apache.flink.connector.jdbc.core.datastream.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.jdbc.derby.DerbyTestBase;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke tests for the {@link org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink} and the
 * underlying classes.
 */
public abstract class BaseJdbcSinkTest implements DerbyTestBase, JdbcITCaseBase {

    private static final BooksTable TEST_TABLE = new BooksTable("SinkTable");

    /**
     * The books table without its key: at-least-once can write the batch of a failed attempt on
     * close and write it again after the restart.
     */
    private static final TableRow RECOVERY_TABLE =
            tableRow(
                    "RecoverySinkTable",
                    field("id", INT().notNull()),
                    field("title", VARCHAR(50)),
                    field("author", VARCHAR(50)),
                    field("price", dbType("FLOAT"), DOUBLE()),
                    field("qty", INT()));

    private static final List<BooksTable.BookEntry> BOOKS =
            Arrays.stream(TEST_DATA)
                    .map(
                            book ->
                                    new BooksTable.BookEntry(
                                            book.id, book.title, book.author, book.price, book.qty))
                    .collect(Collectors.toList());

    @Override
    public List<TableManaged> getManagedTables() {
        return Arrays.asList(TEST_TABLE, RECOVERY_TABLE);
    }

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    protected abstract <T> JdbcSink<T> finishSink(JdbcSinkBuilder<T> builder);

    @Test
    public void testInsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);

        assertResult(new ArrayList<>());

        env.fromCollection(BOOKS)
                .sinkTo(
                        finishSink(
                                new JdbcSinkBuilder<BooksTable.BookEntry>()
                                        .withQueryStatement(
                                                TEST_TABLE.getInsertIntoQuery(),
                                                TEST_TABLE.getStatementBuilder())));
        env.execute();

        assertResult(BOOKS);
    }

    @Test
    public void testInsertWithObjectReuse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);

        assertResult(new ArrayList<>());

        BookEntryReuse reused = new BookEntryReuse();
        env.fromCollection(BOOKS)
                .map(
                        book -> {
                            reused.setBook(book);
                            return reused;
                        })
                .sinkTo(
                        finishSink(
                                new JdbcSinkBuilder<BookEntryReuse>()
                                        .withQueryStatement(
                                                TEST_TABLE.getInsertIntoQuery(),
                                                (ps, t) ->
                                                        TEST_TABLE
                                                                .getStatementBuilder()
                                                                .accept(ps, t.getBook()))));
        env.execute();

        assertResult(BOOKS);
    }

    @Test
    public void testGetLineageVertex() {
        JdbcSink<?> sink =
                finishSink(
                        new JdbcSinkBuilder<BooksTable.BookEntry>()
                                .withQueryStatement(
                                        TEST_TABLE.getInsertIntoQuery(),
                                        TEST_TABLE.getStatementBuilder()));

        LineageVertex lineageVertex = sink.getLineageVertex();
        assertThat(lineageVertex.datasets().size()).isEqualTo(1);
    }

    /**
     * The restart after a failure restores the writer with the state of the last completed
     * checkpoint, and the final checkpoint covers every record. Every record of a completed
     * checkpoint reaches the database.
     */
    @Test
    void testRecovery() throws Exception {
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        SharedReference<AtomicBoolean> failed = sharedObjects.add(new AtomicBoolean(false));
        SharedReference<Set<Integer>> checkpointed =
                sharedObjects.add(new ConcurrentSkipListSet<>());
        DataGeneratorSource<BooksTable.BookEntry> books =
                new DataGeneratorSource<>(
                        BaseJdbcSinkTest::book,
                        RECOVERY_RECORDS,
                        RateLimiterStrategy.perCheckpoint(10),
                        TypeInformation.of(BooksTable.BookEntry.class));

        env.fromSource(books, WatermarkStrategy.noWatermarks(), "books")
                .map(new FailingCheckpointMapper(failed))
                .map(new RecordFetcher(checkpointed))
                .sinkTo(
                        finishSink(
                                new JdbcSinkBuilder<BooksTable.BookEntry>()
                                        .withQueryStatement(
                                                RECOVERY_TABLE.getInsertIntoQuery(),
                                                TEST_TABLE.getStatementBuilder())));
        env.execute();

        assertThat(failed.get()).isTrue();
        assertThat(checkpointed.get()).hasSize((int) RECOVERY_RECORDS);
        List<Integer> written =
                RECOVERY_TABLE.selectAllTable(getMetadata().getConnection()).stream()
                        .map(row -> (Integer) row.getField(0))
                        .collect(Collectors.toList());
        // exactly-once can assert the exact set once FLINK-41060 is fixed: the writer commits at
        // the barrier, so a failure after a barrier restarts from the checkpoint before it and
        // replays rows the writer already committed
        assertThat(written).containsAll(checkpointed.get());
    }

    private static final long RECOVERY_RECORDS = 100;

    private static BooksTable.BookEntry book(Long id) {
        return new BooksTable.BookEntry(id.intValue(), "Book " + id, "Writer", id.doubleValue(), 1);
    }

    /** Fails once, after a checkpoint completed and the next record arrived. */
    private static class FailingCheckpointMapper
            implements MapFunction<BooksTable.BookEntry, BooksTable.BookEntry>, CheckpointListener {

        private final SharedReference<AtomicBoolean> failed;
        private long lastCheckpointId = 0;
        private int emittedBetweenCheckpoints = 0;

        FailingCheckpointMapper(SharedReference<AtomicBoolean> failed) {
            this.failed = failed;
        }

        @Override
        public BooksTable.BookEntry map(BooksTable.BookEntry book) {
            if (lastCheckpointId >= 1 && emittedBetweenCheckpoints > 0 && !failed.get().get()) {
                failed.get().set(true);
                throw new RuntimeException("Planned exception.");
            }
            emittedBetweenCheckpoints++;
            return book;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            lastCheckpointId = checkpointId;
            emittedBetweenCheckpoints = 0;
        }
    }

    /** Keeps the ids of the records every completed checkpoint covers. */
    private static class RecordFetcher
            implements MapFunction<BooksTable.BookEntry, BooksTable.BookEntry>,
                    CheckpointedFunction,
                    CheckpointListener {

        private static final ListStateDescriptor<Integer> STATE =
                new ListStateDescriptor<>("checkpointed-ids", BasicTypeInfo.INT_TYPE_INFO);

        private final SharedReference<Set<Integer>> checkpointed;
        private final List<Integer> sinceLastCheckpoint = new ArrayList<>();
        private ListState<Integer> snapshotted;

        RecordFetcher(SharedReference<Set<Integer>> checkpointed) {
            this.checkpointed = checkpointed;
        }

        @Override
        public BooksTable.BookEntry map(BooksTable.BookEntry book) {
            sinceLastCheckpoint.add(book.id);
            return book;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            snapshotted.addAll(sinceLastCheckpoint);
            sinceLastCheckpoint.clear();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            List<Integer> ids = new ArrayList<>();
            snapshotted.get().forEach(ids::add);
            checkpointed.get().addAll(ids);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            snapshotted = context.getOperatorStateStore().getListState(STATE);
        }
    }

    private void assertResult(List<BooksTable.BookEntry> expected) throws SQLException {
        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(expected);
    }

    /** */
    public static class BookEntryReuse implements Serializable {
        public BooksTable.BookEntry book;

        public void setBook(BooksTable.BookEntry book) {
            this.book = book;
        }

        public BooksTable.BookEntry getBook() {
            return book;
        }
    }
}
