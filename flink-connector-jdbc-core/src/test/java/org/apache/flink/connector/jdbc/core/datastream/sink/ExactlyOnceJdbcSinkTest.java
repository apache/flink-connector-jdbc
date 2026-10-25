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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.xa.SimpleXaConnectionProvider;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.transaction.xa.Xid;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/** Smoke tests for the {@link JdbcSink} and the underlying classes. */
class ExactlyOnceJdbcSinkTest extends BaseJdbcSinkTest {

    /** The books table without its key, so that a record written twice shows up twice. */
    private static final TableRow RECOVERY_TABLE =
            tableRow(
                    "RecoverySinkTable",
                    field("id", INT().notNull()),
                    field("title", VARCHAR(50)),
                    field("author", VARCHAR(50)),
                    field("price", dbType("FLOAT"), DOUBLE()),
                    field("qty", INT()));

    protected static final long RECOVERY_RECORDS = 100;

    private static final long RESCALE_RECORDS = 1_000;

    private static final BooksTable UPDATE_TABLE = new BooksTable("UpdateSinkTable");

    private static final int UPDATES = 10_000;

    @RegisterExtension
    protected final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    @Override
    protected <T> JdbcSink<T> finishSink(JdbcSinkBuilder<T> builder) {
        return builder.withExecutionOptions(
                        JdbcExecutionOptions.builder().withMaxRetries(0).build())
                .buildExactlyOnce(
                        JdbcExactlyOnceOptions.defaults(), getMetadata().getXaSourceSupplier());
    }

    @Override
    public List<TableManaged> getManagedTables() {
        List<TableManaged> tables = new ArrayList<>(super.getManagedTables());
        tables.add(RECOVERY_TABLE);
        tables.add(UPDATE_TABLE);
        return tables;
    }

    /** A failed test can leave a prepared transaction behind, which holds its locks. */
    @AfterEach
    void rollbackPreparedTransactions() throws SQLException {
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            for (Xid xid : xa.recover()) {
                xa.rollback(xid);
            }
        }
    }

    /**
     * An operator ahead of the sink fails the second checkpoint after the sink has prepared its
     * transaction for it. The restart from the first checkpoint replays the records of that
     * transaction, so it must not have been committed.
     */
    @Test
    void testRecoveryAfterAFailedCheckpoint() throws Exception {
        testRecoveryAfterFailedCheckpoints(2, 1, false);
    }

    /**
     * The first checkpoint fails after the sink has prepared its first transaction. The restart has
     * no state and starts the same xid again, so it must have been rolled back.
     */
    @Test
    void testRecoveryAfterAFailedFirstCheckpoint() throws Exception {
        testRecoveryAfterFailedCheckpoints(1, 1, false);
    }

    /**
     * The second checkpoint expires, which is tolerated, and the third one expires too, which fails
     * the job. The sink has prepared a transaction for each of them, and the restart from the first
     * checkpoint must roll back both.
     */
    @Test
    void testRecoveryAfterATolerableFailedCheckpoint() throws Exception {
        testRecoveryAfterFailedCheckpoints(2, 2, true);
    }

    private void testRecoveryAfterFailedCheckpoints(
            long firstFailingCheckpointId, int failingCheckpoints, boolean expire)
            throws Exception {
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        if (expire) {
            env.getCheckpointConfig().setCheckpointTimeout(300L);
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(failingCheckpoints - 1);
        }
        SharedReference<AtomicInteger> failed = sharedObjects.add(new AtomicInteger());
        DataGeneratorSource<BooksTable.BookEntry> books =
                new DataGeneratorSource<>(
                        ExactlyOnceJdbcSinkTest::book,
                        RECOVERY_RECORDS,
                        // records keep coming while checkpoints expire
                        expire
                                ? RateLimiterStrategy.perSecond(40)
                                : RateLimiterStrategy.perCheckpoint(10),
                        TypeInformation.of(BooksTable.BookEntry.class));

        env.fromSource(books, WatermarkStrategy.noWatermarks(), "books")
                .map(
                        new FailingSnapshotMapper(
                                failed, firstFailingCheckpointId, failingCheckpoints, expire))
                .sinkTo(recoverySink());
        env.execute();

        assertThat(failed.get().get()).isEqualTo(failingCheckpoints);
        assertWrittenOnce(RECOVERY_RECORDS);
    }

    /**
     * A restore at a lower parallelism hands a writer the states of several writers. The source
     * goes on from the savepoint, so every record is written once.
     */
    @Test
    void testRestoreAtLowerParallelism(@TempDir Path savepoints) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(2);
        env.enableCheckpointing(100L);
        addRescalePipeline(env, RateLimiterStrategy.perCheckpoint(20));
        String savepoint = stopWithSavepoint(env.executeAsync(), savepoints);

        Configuration config = new Configuration();
        config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);
        env.enableCheckpointing(100L);
        addRescalePipeline(env, RateLimiterStrategy.perCheckpoint(200));
        env.execute();

        assertWrittenOnce(RESCALE_RECORDS);
    }

    private void addRescalePipeline(
            StreamExecutionEnvironment env, RateLimiterStrategy rateLimiterStrategy) {
        DataGeneratorSource<BooksTable.BookEntry> books =
                new DataGeneratorSource<>(
                        ExactlyOnceJdbcSinkTest::book,
                        RESCALE_RECORDS,
                        rateLimiterStrategy,
                        TypeInformation.of(BooksTable.BookEntry.class));
        env.fromSource(books, WatermarkStrategy.noWatermarks(), "books")
                .uid("books")
                .sinkTo(recoverySink())
                .uid("sink");
    }

    /** Retries until all tasks run, as a savepoint can't be taken before. */
    private static String stopWithSavepoint(JobClient client, Path savepoints) throws Exception {
        Instant deadline = Instant.now().plusSeconds(60);
        while (true) {
            try {
                return client.stopWithSavepoint(
                                false, savepoints.toUri().toString(), SavepointFormatType.CANONICAL)
                        .get();
            } catch (ExecutionException e) {
                if (Instant.now().isAfter(deadline)) {
                    throw e;
                }
                Thread.sleep(100L);
            }
        }
    }

    protected JdbcSink<BooksTable.BookEntry> recoverySink() {
        return finishSink(
                new JdbcSinkBuilder<BooksTable.BookEntry>()
                        .withQueryStatement(
                                RECOVERY_TABLE.getInsertIntoQuery(),
                                (ps, book) -> {
                                    ps.setInt(1, book.id);
                                    ps.setString(2, book.title);
                                    ps.setString(3, book.author);
                                    ps.setDouble(4, book.price);
                                    ps.setInt(5, book.qty);
                                }));
    }

    /** Checks for prepared transactions first, as a select would wait for their locks. */
    protected void assertWrittenOnce(long records) throws SQLException {
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            assertThat(xa.recover()).isEmpty();
        }
        List<Integer> written =
                RECOVERY_TABLE.selectAllTable(getMetadata()).stream()
                        .map(row -> (Integer) row.getField(0))
                        .collect(Collectors.toList());
        assertThat(written)
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, (int) records).boxed().collect(Collectors.toList()));
    }

    /**
     * Every record updates the same row, so after each checkpoint the writer needs the lock that
     * the transaction prepared for that checkpoint holds until the committer commits it.
     */
    @Test
    void testUpdateRowOfThePreviousCheckpoint() throws Exception {
        BooksTable.BookEntry book = new BooksTable.BookEntry(0, "Book 0", "Writer", 0D, 0);
        try (Connection connection = getMetadata().getConnection();
                PreparedStatement ps =
                        connection.prepareStatement(UPDATE_TABLE.getInsertIntoQuery())) {
            UPDATE_TABLE.getStatementBuilder().accept(ps, book);
            ps.execute();
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);
        env.enableCheckpointing(50L);

        env.fromSequence(1, UPDATES)
                .sinkTo(
                        new JdbcSinkBuilder<Long>()
                                .withQueryStatement(
                                        "UPDATE UpdateSinkTable SET qty = ? WHERE id = 0",
                                        (ps, qty) -> ps.setInt(1, qty.intValue()))
                                .withExecutionOptions(
                                        JdbcExecutionOptions.builder()
                                                .withBatchSize(1)
                                                .withMaxRetries(0)
                                                .build())
                                .buildExactlyOnce(
                                        JdbcExactlyOnceOptions.defaults(),
                                        getMetadata().getXaSourceSupplier()));
        env.execute();

        try (Connection connection = getMetadata().getConnection()) {
            assertThat(UPDATE_TABLE.selectAllTable(connection))
                    .containsExactly(
                            new BooksTable.BookEntry(
                                    book.id, book.title, book.author, book.price, UPDATES));
        }
    }

    protected static BooksTable.BookEntry book(Long id) {
        return new BooksTable.BookEntry(id.intValue(), "Book " + id, "Writer", id.doubleValue(), 1);
    }

    /**
     * Fails the snapshots of the given number of checkpoints, or lets them expire. A chain takes
     * its snapshots from the tail to the head, so the sink has already prepared its transaction for
     * those checkpoints.
     */
    protected static class FailingSnapshotMapper
            implements MapFunction<BooksTable.BookEntry, BooksTable.BookEntry>,
                    CheckpointedFunction {

        private final SharedReference<AtomicInteger> failed;
        private final long firstFailingCheckpointId;
        private final int failingCheckpoints;
        private final boolean expire;
        private boolean restored;

        FailingSnapshotMapper(
                SharedReference<AtomicInteger> failed,
                long firstFailingCheckpointId,
                int failingCheckpoints,
                boolean expire) {
            this.failed = failed;
            this.firstFailingCheckpointId = firstFailingCheckpointId;
            this.failingCheckpoints = failingCheckpoints;
            this.expire = expire;
        }

        @Override
        public BooksTable.BookEntry map(BooksTable.BookEntry book) {
            return book;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (!restored
                    && context.getCheckpointId() >= firstFailingCheckpointId
                    && failed.get().get() < failingCheckpoints) {
                failed.get().incrementAndGet();
                if (expire) {
                    // longer than the checkpoint timeout, short enough for the next barrier to
                    // reach the sink before the job fails
                    Thread.sleep(400L);
                } else {
                    throw new RuntimeException("Planned exception.");
                }
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            restored = context.isRestored();
        }
    }
}
