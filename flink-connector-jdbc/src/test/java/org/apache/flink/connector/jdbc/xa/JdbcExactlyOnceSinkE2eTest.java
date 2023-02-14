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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_CANCELLATION_TIMEOUT;
import static org.apache.flink.connector.jdbc.xa.JdbcXaFacadeTestHelper.getInsertedIds;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** A simple end-to-end test for {@link JdbcXaSinkFunction}. */
public abstract class JdbcExactlyOnceSinkE2eTest implements DatabaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcExactlyOnceSinkE2eTest.class);

    private static final BooksTable OUTPUT_TABLE = new BooksTable("XaTable");
    protected static final int PARALLELISM = 4;
    protected static final long CHECKPOINT_TIMEOUT_MS = 5_000L;
    protected static final long TASK_CANCELLATION_TIMEOUT_MS = 10_000L;

    @RegisterExtension static final MiniClusterExtension MINI_CLUSTER = createCluster();

    private static MiniClusterExtension createCluster() {
        Configuration configuration = new Configuration();
        // single failover region to allow checkpointing even after some sources have finished and
        // restart all tasks if at least one fails
        configuration.set(EXECUTION_FAILOVER_STRATEGY, "full");
        // cancel tasks eagerly to reduce the risk of running out of memory with many restarts
        configuration.set(TASK_CANCELLATION_TIMEOUT, TASK_CANCELLATION_TIMEOUT_MS);
        configuration.set(CHECKPOINTING_TIMEOUT, Duration.ofMillis(CHECKPOINT_TIMEOUT_MS));

        return new MiniClusterExtension(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberTaskManagers(PARALLELISM)
                        .setConfiguration(configuration)
                        .build());
    }

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(OUTPUT_TABLE);
    }

    // track active sources for:
    // 1. if any cancels, cancel others ASAP
    // 2. wait for others (to participate in checkpointing)
    // not using SharedObjects because we want to explicitly control which tag (attempt) to use
    private static final Map<Integer, CountDownLatch> activeSources = new ConcurrentHashMap<>();
    // track inactive mappers - to start emission only when they are ready (to prevent them from
    // starving for memory)
    // not using SharedObjects because we want to explicitly control which tag (attempt) to use
    private static final Map<Integer, CountDownLatch> inactiveMappers = new ConcurrentHashMap<>();

    @AfterEach
    public void after() {
        activeSources.clear();
        inactiveMappers.clear();
    }

    @Test
    void testInsert() throws Exception {
        long started = System.currentTimeMillis();
        LOG.info("Test insert for {}", getMetadata().getVersion());
        int elementsPerSource = 50;
        int numElementsPerCheckpoint = 10;
        int expectedFailures = elementsPerSource / numElementsPerCheckpoint;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRestartStrategy(fixedDelayRestart(expectedFailures * 2, Time.milliseconds(100)));
        env.getConfig().setAutoWatermarkInterval(0L);
        env.enableCheckpointing(50, CheckpointingMode.EXACTLY_ONCE);
        // NOTE: keep operator chaining enabled to prevent memory exhaustion by sources while maps
        // are still initializing
        env.addSource(new TestEntrySource(elementsPerSource, numElementsPerCheckpoint))
                .setParallelism(PARALLELISM)
                .map(new FailingMapper(numElementsPerCheckpoint + (numElementsPerCheckpoint / 2)))
                .addSink(
                        JdbcSink.exactlyOnceSink(
                                OUTPUT_TABLE.getInsertIntoQuery(),
                                OUTPUT_TABLE.getStatementBuilder(),
                                JdbcExecutionOptions.builder().withMaxRetries(0).build(),
                                JdbcExactlyOnceOptions.builder()
                                        .withTransactionPerConnection(true)
                                        .build(),
                                getMetadata().getXaSourceSupplier()));

        env.execute();

        List<Integer> insertedIds = getInsertedIds(getMetadata(), OUTPUT_TABLE.getTableName());
        List<Integer> expectedIds =
                IntStream.range(0, elementsPerSource * PARALLELISM)
                        .boxed()
                        .collect(Collectors.toList());
        assertThat(insertedIds)
                .as(insertedIds.toString())
                .containsExactlyInAnyOrderElementsOf(expectedIds);

        LOG.info(
                "Test insert for {} finished in {} ms.",
                getMetadata().getVersion(),
                System.currentTimeMillis() - started);
    }

    /**
     * {@link SourceFunction} emits {@link BooksTable.BookEntry test entries} and waits for the
     * checkpoint.
     */
    private static class TestEntrySource extends RichParallelSourceFunction<BooksTable.BookEntry>
            implements CheckpointListener, CheckpointedFunction {
        private final int numElements;
        private final int numElementsPerCheckpoint;

        private transient volatile ListState<SourceRange> ranges;

        private volatile long lastCheckpointId = -1L;
        private volatile boolean lastSnapshotConfirmed = false;
        private volatile boolean running = true;

        private TestEntrySource(int numElements, int numElementsPerCheckpoint) {
            this.numElements = numElements;
            this.numElementsPerCheckpoint = numElementsPerCheckpoint;
        }

        @Override
        public void run(SourceContext<BooksTable.BookEntry> ctx) throws Exception {
            try {
                waitForConsumers();
                for (SourceRange range : ranges.get()) {
                    emitRange(range, ctx);
                }
            } finally {
                getActiveSources().countDown();
            }
            waitOtherSources(); // participate in checkpointing
        }

        private void waitForConsumers() throws InterruptedException {
            // even though the pipeline is (intended to be) chained, other parallel instances may
            // starve if this source uses all the available memory before they initialize
            sleep(() -> !inactiveMappers.containsKey(getRuntimeContext().getAttemptNumber()));
            inactiveMappers.get(getRuntimeContext().getAttemptNumber()).await();
        }

        private void emitRange(SourceRange range, SourceContext<BooksTable.BookEntry> ctx) {
            for (int i = range.from; i < range.to && running; ) {
                int count = Math.min(range.to - i, numElementsPerCheckpoint);
                emit(i, count, range, ctx);
                i += count;
            }
        }

        private void emit(
                int start,
                int count,
                SourceRange toAdvance,
                SourceContext<BooksTable.BookEntry> ctx) {
            synchronized (ctx.getCheckpointLock()) {
                lastCheckpointId = -1L;
                lastSnapshotConfirmed = false;
                for (int j = start; j < start + count && running; j++) {
                    try {
                        ctx.collect(
                                new BooksTable.BookEntry(
                                        j,
                                        Integer.toString(j),
                                        Integer.toString(j),
                                        (double) (j),
                                        j));
                    } catch (Exception e) {
                        if (!ExceptionUtils.findThrowable(e, TestException.class).isPresent()) {
                            LOG.warn("Exception during record emission", e);
                        }
                        throw e;
                    }
                    toAdvance.advance();
                }
            }
            sleep(() -> !lastSnapshotConfirmed);
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (lastCheckpointId > -1L && checkpointId >= this.lastCheckpointId) {
                lastSnapshotConfirmed = true;
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            activeSources.putIfAbsent(
                    getRuntimeContext().getAttemptNumber(),
                    new CountDownLatch(getRuntimeContext().getNumberOfParallelSubtasks()));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ranges =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("SourceState", SourceRange.class));
            if (!context.isRestored()) {
                ranges.update(
                        singletonList(
                                SourceRange.forSubtask(
                                        getRuntimeContext().getIndexOfThisSubtask(), numElements)));
            }
            LOG.debug("Source initialized with ranges: {}", ranges.get());
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            lastCheckpointId = context.getCheckpointId();
        }

        private void sleep(Supplier<Boolean> condition) {
            while (condition.get()
                    && running
                    && !Thread.currentThread().isInterrupted()
                    && haveActiveSources()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ExceptionUtils.rethrow(e);
                }
            }
        }

        private void waitOtherSources() throws InterruptedException {
            while (running && haveActiveSources()) {
                getActiveSources().await(100, TimeUnit.MILLISECONDS);
            }
        }

        private CountDownLatch getActiveSources() {
            return activeSources.get(getRuntimeContext().getAttemptNumber());
        }

        private boolean haveActiveSources() {
            return getActiveSources().getCount() > 0;
        }

        private static final class SourceRange {
            private int from;
            private final int to;

            private SourceRange(int from, int to) {
                this.from = from;
                this.to = to;
            }

            public static SourceRange forSubtask(int subtaskIndex, int elementCount) {
                return new SourceRange(
                        subtaskIndex * elementCount, (subtaskIndex + 1) * elementCount);
            }

            public void advance() {
                checkState(from < to);
                from++;
            }

            @Override
            public String toString() {
                return String.format("%d..%d", from, to);
            }
        }
    }

    private static class FailingMapper
            extends RichMapFunction<BooksTable.BookEntry, BooksTable.BookEntry> {
        private final int failingMessage;
        private transient AtomicInteger counter;

        public FailingMapper(int failingMessage) {
            this.failingMessage = failingMessage;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            inactiveMappers
                    .computeIfAbsent(
                            getRuntimeContext().getAttemptNumber(),
                            u ->
                                    new CountDownLatch(
                                            getRuntimeContext().getNumberOfParallelSubtasks()))
                    .countDown();
            counter = new AtomicInteger(failingMessage);
            LOG.debug("Mapper will fail after {} records.", failingMessage);
        }

        @Override
        public BooksTable.BookEntry map(BooksTable.BookEntry value) throws Exception {
            if (counter.getAndDecrement() <= 0) {
                LOG.debug("Mapper failing intentionally.");
                throw new TestException();
            }
            return value;
        }
    }

    private static final class TestException extends Exception {
        public TestException() {
            // use this string to prevent error parsing scripts from failing the build
            // and still have exception type
            super("java.lang.Exception: Artificial failure", null, true, false);
        }
    }
}
