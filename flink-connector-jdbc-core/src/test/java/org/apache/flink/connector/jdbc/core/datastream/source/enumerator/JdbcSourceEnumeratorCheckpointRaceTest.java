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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.jdbc.core.datastream.source.config.ContinuousUnBoundingSettings;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.JdbcSqlSplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.SplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.split.CheckpointedOffset;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.split.JdbcSlideTimingParameterProvider;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression test for FLINK-40779. */
class JdbcSourceEnumeratorCheckpointRaceTest {

    /**
     * Reproduces the window that FLINK-40779 describes.
     *
     * <p>{@code JdbcSourceEnumerator} enumerates splits on a worker thread and handles the result
     * on the coordinator thread. A checkpoint taken in between used to record the splitter state
     * that had already advanced past the splits still in flight, so those splits were lost on
     * restore.
     */
    @Test
    void testCheckpointBetweenEnumerationAndHandlingDoesNotLoseSplits() throws Exception {
        final ControllableAsyncContext context = new ControllableAsyncContext(1);
        final CountingSplitterEnumerator splitter = new CountingSplitterEnumerator();
        final JdbcSourceEnumerator enumerator =
                new JdbcSourceEnumerator(context, splitter, null, new ArrayList<>());

        // Schedules the first enumeration, which has not run yet.
        enumerator.start();

        // The enumeration runs "on the worker thread": the splitter advances to state 1 and
        // produces split-000, but the handler that would buffer split-000 has not run yet.
        context.runNextCallable();
        assertThat(splitter.serializableState()).isEqualTo(1);

        // A checkpoint taken in this window must not claim the advanced state, because split-000 is
        // not part of it yet.
        final JdbcSourceEnumeratorState checkpoint = enumerator.snapshotState(1L);
        assertThat(checkpoint.getRemainingSplits()).isEmpty();
        assertThat(checkpoint.getOptionalUserDefinedSplitEnumeratorState()).isEqualTo(0);

        // Restoring from that checkpoint must produce split-000 again rather than dropping it.
        final ControllableAsyncContext restoredContext = new ControllableAsyncContext(1);
        final JdbcSourceEnumerator restored =
                new JdbcSourceEnumerator(
                        restoredContext,
                        splitter.restoreState(
                                checkpoint.getOptionalUserDefinedSplitEnumeratorState()),
                        null,
                        new ArrayList<>(checkpoint.getRemainingSplits()));
        restored.start();
        restoredContext.runNextCallable();
        restoredContext.runNextHandler();

        assertThat(restored.snapshotState(2L).getRemainingSplits())
                .extracting(JdbcSourceSplit::splitId)
                .containsExactly("split-000");
    }

    @Test
    void testStateIsCommittedOnceSplitsHaveBeenHandled() throws Exception {
        final ControllableAsyncContext context = new ControllableAsyncContext(1);
        final CountingSplitterEnumerator splitter = new CountingSplitterEnumerator();
        final JdbcSourceEnumerator enumerator =
                new JdbcSourceEnumerator(context, splitter, null, new ArrayList<>());

        enumerator.start();
        context.runNextCallable();
        context.runNextHandler();

        // split-000 is now owned by the enumerator, so the state that produced it is checkpointed
        // together with it.
        final JdbcSourceEnumeratorState checkpoint = enumerator.snapshotState(1L);
        assertThat(checkpoint.getRemainingSplits())
                .extracting(JdbcSourceSplit::splitId)
                .containsExactly("split-000");
        assertThat(checkpoint.getOptionalUserDefinedSplitEnumeratorState()).isEqualTo(1);

        // Let the next enumeration run on the worker thread: the splitter advances to 2, but its
        // handler has not run yet. That state must not leak into the checkpoint, otherwise the
        // split it produces would be skipped after a restore. This fails on the code without the
        // fix, where snapshotState() reads the live splitter state.
        context.runNextCallable();
        assertThat(splitter.serializableState()).isEqualTo(2);

        final JdbcSourceEnumeratorState inFlight = enumerator.snapshotState(2L);
        assertThat(inFlight.getRemainingSplits())
                .extracting(JdbcSourceSplit::splitId)
                .containsExactly("split-000");
        assertThat(inFlight.getOptionalUserDefinedSplitEnumeratorState()).isEqualTo(1);
    }

    @Test
    void testSetSqlProviderSeedsStateFromProviderWhenNoStateIsConfigured() {
        final ControllableSlideTimingProvider provider =
                new ControllableSlideTimingProvider(1000L, 500L, 1L, 0L, 1500L);
        final SqlTemplateSplitEnumerator.TemplateSqlSplitEnumeratorProvider providerBuilder =
                new SqlTemplateSplitEnumerator.TemplateSqlSplitEnumeratorProvider()
                        .setSqlTemplate("SELECT * FROM t WHERE ts >= ? AND ts < ?")
                        .setParameterValuesProvider(provider);

        assertThat(providerBuilder.create().enumeratorState()).isEqualTo(1000L);
    }

    /**
     * The first batch of a setSql source with a stateful provider must survive a restore. The
     * provider is mutated in place while enumerating, so a checkpoint taken between the enumeration
     * and its handler used to store a null state that a restore could not roll back.
     */
    @Test
    void testFirstBatchOfSetSqlSourceIsReEnumeratedAfterRestore() throws Exception {
        // now = 1500 produces exactly one window [1000, 1500) and advances the provider to 1001.
        final ControllableSlideTimingProvider provider =
                new ControllableSlideTimingProvider(1000L, 500L, 1L, 0L, 1500L);
        final JdbcSqlSplitterEnumerator splitter =
                new JdbcSqlSplitterEnumerator(
                        new SqlTemplateSplitEnumerator.TemplateSqlSplitEnumeratorProvider()
                                .setSqlTemplate("SELECT * FROM t WHERE ts >= ? AND ts < ?")
                                .setParameterValuesProvider(provider),
                        new ContinuousUnBoundingSettings(null, Duration.ZERO));

        final ControllableAsyncContext context = new ControllableAsyncContext(1);
        final JdbcSourceEnumerator enumerator =
                new JdbcSourceEnumerator(context, splitter, null, new ArrayList<>());

        enumerator.start();
        context.runNextCallable();
        // The provider advanced on the worker thread, but its split has not been handled yet.
        assertThat(provider.getLatestOptionalState()).isEqualTo(1001L);

        final JdbcSourceEnumeratorState checkpoint = enumerator.snapshotState(1L);
        // Without the initial-state fallback this would be null and the first batch would be lost.
        assertThat(checkpoint.getOptionalUserDefinedSplitEnumeratorState()).isEqualTo(1000L);
        assertThat(checkpoint.getRemainingSplits()).isEmpty();

        // A global failover reuses the source's splitter, i.e. the already advanced provider.
        final ControllableAsyncContext restoredContext = new ControllableAsyncContext(1);
        final JdbcSourceEnumerator restored =
                new JdbcSourceEnumerator(
                        restoredContext,
                        splitter.restoreState(
                                checkpoint.getOptionalUserDefinedSplitEnumeratorState()),
                        null,
                        new ArrayList<>(checkpoint.getRemainingSplits()));
        restored.start();
        restoredContext.runNextCallable();
        restoredContext.runNextHandler();

        assertThat(restored.snapshotState(2L).getRemainingSplits())
                .extracting(split -> split.getParameters()[0])
                .containsExactly(1000L);
    }

    @Test
    void testAtMostOneEnumerationIsInFlight() throws Exception {
        final ControllableAsyncContext context = new ControllableAsyncContext(4);
        final CountingSplitterEnumerator splitter = new CountingSplitterEnumerator();
        final JdbcSourceEnumerator enumerator =
                new JdbcSourceEnumerator(context, splitter, null, new ArrayList<>());

        enumerator.start();
        // Even with an enumerator parallelism of 4, only one enumeration may be scheduled.
        assertThat(context.pendingCallableCount()).isEqualTo(1);

        context.runNextCallable();
        // The handler has not run, so no further enumeration may have been scheduled.
        assertThat(context.pendingCallableCount()).isZero();
    }

    /**
     * A {@link TestingSplitEnumeratorContext} that keeps the async callable and its handler apart,
     * so that a test can run the callable, observe the intermediate state, and only then run the
     * handler. The parent class always runs the two back to back, which hides the race.
     */
    private static final class ControllableAsyncContext
            extends TestingSplitEnumeratorContext<JdbcSourceSplit> {

        private final List<Runnable> pendingCallables = new ArrayList<>();
        private final List<Runnable> pendingHandlers = new ArrayList<>();

        private ControllableAsyncContext(int parallelism) {
            super(parallelism);
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            pendingCallables.add(
                    () -> {
                        try {
                            final T result = callable.call();
                            pendingHandlers.add(() -> handler.accept(result, null));
                        } catch (Throwable t) {
                            pendingHandlers.add(() -> handler.accept(null, t));
                        }
                    });
        }

        private void runNextCallable() {
            assertThat(pendingCallables).as("a pending callable").isNotEmpty();
            pendingCallables.remove(0).run();
        }

        private void runNextHandler() {
            assertThat(pendingHandlers).as("a pending handler").isNotEmpty();
            pendingHandlers.remove(0).run();
        }

        private int pendingCallableCount() {
            return pendingCallables.size();
        }
    }

    /**
     * A {@link JdbcSlideTimingParameterProvider} with a controllable clock, so that the number of
     * produced windows is deterministic and does not depend on wall-clock time.
     */
    private static final class ControllableSlideTimingProvider
            extends JdbcSlideTimingParameterProvider {

        private final long now;

        private ControllableSlideTimingProvider(
                long startMillis,
                long slideSpanMills,
                long slideStepMills,
                long splitGenerateDelayMillis,
                long now) {
            super(startMillis, slideSpanMills, slideStepMills, splitGenerateDelayMillis);
            this.now = now;
        }

        @Override
        public Long currentAvailableMillis() {
            return now;
        }
    }

    /**
     * Produces one split per enumeration and records the number of splits handed out so far as its
     * serializable state, mimicking how the real splitter enumerators advance their cursor.
     */
    private static final class CountingSplitterEnumerator implements SplitterEnumerator {

        private int nextSplit = 0;
        private int produced = 0;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public void start(JdbcConnectionProvider connectionProvider) {}

        @Override
        public void close() {}

        @Override
        public boolean isAllSplitsFinished() {
            return false;
        }

        @Override
        public List<JdbcSourceSplit> enumerateSplits() {
            final String splitId = String.format("split-%03d", nextSplit++);
            produced = nextSplit;
            return Collections.singletonList(
                    new JdbcSourceSplit(
                            splitId,
                            "select 1",
                            new Serializable[] {nextSplit},
                            new CheckpointedOffset()));
        }

        @Override
        public List<String> lineageQueries() {
            return Collections.emptyList();
        }

        @Override
        public Serializable serializableState() {
            return produced;
        }

        @Override
        public SplitterEnumerator restoreState(Serializable state) {
            this.nextSplit = (Integer) state;
            this.produced = (Integer) state;
            return this;
        }
    }
}
