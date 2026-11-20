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
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.SnapshotEnumeratorTestUtils.drainAllSplits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncSnapshotSplitterEnumeratorTest {

    @Test
    void testOfferAndOfferAllArePickedUpByEnumerateSplits() {
        TestAsyncEnumerator enumerator =
                new TestAsyncEnumerator(
                        self -> {
                            self.offer("a");
                            self.offerAll(Arrays.asList("b", "c"));
                        });

        enumerator.startBackgroundWork();
        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);

        assertThat(splits.stream().map(JdbcSourceSplit::getSqlTemplate))
                .containsExactlyInAnyOrder("a", "b", "c");
        assertThat(enumerator.isAllSplitsFinished()).isTrue();
    }

    @Test
    void testOfferAllWithEmptyCollectionDoesNotWakeUpEarly() {
        TestAsyncEnumerator enumerator =
                new TestAsyncEnumerator(
                        self -> {
                            self.offerAll(Collections.emptyList());
                            self.offer("only");
                        });

        enumerator.startBackgroundWork();
        List<JdbcSourceSplit> splits = drainAllSplits(enumerator);

        assertThat(splits).hasSize(1);
    }

    @Test
    void testIsAllSplitsFinishedFalseBeforeStart() {
        TestAsyncEnumerator enumerator = new TestAsyncEnumerator(self -> {});

        assertThat(enumerator.isAllSplitsFinished()).isFalse();
    }

    @Test
    void testBackgroundFailurePropagatesAsIllegalStateException() {
        RuntimeException failure = new RuntimeException("boom");
        TestAsyncEnumerator enumerator =
                new TestAsyncEnumerator(
                        self -> {
                            throw failure;
                        });

        enumerator.startBackgroundWork();

        assertThatThrownBy(enumerator::enumerateSplits)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Split computation failed")
                .cause()
                .isSameAs(failure);
        // A failure is deliberately not "finished": the enumerator keeps seeing work pending,
        // so the thrown error propagates to the coordinator and fails the job instead of letting
        // it finish "successfully" with zero splits.
        assertThat(enumerator.isAllSplitsFinished()).isFalse();
    }

    @Test
    void testInterruptedExceptionDuringBackgroundWorkDoesNotSetFailure()
            throws InterruptedException {
        TestAsyncEnumerator enumerator =
                new TestAsyncEnumerator(
                        self -> {
                            throw new InterruptedException("interrupted");
                        });

        enumerator.startBackgroundWork();
        // Give the background thread a moment to run and hit the finally block.
        waitUntil(enumerator::isAllSplitsFinished);

        assertThat(enumerator.enumerateSplits()).isEmpty();
        assertThat(enumerator.isAllSplitsFinished()).isTrue();
        // Clear the interrupt flag set on this test thread's pool worker isn't relevant here;
        // only the background thread's flag is set, which is a distinct thread.
    }

    @Test
    void testCloseBeforeStartDoesNotThrowAndCallsCloseResources() {
        TestAsyncEnumerator enumerator = new TestAsyncEnumerator(self -> {});

        enumerator.close();

        assertThat(enumerator.closeResourcesCalls.get()).isEqualTo(1);
    }

    @Test
    void testCloseAfterStartShutsDownBackgroundThreadAndCallsCloseResources()
            throws InterruptedException {
        CountDownLatch blockUntilClosed = new CountDownLatch(1);
        TestAsyncEnumerator enumerator =
                new TestAsyncEnumerator(
                        self -> {
                            self.offer("first");
                            blockUntilClosed.await();
                        });

        enumerator.startBackgroundWork();
        // Wait for the first item so we know the background thread is actually running.
        assertThat(enumerator.enumerateSplits()).hasSize(1);

        enumerator.close();

        assertThat(enumerator.closeResourcesCalls.get()).isEqualTo(1);
    }

    @Test
    void testGetBoundednessIsConfigurable() {
        assertThat(new TestAsyncEnumerator(self -> {}).getBoundedness().name())
                .isEqualTo("CONTINUOUS_UNBOUNDED");
    }

    @Test
    void testUnconfirmedSplitsStayInSerializableStateUntilDelivered() throws InterruptedException {
        TestAsyncEnumerator enumerator = new TestAsyncEnumerator(self -> self.offer("a"));

        enumerator.startBackgroundWork();
        List<JdbcSourceSplit> splits = enumerator.enumerateSplits();
        assertThat(splits).hasSize(1);

        // The split left the output queue but was not confirmed as buffered/assigned: it must
        // still be captured by the checkpoint state, otherwise a checkpoint cutting in between
        // the poll and the hand-over would silently lose it.
        assertThat((List<?>) enumerator.serializableState()).hasSize(1);
        assertThat(enumerator.isAllSplitsFinished()).isFalse();

        enumerator.confirmSplitsDelivered(splits);
        waitUntil(enumerator::isAllSplitsFinished);
        assertThat((List<?>) enumerator.serializableState()).isEmpty();
    }

    @Test
    void testSerializableStateCapturesPendingItems() throws InterruptedException {
        TestAsyncEnumerator enumerator =
                new TestAsyncEnumerator(
                        self -> {
                            self.offer("a");
                            self.offer("b");
                        });

        enumerator.startBackgroundWork();
        waitUntil(
                () -> {
                    Serializable state = enumerator.serializableState();
                    return state instanceof List && ((List<?>) state).size() == 2;
                });

        assertThat(enumerator.lastSnapshottedPending).containsExactly("a", "b");
        assertThat(enumerator.restoreState(null)).isSameAs(enumerator);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRestoreStateReEmitsPendingItems() {
        TestAsyncEnumerator enumerator = new TestAsyncEnumerator(self -> {});

        enumerator.restoreState(new ArrayList<>(Arrays.asList("a", "b")));

        assertThat(enumerator.enumerateSplits().stream().map(JdbcSourceSplit::getSqlTemplate))
                .containsExactly("a", "b");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testToSplitFailureReQueuesItemForCheckpoint() throws InterruptedException {
        // A toSplit that throws after the item was polled must not drop the chunk: the item is put
        // back on the queue so a checkpoint completing before the failure reaches the coordinator
        // still persists it (otherwise the chunk is silently skipped after restore).
        TestAsyncEnumerator enumerator = new TestAsyncEnumerator(self -> self.offer("a"));
        enumerator.startBackgroundWork();
        // Wait until the item is queued (before draining) using a checkpoint read.
        waitUntil(() -> enumerator.queuedItemCount() == 1);

        enumerator.failNextToSplit = true;
        assertThatThrownBy(enumerator::enumerateSplits)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("toSplit boom");

        // Item is still accounted for and the pending count is unchanged.
        assertThat((List<String>) enumerator.serializableState()).containsExactly("a");
        assertThat(enumerator.queuedItemCount()).isEqualTo(1);

        // And it is delivered normally once the conversion succeeds.
        enumerator.failNextToSplit = false;
        assertThat(drainAllSplits(enumerator).stream().map(JdbcSourceSplit::getSqlTemplate))
                .containsExactly("a");
    }

    @Test
    void testInitConnectionRejectsNonConnectionProviderInstance() {
        TestAsyncEnumerator enumerator = new TestAsyncEnumerator(self -> {});

        assertThatThrownBy(
                        () ->
                                enumerator.initConnection(
                                        new SnapshotEnumeratorTestUtils.NotAConnectionProvider()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ConnectionProvider");
    }

    @Test
    void testInitConnectionAcceptsConnectionProviderInstance() {
        TestAsyncEnumerator enumerator = new TestAsyncEnumerator(self -> {});
        FakeConnectionProvider connectionProvider =
                new FakeConnectionProvider(
                        Collections.emptySet(), new HashMap<>(), new HashMap<>());

        enumerator.initConnection(connectionProvider);

        assertThat(enumerator.connection).isSameAs(connectionProvider);
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
    }

    /** Functional interface for the background work under test, given access to {@code this}. */
    @FunctionalInterface
    private interface BackgroundWork {
        void run(TestAsyncEnumerator self) throws Exception;
    }

    /** Minimal concrete subclass exercising the shared async lifecycle machinery. */
    private static final class TestAsyncEnumerator extends AsyncSnapshotSplitterEnumerator<String> {

        private final BackgroundWork work;
        final AtomicInteger closeResourcesCalls = new AtomicInteger();
        volatile List<String> lastSnapshottedPending = null;
        volatile boolean failNextToSplit = false;

        TestAsyncEnumerator(BackgroundWork work) {
            super("test", Boundedness.CONTINUOUS_UNBOUNDED);
            this.work = work;
        }

        @Override
        public void start(JdbcConnectionProvider connectionProvider) {
            initConnection(connectionProvider);
            startBackgroundWork();
        }

        @Override
        public List<String> lineageQueries() {
            return Collections.emptyList();
        }

        @Override
        protected void runBackgroundWork() throws Exception {
            work.run(this);
        }

        @Override
        protected JdbcSourceSplit toSplit(String item) {
            if (failNextToSplit) {
                throw new RuntimeException("toSplit boom");
            }
            return new JdbcSourceSplit(item, item, null, null);
        }

        @Override
        protected void closeResources() {
            closeResourcesCalls.incrementAndGet();
        }

        @Override
        protected Serializable snapshotProgress(List<String> pendingItems) {
            lastSnapshottedPending = new ArrayList<>(pendingItems);
            return new ArrayList<>(pendingItems);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void restoreProgress(Serializable state) {
            restorePendingItems((List<String>) state);
        }
    }
}
