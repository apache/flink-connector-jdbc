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

import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

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
        assertThat(enumerator.isAllSplitsFinished()).isTrue();
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
    void testGetBoundednessIsContinuousUnbounded() {
        TestAsyncEnumerator enumerator = new TestAsyncEnumerator(self -> {});

        assertThat(enumerator.getBoundedness().name()).isEqualTo("CONTINUOUS_UNBOUNDED");
    }

    @Test
    void testSerializableStateAndRestoreState() {
        TestAsyncEnumerator enumerator = new TestAsyncEnumerator(self -> {});

        assertThat(enumerator.serializableState()).isNull();
        assertThat(enumerator.restoreState(null)).isSameAs(enumerator);
    }

    @Test
    void testInitConnectionRejectsNonConnectionProviderInstance() {
        TestAsyncEnumerator enumerator = new TestAsyncEnumerator(self -> {});

        assertThatThrownBy(() -> enumerator.initConnection(new NotAConnectionProvider()))
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

    private static List<JdbcSourceSplit> drainAllSplits(
            AsyncSnapshotSplitterEnumerator<?> enumerator) {
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
        public Connection getConnection() {
            return null;
        }

        @Override
        public boolean isConnectionValid() {
            return false;
        }

        @Override
        public Connection getOrEstablishConnection() {
            return null;
        }

        @Override
        public void closeConnection() {}

        @Override
        public Connection reestablishConnection() {
            return null;
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

        TestAsyncEnumerator(BackgroundWork work) {
            super("test");
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
            return new JdbcSourceSplit(item, item, null, null);
        }

        @Override
        protected void closeResources() {
            closeResourcesCalls.incrementAndGet();
        }
    }
}
