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
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.SplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.split.CheckpointedOffset;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link
 * org.apache.flink.connector.jdbc.core.datastream.source.enumerator.JdbcSourceEnumerator}.
 */
class JdbcSourceEnumeratorTest {

    private static long splitId = 1L;

    private TestingSplitEnumeratorContext<JdbcSourceSplit> context;
    private JdbcSourceSplit split;
    private JdbcSourceEnumerator enumerator;

    @BeforeEach
    void setup() {
        this.context = new TestingSplitEnumeratorContext<>(4);
        this.split = createRandomSplit();
        this.enumerator = createEnumerator(context, split);
    }

    @Test
    void testCheckpointNoSplitRequested() throws Exception {
        JdbcSourceEnumeratorState state = enumerator.snapshotState(1L);
        assertThat(state.getPendingSplits()).isEmpty();
        assertThat(state.getRemainingSplits()).contains(split);
    }

    @Test
    void testSplitRequestForRegisteredReader() throws Exception {
        context.registerReader(3, "somehost");
        enumerator.addReader(3);
        enumerator.handleSplitRequest(3, "somehost");
        assertThat(enumerator.snapshotState(1L).getRemainingSplits()).isEmpty();
        assertThat(context.getSplitAssignments().get(3).getAssignedSplits()).contains(split);
    }

    @Test
    void testSplitRequestForNonRegisteredReader() throws Exception {
        enumerator.handleSplitRequest(3, "somehost");
        assertThat(context.getSplitAssignments()).doesNotContainKey(3);
        assertThat(enumerator.snapshotState(1L).getRemainingSplits()).contains(split);
    }

    @Test
    void testNoMoreSplits() {
        // first split assignment
        context.registerReader(1, "somehost");
        enumerator.addReader(1);
        enumerator.handleSplitRequest(1, "somehost");

        // second request has no more split
        enumerator.handleSplitRequest(1, "somehost");

        assertThat(context.getSplitAssignments().get(1).getAssignedSplits()).contains(split);
        assertThat(context.getSplitAssignments().get(1).hasReceivedNoMoreSplitsSignal()).isTrue();
    }

    @Test
    void testBackgroundEnumerationFailureFailsTheJob() {
        // Enumeration failures are sticky, so onSplitsDiscovered must rethrow on the coordinator
        // thread (which Flink turns into a job failure) instead of spinning on the error.
        TestingSplitEnumeratorContext<JdbcSourceSplit> ctx = new TestingSplitEnumeratorContext<>(1);
        IllegalStateException failure = new IllegalStateException("boom");
        JdbcSourceEnumerator failingEnumerator =
                new JdbcSourceEnumerator(
                        ctx,
                        new StubSplitterEnumerator() {
                            @Override
                            public boolean isAllSplitsFinished() {
                                return false;
                            }

                            @Override
                            public List<JdbcSourceSplit> enumerateSplits() {
                                throw failure;
                            }
                        },
                        null,
                        new ArrayList<>());

        failingEnumerator.start();

        assertThatThrownBy(() -> ctx.getExecutorService().triggerAll())
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to discover splits")
                .cause()
                .isSameAs(failure);
    }

    @Test
    void testStaleSnapshotIdIsRefreshedAtAssignment() {
        // A split restored from a checkpoint carries the snapshot id exported by the failed
        // attempt, which no longer exists; the enumerator must re-stamp it with the current
        // snapshot before handing it to a reader.
        TestingSplitEnumeratorContext<JdbcSourceSplit> ctx = new TestingSplitEnumeratorContext<>(1);
        JdbcSourceSplit staleSplit =
                new JdbcSourceSplit("s1", "select 1", null, null, "snapshot-old");
        JdbcSourceEnumerator restoringEnumerator =
                new JdbcSourceEnumerator(
                        ctx,
                        new StubSplitterEnumerator() {
                            @Override
                            public String currentSnapshotId() {
                                return "snapshot-new";
                            }
                        },
                        null,
                        new ArrayList<>(Collections.singletonList(staleSplit)));

        ctx.registerReader(0, "host");
        restoringEnumerator.addReader(0);
        restoringEnumerator.handleSplitRequest(0, "host");

        List<JdbcSourceSplit> assigned = ctx.getSplitAssignments().get(0).getAssignedSplits();
        assertThat(assigned).hasSize(1);
        assertThat(assigned.get(0).splitId()).isEqualTo("s1");
        assertThat(assigned.get(0).getGlobalSnapshotId()).isEqualTo("snapshot-new");
    }

    @Test
    void testNoEnumerationScheduledWhileUnassignedBacklogFull() {
        // Slow readers must not let discovery outrun consumption: while the unassigned backlog is
        // at its ceiling, no further enumeration calls may be scheduled (back-pressure for the
        // coordinator heap and checkpoint size).
        List<JdbcSourceSplit> fullBacklog =
                new ArrayList<>(
                        Collections.nCopies(
                                JdbcSourceEnumerator.MAX_UNASSIGNED_SPLITS, createRandomSplit()));
        TestingSplitEnumeratorContext<JdbcSourceSplit> ctx = new TestingSplitEnumeratorContext<>(1);
        JdbcSourceEnumerator backloggedEnumerator =
                new JdbcSourceEnumerator(
                        ctx,
                        new StubSplitterEnumerator() {
                            @Override
                            public boolean isAllSplitsFinished() {
                                return false;
                            }
                        },
                        null,
                        fullBacklog);

        backloggedEnumerator.start();

        assertThat(ctx.getExecutorService().numQueuedRunnables()).isZero();
    }

    @Test
    void testCloseClosesProviderConnection() throws Exception {
        // The database-level splitter deliberately leaves the main provider connection open; the
        // enumerator must close it, or the snapshot-exporting REPEATABLE READ transaction pins the
        // Postgres xmin horizon for the lifetime of the TaskManager JVM.
        RecordingConnectionProvider provider = new RecordingConnectionProvider();
        JdbcSourceEnumerator closingEnumerator =
                new JdbcSourceEnumerator(
                        context, new StubSplitterEnumerator(), provider, new ArrayList<>());

        closingEnumerator.close();

        assertThat(provider.closeCount).isEqualTo(1);
    }

    @Test
    void testPushedSplitsAreRefreshedBeforeAssignment() {
        // Splits discovered by the background enumeration go through assignOrBuffer, not
        // handleSplitRequest: the snapshot re-stamp must cover that path too, or a split restored
        // by the splitter and re-discovered after failover reaches the reader with a dead id.
        TestingSplitEnumeratorContext<JdbcSourceSplit> ctx = new TestingSplitEnumeratorContext<>(1);
        JdbcSourceSplit staleSplit =
                new JdbcSourceSplit("s1", "select 1", null, null, "snapshot-old");
        AtomicBoolean delivered = new AtomicBoolean(false);
        JdbcSourceEnumerator pushingEnumerator =
                new JdbcSourceEnumerator(
                        ctx,
                        new StubSplitterEnumerator() {
                            @Override
                            public boolean isAllSplitsFinished() {
                                return delivered.get();
                            }

                            @Override
                            public String currentSnapshotId() {
                                return "snapshot-new";
                            }

                            @Override
                            public List<JdbcSourceSplit> enumerateSplits() {
                                return delivered.getAndSet(true)
                                        ? Collections.emptyList()
                                        : Collections.singletonList(staleSplit);
                            }
                        },
                        null,
                        new ArrayList<>());

        ctx.registerReader(0, "host");
        pushingEnumerator.addReader(0);
        pushingEnumerator.handleSplitRequest(0, "host");
        ctx.getExecutorService().triggerAll();

        List<JdbcSourceSplit> assigned = ctx.getSplitAssignments().get(0).getAssignedSplits();
        assertThat(assigned).hasSize(1);
        assertThat(assigned.get(0).getGlobalSnapshotId()).isEqualTo("snapshot-new");
    }

    /** {@link JdbcConnectionProvider} that counts {@code closeConnection()} calls. */
    private static class RecordingConnectionProvider implements JdbcConnectionProvider {
        private int closeCount;

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
        public void closeConnection() {
            closeCount++;
        }

        @Override
        public Connection reestablishConnection() {
            return null;
        }
    }

    /** Minimal non-snapshot {@link SplitterEnumerator} stub with overridable behavior. */
    private static class StubSplitterEnumerator implements SplitterEnumerator {
        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void start(JdbcConnectionProvider connectionProvider) {}

        @Override
        public void close() {}

        @Override
        public boolean isAllSplitsFinished() {
            return true;
        }

        @Override
        public List<JdbcSourceSplit> enumerateSplits() {
            return Collections.emptyList();
        }

        @Override
        public List<String> lineageQueries() {
            return Collections.emptyList();
        }

        @Override
        public Serializable serializableState() {
            return null;
        }

        @Override
        public SplitterEnumerator restoreState(Serializable state) {
            return this;
        }
    }

    private static JdbcSourceSplit createRandomSplit() {
        return new JdbcSourceSplit(
                String.valueOf(splitId++),
                "select 1",
                new Serializable[] {0},
                new CheckpointedOffset(0, 0));
    }

    private static JdbcSourceEnumerator createEnumerator(
            final SplitEnumeratorContext<JdbcSourceSplit> context,
            final JdbcSourceSplit... splits) {

        return new JdbcSourceEnumerator(
                context,
                new SplitterEnumerator() {
                    @Override
                    public Boundedness getBoundedness() {
                        return Boundedness.BOUNDED;
                    }

                    @Override
                    public void start(JdbcConnectionProvider connectionProvider) {}

                    @Override
                    public void close() {}

                    @Override
                    public boolean isAllSplitsFinished() {
                        return true;
                    }

                    @Override
                    public List<JdbcSourceSplit> enumerateSplits() {
                        return Collections.emptyList();
                    }

                    @Override
                    public List<String> lineageQueries() {
                        return Collections.emptyList();
                    }

                    @Override
                    public Serializable serializableState() {
                        return null;
                    }

                    @Override
                    public SplitterEnumerator restoreState(Serializable state) {
                        return null;
                    }
                },
                null,
                Arrays.stream(splits).collect(Collectors.toList()));
    }
}
