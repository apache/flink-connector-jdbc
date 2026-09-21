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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;

/**
 * Base class for {@link SplitterEnumerator} implementations that must open a real database
 * connection at job runtime to discover their splits (e.g. by executing a user-provided query), as
 * opposed to computing splits purely from static configuration.
 *
 * <p>Split discovery is deliberately deferred to the first call of {@link #enumerateSplits()}
 * rather than performed in {@link #start(JdbcConnectionProvider)}: the enclosing {@code
 * JdbcSourceEnumerator} calls {@code start()} synchronously on the JobManager coordinator thread,
 * but invokes {@code enumerateSplits()} through Flink's own {@code
 * SplitEnumeratorContext#callAsync}, which already runs off that thread. Deferring the actual I/O
 * this way avoids blocking job startup and avoids having to manage any dedicated thread of our own.
 *
 * <p>{@code enumerateSplits()} runs on the callAsync worker pool, which - as of Flink 2.1.3 -
 * {@code SourceOperatorFactory} always sizes at exactly one thread for the standard {@code
 * env.fromSource(...)} path (there's no public API to request more), so two calls into this
 * method can never actually execute at once today. Discovery is still guarded by a dedicated lock
 * rather than relying on that: it's an internal-to-Flink implementation detail this class has no
 * contract with, so the lock is a correctness safety net against that changing (a future Flink
 * version raising worker parallelism, or a different call path than {@code
 * JdbcSourceEnumerator#preDiscoverSplits()}) rather than a fix for a concurrency issue observed
 * today. The lock adds no real cost while that assumption holds, since single-threaded callers
 * never contend on it.
 *
 * <p>That lock is intentionally separate from {@link #finished}, which is a plain {@code volatile}
 * rather than something callers need to synchronize on: {@link #isAllSplitsFinished()} and {@link
 * #serializableState()} are called directly from the coordinator thread (e.g. from {@code start()}
 * or while handling a checkpoint), and must never block waiting for the discovery query - a
 * discovery-query-sized stall on the coordinator thread would defeat the whole point of running
 * that query off-thread in the first place. {@link #finished} is also deliberately flipped to
 * {@code true} only on the call *after* the one that returned the discovered splits, not on the
 * same call. This one is a real, currently-reachable race, unrelated to worker-pool size: the
 * callAsync callback that returns the discovered splits and the coordinator's next checkpoint are
 * two independent tasks queued onto the same single-threaded coordinator executor, so a checkpoint
 * can be queued and run before {@code JdbcSourceEnumerator} has integrated the splits that
 * callback returned. Flipping {@link #finished} eagerly would let that checkpoint persist
 * "discovery is done" without the splits it produced, losing them for good on restore. Delaying it
 * means the same race merely costs a redundant (cheap, cached) rediscovery call.
 */
@Internal
public abstract class AbstractDynamicSplitterEnumerator implements SplitterEnumerator {

    private transient JdbcConnectionProvider connectionProvider;

    // Not final: a fresh lock is (re)created in readObject() below, since Object itself isn't
    // Serializable and this class - like the rest of SplitterEnumerator - must remain so despite
    // never actually needing to survive a Java-serialization round trip with useful lock state.
    private transient Object discoveryLock = new Object();

    private List<JdbcSourceSplit> discoveredSplits;

    private volatile boolean finished = false;

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public void start(JdbcConnectionProvider connectionProvider) {
        // Intentionally no I/O here - see class javadoc.
        this.connectionProvider = connectionProvider;
    }

    @Override
    public List<JdbcSourceSplit> enumerateSplits() {
        if (finished) {
            return Collections.emptyList();
        }
        synchronized (discoveryLock) {
            if (discoveredSplits == null) {
                try {
                    Connection connection = connectionProvider.getOrEstablishConnection();
                    discoveredSplits = discoverSplits(connection);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to discover splits for " + describeSource(), e);
                }
                return discoveredSplits;
            }
        }
        // Reached only on a call after discovery already completed (our own follow-up poll, or a
        // concurrent call that lost the race above) - see class javadoc for why finished flips
        // here rather than alongside returning discoveredSplits above.
        finished = true;
        return Collections.emptyList();
    }

    /**
     * Discover the full set of splits using the given, already-established connection. Called at
     * most once per enumerator instance.
     */
    protected abstract List<JdbcSourceSplit> discoverSplits(Connection connection) throws Exception;

    /** Short human-readable description of what this enumerator discovers splits for. */
    protected abstract String describeSource();

    @Override
    public boolean isAllSplitsFinished() {
        return finished;
    }

    @Override
    public void close() {
        if (connectionProvider != null) {
            connectionProvider.closeConnection();
        }
    }

    @Override
    public List<String> lineageQueries() {
        return Collections.singletonList(describeSource());
    }

    @Override
    public Serializable serializableState() {
        // All splits are discovered and handed out in a single enumerateSplits() batch, so by the
        // time any checkpoint observes finished == true, the splits it produced are guaranteed to
        // already be tracked by JdbcSourceEnumeratorState (see the delayed flip in
        // enumerateSplits() above). The only thing that must survive a restore is "don't run
        // discovery again" - a single boolean is sufficient for that.
        return finished;
    }

    @Override
    public SplitterEnumerator restoreState(Serializable state) {
        this.finished = Boolean.TRUE.equals(state);
        return this;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        discoveryLock = new Object();
    }
}
