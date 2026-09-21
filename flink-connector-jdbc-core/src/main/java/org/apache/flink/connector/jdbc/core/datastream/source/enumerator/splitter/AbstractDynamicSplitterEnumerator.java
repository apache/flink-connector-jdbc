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
 * <p>Because {@code enumerateSplits()} can be invoked concurrently by the coordinator before the
 * first call returns, discovery is guarded so it only ever runs once.
 */
@Internal
public abstract class AbstractDynamicSplitterEnumerator implements SplitterEnumerator {

    private transient JdbcConnectionProvider connectionProvider;

    private List<JdbcSourceSplit> discoveredSplits;

    private boolean finished = false;

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
    public synchronized List<JdbcSourceSplit> enumerateSplits() {
        if (finished) {
            return Collections.emptyList();
        }
        if (discoveredSplits == null) {
            try {
                Connection connection = connectionProvider.getOrEstablishConnection();
                discoveredSplits = discoverSplits(connection);
            } catch (Exception e) {
                throw new RuntimeException("Failed to discover splits for " + describeSource(), e);
            }
        }
        finished = true;
        return discoveredSplits;
    }

    /**
     * Discover the full set of splits using the given, already-established connection. Called at
     * most once per enumerator instance.
     */
    protected abstract List<JdbcSourceSplit> discoverSplits(Connection connection) throws Exception;

    /** Short human-readable description of what this enumerator discovers splits for. */
    protected abstract String describeSource();

    @Override
    public synchronized boolean isAllSplitsFinished() {
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
        // All splits are discovered and handed out in a single enumerateSplits() batch, so by
        // the time any checkpoint happens either nothing was discovered yet (safe to redo from
        // scratch on restore) or discovery already fully completed (any not-yet-assigned splits
        // are tracked separately by JdbcSourceEnumeratorState). The only thing that must survive
        // a restore is "don't run discovery again" - a single boolean is sufficient for that.
        return finished;
    }

    @Override
    public SplitterEnumerator restoreState(Serializable state) {
        this.finished = Boolean.TRUE.equals(state);
        return this;
    }
}
