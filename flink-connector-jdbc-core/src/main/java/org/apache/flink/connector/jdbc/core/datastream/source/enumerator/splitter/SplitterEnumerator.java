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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/** Interface for jdbc sql split enumerator. */
@PublicEvolving
public interface SplitterEnumerator extends AutoCloseable, Serializable {

    /** Returns whether this enumerator produces a bounded or continuous unbounded stream. */
    Boundedness getBoundedness();

    /**
     * Start the enumerator.
     *
     * @param connectionProvider The JDBC connection provider.
     */
    void start(JdbcConnectionProvider connectionProvider);

    /** Close the enumerator. */
    void close();

    /** All splits have been enumerated. */
    boolean isAllSplitsFinished();

    /** Enumerate the JDBC splits. */
    List<JdbcSourceSplit> enumerateSplits();

    /**
     * Confirms that the given splits returned by a previous {@link #enumerateSplits()} call have
     * been handed to the enumerator (buffered or assigned). Until confirmed, enumerated splits are
     * retained in the enumerator's serializable state so that a checkpoint cutting in between the
     * enumeration and the hand-over does not silently lose them. The default implementation is a
     * no-op for splitters that do not track in-flight splits.
     *
     * @param splits The splits that have been delivered to the enumerator.
     */
    default void confirmSplitsDelivered(List<JdbcSourceSplit> splits) {}

    /**
     * Returns the id of the global snapshot the <em>current</em> enumeration runs within, or {@code
     * null} if this splitter does not participate in a snapshot. Split ids restored from a previous
     * checkpoint carry the snapshot id exported by the failed attempt, whose exporting connection
     * is gone; the enumerator re-stamps such splits with this value at hand-over so every split of
     * the restored run is read within the freshly exported snapshot. The default implementation
     * returns {@code null} for splitters without snapshot support.
     */
    @Nullable
    default String currentSnapshotId() {
        return null;
    }

    /**
     * Get lineage queries for splits.
     *
     * @return The lineage queries.
     */
    List<String> lineageQueries();

    /**
     * Get the serializable state of the enumerator.
     *
     * @return The serializable state.
     */
    Serializable serializableState();

    /**
     * Restore the enumerator state from the given serializable state.
     *
     * @param state The serializable state.
     * @return The restored SplitterEnumerator.
     */
    SplitterEnumerator restoreState(Serializable state);
}
