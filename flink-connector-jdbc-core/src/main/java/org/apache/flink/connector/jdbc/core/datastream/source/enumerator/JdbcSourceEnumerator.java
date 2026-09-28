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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.SplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** JDBC source enumerator. */
public class JdbcSourceEnumerator
        implements SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceEnumerator.class);

    private final SplitEnumeratorContext<JdbcSourceSplit> context;
    private final List<JdbcSourceSplit> unassigned;
    private final SplitterEnumerator splitterEnumerator;
    private final JdbcConnectionProvider connectionProvider;
    private final List<Integer> readersWaitingForSplits = new ArrayList<>();
    private final AtomicInteger asyncCallsPending = new AtomicInteger(0);

    /**
     * The splitter state that corresponds to the splits that have already been handed over to
     * {@link #unassigned} or to a reader.
     *
     * <p>Only touched from the coordinator thread: {@link #onSplitsDiscovered} commits it and
     * {@link #snapshotState(long)} reads it. Both run on the same thread as each other, so the two
     * never interleave. It must NOT be read from {@link SplitterEnumerator#serializableState()}
     * directly in {@link #snapshotState(long)}: enumeration happens on a worker thread, so that
     * state can already have advanced past splits that are still in flight and therefore not part
     * of the checkpoint.
     */
    private Serializable lastHandledSplitterState;

    public JdbcSourceEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> context,
            SplitterEnumerator splitterEnumerator,
            JdbcConnectionProvider connectionProvider,
            List<JdbcSourceSplit> unassigned) {
        this.context = Preconditions.checkNotNull(context);
        this.splitterEnumerator = Preconditions.checkNotNull(splitterEnumerator);
        this.unassigned = Preconditions.checkNotNull(unassigned);
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void start() {
        splitterEnumerator.start(connectionProvider);
        // No split has been handled yet: this is the state a concurrent checkpoint has to fall
        // back to while the first batch is still being enumerated on a worker thread.
        lastHandledSplitterState = splitterEnumerator.serializableState();
        preDiscoverSplits();
    }

    @Override
    public void close() throws IOException {
        splitterEnumerator.close();
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public void handleSplitRequest(int subtask, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtask)) {
            LOG.warn("Ignoring split request from unregistered reader {}", subtask);
            return;
        }
        final Optional<JdbcSourceSplit> nextSplit = getNextSplit();
        if (nextSplit.isPresent()) {
            context.assignSplit(nextSplit.get(), subtask);
            LOG.info("Assigned split to subtask {} : {}", subtask, nextSplit.get());
            preDiscoverSplits();
        } else {
            if (!readersWaitingForSplits.contains(subtask)) {
                readersWaitingForSplits.add(subtask);
            }
            preDiscoverSplits();
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        LOG.debug("Source Enumerator adds splits back: {}", splits);
        unassigned.addAll(splits);
        if (context.registeredReaders().containsKey(subtaskId)
                && !readersWaitingForSplits.contains(subtaskId)) {
            readersWaitingForSplits.add(subtaskId);
        }
        preDiscoverSplits();
    }

    @Override
    public JdbcSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.debug("Source Checkpoint is {}", checkpointId);
        return new JdbcSourceEnumeratorState(
                Collections.emptyList(),
                Collections.emptyList(),
                new ArrayList<>(unassigned),
                lastHandledSplitterState);
    }

    private Optional<JdbcSourceSplit> getNextSplit() {
        if (unassigned == null || unassigned.isEmpty()) {
            return Optional.empty();
        }
        Iterator<JdbcSourceSplit> iterator = unassigned.iterator();
        JdbcSourceSplit next = iterator.next();
        iterator.remove();
        return Optional.of(next);
    }

    private void preDiscoverSplits() {
        // Keep at most one enumeration in flight. The splitter state that goes with a batch is
        // committed by its handler, so the commits only stay ordered consistently with the state
        // advances while there is a single enumeration running at a time. callAsync allows
        // concurrent callables, so relying on the completion order would be unsafe.
        while (asyncCallsPending.get() < 1 && !splitterEnumerator.isAllSplitsFinished()) {
            asyncCallsPending.incrementAndGet();
            context.callAsync(this::enumerateSplitsWithState, this::onSplitsDiscovered);
        }

        signalNoMoreSplitsIfDone();
    }

    /**
     * Enumerates splits and captures the splitter state that corresponds to exactly those splits.
     *
     * <p>Both are captured on the same worker thread invocation, so the returned state and splits
     * belong together. Splits must never be separated from the state that produced them, otherwise
     * a checkpoint taken in between would either drop splits or duplicate them.
     */
    private EnumerationResult enumerateSplitsWithState() {
        final List<JdbcSourceSplit> splits = splitterEnumerator.enumerateSplits();
        return new EnumerationResult(splits, splitterEnumerator.serializableState());
    }

    private void onSplitsDiscovered(EnumerationResult result, Throwable error) {
        asyncCallsPending.decrementAndGet();
        if (error != null) {
            LOG.error("Failed to discover splits.", error);
            preDiscoverSplits();
            return;
        }

        // The splits are now owned by this enumerator (buffered) or by a reader, so the state that
        // produced them may be included in a checkpoint from here on. Committing it here, on the
        // coordinator thread, keeps the checkpointed state in sync with the splits that have
        // actually been handled; preDiscoverSplits keeps at most one enumeration in flight so the
        // states are always committed in the order they were captured.
        lastHandledSplitterState = result.state;

        if (result.splits != null && !result.splits.isEmpty()) {
            assignOrBuffer(result.splits);
            preDiscoverSplits();
        } else if (!splitterEnumerator.isAllSplitsFinished()) {
            preDiscoverSplits();
        } else {
            signalNoMoreSplitsIfDone();
        }
    }

    private void assignOrBuffer(List<JdbcSourceSplit> splits) {
        for (JdbcSourceSplit split : splits) {
            if (!readersWaitingForSplits.isEmpty()) {
                int subtaskId = readersWaitingForSplits.remove(0);
                if (context.registeredReaders().containsKey(subtaskId)) {
                    LOG.info(
                            "Assigning discovered split {} to waiting subtask {}",
                            split,
                            subtaskId);
                    context.assignSplit(split, subtaskId);
                } else {
                    unassigned.add(split);
                }
            } else {
                unassigned.add(split);
            }
        }
    }

    private void signalNoMoreSplitsIfDone() {
        if (asyncCallsPending.get() == 0
                && splitterEnumerator.isAllSplitsFinished()
                && unassigned.isEmpty()
                && !readersWaitingForSplits.isEmpty()) {
            for (int subtaskId : readersWaitingForSplits) {
                context.signalNoMoreSplits(subtaskId);
                LOG.info("No more splits available for subtask {}", subtaskId);
            }
            readersWaitingForSplits.clear();
        }
    }

    /**
     * The outcome of a single {@link SplitterEnumerator#enumerateSplits()} invocation: the produced
     * splits together with the splitter state as of that invocation.
     */
    private static final class EnumerationResult {
        private final @Nullable List<JdbcSourceSplit> splits;
        private final @Nullable Serializable state;

        private EnumerationResult(
                @Nullable List<JdbcSourceSplit> splits, @Nullable Serializable state) {
            this.splits = splits;
            this.state = state;
        }
    }
}
