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
import org.apache.flink.util.FlinkRuntimeException;
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

    /**
     * Ceiling for splits discovered but not yet handed to a reader. Without it the backlog simply
     * migrates from the splitter queue into {@link #unassigned} (the splitter pacing only observes
     * its own queue), so a slow reader set would grow the coordinator heap without bound and blow
     * up checkpoint size. Enumeration resumes once consumption drains the backlog.
     */
    static final int MAX_UNASSIGNED_SPLITS = 10_000;

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
        try {
            splitterEnumerator.close();
        } catch (RuntimeException primary) {
            // Still release the provider connection, but do not mask the primary failure.
            try {
                closeProviderConnection();
            } catch (RuntimeException suppressed) {
                primary.addSuppressed(suppressed);
            }
            throw primary;
        }
        // The database-level splitter deliberately does NOT close the main provider connection, so
        // close it here: for snapshot-capable providers that connection is the REPEATABLE READ
        // transaction exporting the shared snapshot. Leaving it open pins the Postgres xmin horizon
        // ("idle in transaction") for the lifetime of the TaskManager JVM, blocking vacuum. A
        // standalone splitter may have closed it already; closeConnection() is idempotent.
        closeProviderConnection();
    }

    private void closeProviderConnection() {
        if (connectionProvider != null) {
            connectionProvider.closeConnection();
        }
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
            JdbcSourceSplit split = refreshSnapshotId(nextSplit.get());
            context.assignSplit(split, subtask);
            LOG.debug("Assigned split to subtask {} : {}", subtask, split.splitId());
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

    /**
     * Re-stamps a split with the snapshot id of the current run. Splits restored from a checkpoint
     * carry the id exported by the failed attempt, but that exported snapshot died with its
     * exporting connection, so the reader would fail with "snapshot does not exist" (and keep
     * failing after every restart). Splits produced fresh by this run already carry the current id
     * and are returned unchanged; non-snapshot splits are unaffected.
     */
    private JdbcSourceSplit refreshSnapshotId(JdbcSourceSplit split) {
        String currentSnapshotId = splitterEnumerator.currentSnapshotId();
        if (currentSnapshotId == null) {
            return split;
        }
        return split.withGlobalSnapshotId(currentSnapshotId);
    }

    private void preDiscoverSplits() {
        // Keep at most one enumeration in flight. The splitter state that goes with a batch is
        // committed by its handler, so the commits only stay ordered consistently with the state
        // advances while there is a single enumeration running at a time. callAsync allows
        // concurrent callables, so relying on the completion order would be unsafe.
        while (asyncCallsPending.get() < 1
                && unassigned.size() < MAX_UNASSIGNED_SPLITS
                && !splitterEnumerator.isAllSplitsFinished()) {
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
    private EnumerationResult enumerateSplitsWithState() throws InterruptedException {
        final List<JdbcSourceSplit> splits = splitterEnumerator.enumerateSplits();
        if (splits.isEmpty() && !splitterEnumerator.isAllSplitsFinished()) {
            // Transiently empty: back off briefly instead of hammering the coordinator event
            // loop with instant empty results on the redelivery of this single in-flight call.
            Thread.sleep(50);
        }
        return new EnumerationResult(splits, splitterEnumerator.serializableState());
    }

    private void onSplitsDiscovered(EnumerationResult result, Throwable error) {
        asyncCallsPending.decrementAndGet();
        if (error != null) {
            // Enumeration failures are sticky (the background computation gave up; the same
            // failure would recur on every retry). Rethrow on the coordinator thread so the
            // job fails instead of spinning forever with — or finishing "successfully" without
            // — the splits that were never discovered.
            throw new FlinkRuntimeException("Failed to discover splits, failing the job.", error);
        }

        if (result.splits != null && !result.splits.isEmpty()) {
            assignOrBuffer(result.splits);
            // Release the splitter's in-flight staging (no-op for splitters that do not stage):
            // the enumerator now owns these splits, so the splitter's own state must not persist
            // them a second time. Commit the post-handover capture; with a single enumeration in
            // flight (see preDiscoverSplits) nothing can have advanced past these splits, so this
            // capture pairs exactly with them and keeps the checkpointed state in sync with the
            // splits that have actually been handled.
            splitterEnumerator.confirmSplitsDelivered(result.splits);
            lastHandledSplitterState = splitterEnumerator.serializableState();
            preDiscoverSplits();
        } else {
            lastHandledSplitterState = result.state;
            if (!splitterEnumerator.isAllSplitsFinished()) {
                preDiscoverSplits();
            } else {
                signalNoMoreSplitsIfDone();
            }
        }
    }

    private void assignOrBuffer(List<JdbcSourceSplit> splits) {
        for (JdbcSourceSplit split : splits) {
            // Re-stamp before buffering/assigning so restored splits never reach a reader with the
            // dead snapshot id of a previous attempt.
            JdbcSourceSplit refreshed = refreshSnapshotId(split);
            if (!readersWaitingForSplits.isEmpty()) {
                int subtaskId = readersWaitingForSplits.remove(0);
                if (context.registeredReaders().containsKey(subtaskId)) {
                    LOG.debug(
                            "Assigning discovered split {} to waiting subtask {}",
                            refreshed.splitId(),
                            subtaskId);
                    context.assignSplit(refreshed, subtaskId);
                } else {
                    unassigned.add(refreshed);
                }
            } else {
                unassigned.add(refreshed);
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
