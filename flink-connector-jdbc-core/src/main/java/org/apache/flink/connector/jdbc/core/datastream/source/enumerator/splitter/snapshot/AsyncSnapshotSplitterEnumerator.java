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
import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionProvider;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.SplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared async background-computation machinery for the snapshot splitters ({@link
 * TableSplitterEnumerator}, {@link DatabaseSplitterEnumerator}): a single background daemon thread
 * computes {@code T} items and offers them into a queue, while {@link #enumerateSplits()} drains
 * and converts whatever's ready — non-blocking apart from a short wait for the very first item.
 *
 * @param <T> the type of item the background thread produces, converted to a {@link
 *     JdbcSourceSplit} at drain time via {@link #toSplit}
 */
abstract class AsyncSnapshotSplitterEnumerator<T> implements SplitterEnumerator {

    private static final Logger LOG =
            LoggerFactory.getLogger(AsyncSnapshotSplitterEnumerator.class);

    private final String name;
    private final Boundedness boundedness;
    private final Queue<T> outputQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queuedItemCount = new AtomicInteger();

    /**
     * Splits drained from {@link #outputQueue} by {@link #enumerateSplits()} but not yet confirmed
     * as buffered/assigned by the caller. Kept in serializable state until confirmed so a
     * checkpoint cutting in between cannot lose them.
     *
     * <p>All handover synchronization is done on the enumerator instance itself ({@code this}) so
     * it survives the round-trip serialization of the source to the JobManager (a dedicated lock
     * object would be {@code transient} and come back {@code null}). {@link #serializableState()}
     * holds this monitor for the whole snapshot, so an item is always accounted for either in the
     * queue, in the {@link #handedOffSplits} staging map, or in the subclass progress — never in a
     * gap in between.
     */
    private final Map<JdbcSourceSplit, T> handedOffSplits = new LinkedHashMap<>();

    protected transient ConnectionProvider connection;

    private transient ExecutorService executor;
    private transient AtomicBoolean workDone;
    private transient CountDownLatch firstReady;
    private transient volatile Throwable backgroundFailure;

    protected AsyncSnapshotSplitterEnumerator(String name, Boundedness boundedness) {
        this.name = name;
        this.boundedness = boundedness;
    }

    /** Validates and stores the connection provider for use by subclasses. */
    protected final void initConnection(JdbcConnectionProvider connectionProvider) {
        if (!(connectionProvider instanceof ConnectionProvider)) {
            throw new IllegalArgumentException(
                    "Connection provider must be an instance of "
                            + ConnectionProvider.class.getSimpleName());
        }
        this.connection = (ConnectionProvider) connectionProvider;
    }

    @Override
    public final Boundedness getBoundedness() {
        return boundedness;
    }

    /** Starts the background thread that runs {@link #runBackgroundWork()}. */
    protected final void startBackgroundWork() {
        this.workDone = new AtomicBoolean(false);
        this.firstReady = new CountDownLatch(1);
        if (!outputQueue.isEmpty() && readyToConvertQueuedItems()) {
            // Items restored via restorePendingItems() predate this latch; wake enumerateSplits()
            // immediately instead of letting it sit on the first-ready timeout. Gated on
            // readiness: converted items may still depend on state the background thread has not
            // computed yet (e.g. the primary key), which enumerateSplits() must not dereference.
            firstReady.countDown();
        }
        this.executor =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r, "snapshot-compute-" + name);
                            t.setDaemon(true);
                            return t;
                        });
        executor.submit(
                () -> {
                    try {
                        runBackgroundWork();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable e) {
                        LOG.error("Background computation failed for {}", name, e);
                        backgroundFailure = e;
                    } finally {
                        workDone.set(true);
                        firstReady.countDown();
                    }
                });
    }

    /** Subclass-specific unit of work; push results via {@link #offer}/{@link #offerAll}. */
    protected abstract void runBackgroundWork() throws Exception;

    /** Converts a queued item into an emittable split. */
    protected abstract JdbcSourceSplit toSplit(T item);

    /** Subclass-specific resource cleanup, called after the background thread has stopped. */
    protected abstract void closeResources();

    /** Offers a single computed item and wakes up anyone waiting on the first-ready signal. */
    protected final void offer(T item) {
        outputQueue.add(item);
        queuedItemCount.incrementAndGet();
        firstReady.countDown();
    }

    /** Offers a batch of computed items and wakes up anyone waiting on the first-ready signal. */
    protected final void offerAll(Collection<T> items) {
        if (!items.isEmpty()) {
            outputQueue.addAll(items);
            queuedItemCount.addAndGet(items.size());
            firstReady.countDown();
        }
    }

    @Override
    public final synchronized boolean isAllSplitsFinished() {
        // A background failure is deliberately NOT "finished": enumerateSplits() throws so the
        // failure reaches the coordinator and fails the job instead of ending it with partial
        // results. Splits staged in handedOffSplits are still owed to the caller. Read under the
        // same monitor as the staging map so an unsynchronized read cannot observe a torn state.
        return workDone != null
                && workDone.get()
                && backgroundFailure == null
                && outputQueue.isEmpty()
                && handedOffSplits.isEmpty();
    }

    /**
     * Whether {@link #toSplit(Object)} can safely convert items already sitting in {@link
     * #outputQueue} (including items restored before the background thread produced anything). The
     * base implementation returns {@code true}; subclasses whose conversion depends on lazily
     * computed state override this to stay false until that state exists, so a restored, non-empty
     * queue never causes {@code enumerateSplits()} to convert before the background thread is
     * ready.
     */
    protected boolean readyToConvertQueuedItems() {
        return true;
    }

    @Override
    public final List<JdbcSourceSplit> enumerateSplits() {
        // Wait briefly for the background thread to produce at least one item.
        if (firstReady != null) {
            try {
                firstReady.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (backgroundFailure != null) {
            throw new IllegalStateException(
                    "Split computation failed for " + name + " — refusing to emit partial splits",
                    backgroundFailure);
        }

        if (!readyToConvertQueuedItems()) {
            // Restored items exist but the background thread has not computed the state needed to
            // convert them yet. Report "transiently empty" instead of dereferencing that state and
            // turning a benign restore race into a fatal NPE.
            return Collections.emptyList();
        }

        List<JdbcSourceSplit> splits = new ArrayList<>();
        synchronized (this) {
            T item;
            while ((item = outputQueue.poll()) != null) {
                JdbcSourceSplit split;
                try {
                    split = toSplit(item);
                } catch (RuntimeException | Error e) {
                    // The polled item is in neither the queue nor the staging map: put it back so
                    // a checkpoint completing before the failure reaches the coordinator still
                    // accounts for it (otherwise the chunk is silently lost and skipped on
                    // restore). The count is decremented only on a successful conversion.
                    outputQueue.add(item);
                    throw e;
                }
                queuedItemCount.decrementAndGet();
                handedOffSplits.put(split, item);
                splits.add(split);
            }
        }
        return splits;
    }

    @Override
    public final void confirmSplitsDelivered(List<JdbcSourceSplit> splits) {
        if (splits.isEmpty()) {
            return;
        }
        synchronized (this) {
            splits.forEach(handedOffSplits::remove);
        }
    }

    @Override
    public final void close() {
        if (executor != null) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Background computation for {} did not stop within the shutdown grace period — a"
                                    + " query may still be running against its connection.",
                            name);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        closeResources();
    }

    /**
     * Hook for subclasses to capture their computation progress (and the given copy of items
     * already computed but not yet emitted) so a restored job resumes where it left off instead of
     * re-emitting every split. Returns {@code null} when nothing has been computed yet.
     */
    @Nullable
    protected abstract Serializable snapshotProgress(List<T> pendingItems);

    /**
     * Hook for subclasses to reinstate the progress captured by {@link #snapshotProgress}. Called
     * before {@code start()}; the restored computation must pick up from the persisted progress and
     * re-emit the given pending items that had not been handed out yet.
     */
    protected abstract void restoreProgress(Serializable state);

    /** Number of items currently queued for emission (approximate; for back-pressure checks). */
    protected final int queuedItemCount() {
        return queuedItemCount.get();
    }

    /** Re-queues items captured at checkpoint time so they are emitted again after restore. */
    protected final void restorePendingItems(Collection<T> items) {
        if (items.isEmpty()) {
            return;
        }
        synchronized (this) {
            outputQueue.addAll(items);
            queuedItemCount.addAndGet(items.size());
        }
        // No firstReady signal here: restore always precedes start(), and startBackgroundWork()
        // installs a fresh latch pre-counted-down when the queue is non-empty.
    }

    @Override
    public String currentSnapshotId() {
        // The snapshot is exported synchronously in start() (either directly, or — for the
        // database splitter — inherited pool clones carry it), so once the enumerator has started
        // this is always the id of the CURRENT attempt: exactly what a restored split must be
        // re-stamped with.
        return connection != null ? connection.getGlobalSnapshotId() : null;
    }

    @Override
    public final Serializable serializableState() {
        synchronized (this) {
            List<T> pending = new ArrayList<>(outputQueue);
            pending.addAll(handedOffSplits.values());
            return snapshotProgress(pending);
        }
    }

    @Override
    public final SplitterEnumerator restoreState(Serializable state) {
        if (state != null) {
            restoreProgress(state);
        }
        return this;
    }
}
