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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String name;
    private final Queue<T> outputQueue = new ConcurrentLinkedQueue<>();

    protected transient ConnectionProvider connection;

    private transient ExecutorService executor;
    private transient AtomicBoolean workDone;
    private transient CountDownLatch firstReady;
    private transient volatile Throwable backgroundFailure;

    protected AsyncSnapshotSplitterEnumerator(String name) {
        this.name = name;
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
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /** Starts the background thread that runs {@link #runBackgroundWork()}. */
    protected final void startBackgroundWork() {
        this.workDone = new AtomicBoolean(false);
        this.firstReady = new CountDownLatch(1);
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
                        log.error("Background computation failed for {}", name, e);
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
        firstReady.countDown();
    }

    /** Offers a batch of computed items and wakes up anyone waiting on the first-ready signal. */
    protected final void offerAll(Collection<T> items) {
        if (!items.isEmpty()) {
            outputQueue.addAll(items);
            firstReady.countDown();
        }
    }

    @Override
    public final boolean isAllSplitsFinished() {
        return workDone != null && workDone.get() && outputQueue.isEmpty();
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

        List<JdbcSourceSplit> splits = new ArrayList<>();
        T item;
        while ((item = outputQueue.poll()) != null) {
            splits.add(toSplit(item));
        }
        return splits;
    }

    @Override
    public final void close() {
        if (executor != null) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn(
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

    @Override
    public final Serializable serializableState() {
        return null;
    }

    @Override
    public final SplitterEnumerator restoreState(Serializable state) {
        return this;
    }
}
