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
                splitterEnumerator.serializableState());
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
        int targetParallelism = context.currentParallelism();
        while (asyncCallsPending.get() < targetParallelism
                && !splitterEnumerator.isAllSplitsFinished()) {
            asyncCallsPending.incrementAndGet();
            context.callAsync(() -> splitterEnumerator.enumerateSplits(), this::onSplitsDiscovered);
        }

        signalNoMoreSplitsIfDone();
    }

    private void onSplitsDiscovered(List<JdbcSourceSplit> splits, Throwable error) {
        asyncCallsPending.decrementAndGet();
        if (error != null) {
            LOG.error("Failed to discover splits.", error);
            preDiscoverSplits();
            return;
        }

        if (splits != null && !splits.isEmpty()) {
            assignOrBuffer(splits);
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
}
