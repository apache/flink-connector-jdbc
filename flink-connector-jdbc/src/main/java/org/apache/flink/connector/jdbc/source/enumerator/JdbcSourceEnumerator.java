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

package org.apache.flink.connector.jdbc.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
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

/** JDBC source enumerator. */
public class JdbcSourceEnumerator
        implements SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceEnumerator.class);

    private final SplitEnumeratorContext<JdbcSourceSplit> context;
    private final Boundedness boundedness;
    private final List<JdbcSourceSplit> unassigned;
    private final JdbcSqlSplitEnumeratorBase<JdbcSourceSplit> sqlSplitEnumerator;

    public JdbcSourceEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> context,
            JdbcSqlSplitEnumeratorBase<JdbcSourceSplit> sqlSplitEnumerator,
            List<JdbcSourceSplit> unassigned) {
        this.context = Preconditions.checkNotNull(context);
        this.sqlSplitEnumerator = Preconditions.checkNotNull(sqlSplitEnumerator);
        this.boundedness = Boundedness.BOUNDED;
        this.unassigned = Preconditions.checkNotNull(unassigned);
    }

    @Override
    public void start() {
        sqlSplitEnumerator.open();
        try {
            unassigned.addAll(sqlSplitEnumerator.enumerateSplits());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        sqlSplitEnumerator.close();
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public void handleSplitRequest(int subtask, @Nullable String hostname) {
        if (boundedness == Boundedness.BOUNDED) {
            assignSplitsForBounded(subtask, hostname);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        LOG.debug("File Source Enumerator adds splits back: {}", splits);
        unassigned.addAll(splits);
    }

    @Override
    public JdbcSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.debug("Source Checkpoint is {}", checkpointId);
        return new JdbcSourceEnumeratorState(
                Collections.emptyList(),
                Collections.emptyList(),
                new ArrayList<>(unassigned),
                sqlSplitEnumerator.optionalSqlSplitEnumeratorState);
    }

    private Optional<JdbcSourceSplit> getNextSplit() {
        if (unassigned == null || unassigned.isEmpty()) {
            return Optional.empty();
        }
        Iterator<JdbcSourceSplit> iterator = unassigned.iterator();
        JdbcSourceSplit next = null;
        if (iterator.hasNext()) {
            next = iterator.next();
            iterator.remove();
        }
        return Optional.ofNullable(next);
    }

    private void assignSplitsForBounded(int subtask, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtask)) {
            return;
        }
        if (LOG.isInfoEnabled()) {
            final String hostInfo =
                    hostname == null ? "(no host locality info)" : "(on host '" + hostname + "')";
            LOG.info("Subtask {} {} is requesting a Jdbc source split", subtask, hostInfo);
        }
        final Optional<JdbcSourceSplit> nextSplit = getNextSplit();
        if (nextSplit.isPresent()) {
            final JdbcSourceSplit split = nextSplit.get();
            context.assignSplit(split, subtask);
            LOG.info("Assigned split to subtask {} : {}", subtask, split);
        } else {
            context.signalNoMoreSplits(subtask);
            LOG.info("No more splits available for subtask {}", subtask);
        }
    }
}
