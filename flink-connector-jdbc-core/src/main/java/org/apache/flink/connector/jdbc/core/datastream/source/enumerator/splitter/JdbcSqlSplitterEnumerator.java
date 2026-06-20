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
import org.apache.flink.connector.jdbc.core.datastream.source.config.ContinuousUnBoundingSettings;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.JdbcSqlSplitEnumeratorBase;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.SqlTemplateSplitEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A splitter enumerator for JdbcSqlSplitEnumeratorBase.
 *
 * <p>This class is used to allow the retrofit of existing implementations of
 * JdbcSqlSplitEnumeratorBase to the new SplitterEnumerator interface. It delegates the split
 * enumeration to the underlying JdbcSqlSplitEnumeratorBase instance.
 */
@Deprecated
@Internal
public class JdbcSqlSplitterEnumerator implements SplitterEnumerator {
    private final JdbcSqlSplitEnumeratorBase.Provider<?> provider;
    private final Boundedness boundedness;
    private final @Nullable ContinuousUnBoundingSettings continuousUnBoundingSettings;
    private JdbcSqlSplitEnumeratorBase<?> base;
    private volatile boolean finished;
    private volatile boolean initialDelayApplied;

    public JdbcSqlSplitterEnumerator(
            JdbcSqlSplitEnumeratorBase.Provider<?> provider,
            @Nullable ContinuousUnBoundingSettings continuousUnBoundingSettings) {
        this.provider = provider;
        this.base = provider.create();
        this.finished = false;
        this.boundedness =
                Objects.nonNull(continuousUnBoundingSettings)
                        ? Boundedness.CONTINUOUS_UNBOUNDED
                        : Boundedness.BOUNDED;
        this.continuousUnBoundingSettings = continuousUnBoundingSettings;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public void start(JdbcConnectionProvider connectionProvider) {
        base.open();
    }

    @Override
    public void close() {
        base.close();
    }

    @Override
    public synchronized boolean isAllSplitsFinished() {
        if (boundedness == Boundedness.CONTINUOUS_UNBOUNDED) {
            return false;
        }
        return this.finished;
    }

    @Override
    public synchronized List<JdbcSourceSplit> enumerateSplits() {
        try {
            if (finished) {
                return new ArrayList<>();
            }
            if (boundedness == Boundedness.CONTINUOUS_UNBOUNDED && !initialDelayApplied) {
                initialDelayApplied = true;
                Duration delayMs = continuousUnBoundingSettings.getInitialDiscoveryDelay();
                if (delayMs != null) {
                    Thread.sleep(delayMs.toMillis());
                }
            }
            List<JdbcSourceSplit> splits = base.enumerateSplits(() -> true);
            if (boundedness == Boundedness.BOUNDED) {
                this.finished = true;
            }
            if (boundedness == Boundedness.CONTINUOUS_UNBOUNDED && splits.isEmpty()) {
                Duration throttleMs = continuousUnBoundingSettings.getDiscoveryInterval();
                if (throttleMs != null) {
                    Thread.sleep(throttleMs.toMillis());
                }
            }
            return splits;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ArrayList<>();
        } catch (Exception e) {
            throw new RuntimeException("Error enumerating splits", e);
        }
    }

    @Override
    public List<String> lineageQueries() {
        List<String> queries = new ArrayList<>();
        if (base instanceof SqlTemplateSplitEnumerator) {
            queries.add(((SqlTemplateSplitEnumerator) base).getSqlTemplate());
        }
        return queries;
    }

    @Override
    public Serializable serializableState() {
        return base.enumeratorState();
    }

    @Override
    public SplitterEnumerator restoreState(Serializable state) {
        this.base = provider.restore(state);
        return this;
    }
}
