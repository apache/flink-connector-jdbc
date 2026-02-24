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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** A split enumerator based on sql-parameters grains. */
@PublicEvolving
public class PreparedSplitterEnumerator extends SqlSplitterEnumerator {
    public static final Logger LOG = LoggerFactory.getLogger(PreparedSplitterEnumerator.class);

    private final Serializable[][] sqlParameters;
    private boolean finished;

    protected PreparedSplitterEnumerator(String sqlTemplate, Serializable[][] sqlParameters) {
        super(Preconditions.checkNotNull(sqlTemplate));
        this.sqlParameters = Preconditions.checkNotNull(sqlParameters);
        this.finished = false;
    }

    public static PreparedSplitterEnumerator of(
            String sqlTemplate, Serializable[][] sqlParameters) {
        return new PreparedSplitterEnumerator(sqlTemplate, sqlParameters);
    }

    public static PreparedSplitterEnumerator of(String sqlTemplate) {
        return new PreparedSplitterEnumerator(sqlTemplate, new Serializable[0][]);
    }

    public static PreparedSplitterEnumerator of(
            String sqlTemplate, long minValue, long maxValue, long batchSize) {
        PreparedSplitterNumericParameters parameters =
                new PreparedSplitterNumericParameters(minValue, maxValue).withBatchSize(batchSize);
        return of(sqlTemplate, parameters);
    }

    public static PreparedSplitterEnumerator of(
            String sqlTemplate, long minValue, long maxValue, int batchNum) {
        PreparedSplitterNumericParameters parameters =
                new PreparedSplitterNumericParameters(minValue, maxValue).withBatchNum(batchNum);
        return of(sqlTemplate, parameters);
    }

    public static PreparedSplitterEnumerator of(
            String sqlTemplate, PreparedSplitterNumericParameters numericParameters) {
        Serializable[][] parameters = numericParameters.getParameterValues();
        return new PreparedSplitterEnumerator(sqlTemplate, parameters);
    }

    @Override
    public void start(JdbcConnectionProvider connectionProvider) {}

    @Override
    public void close() {}

    @Override
    public boolean isAllSplitsFinished() {
        return this.finished;
    }

    @VisibleForTesting
    public Serializable[][] getSqlParameters() {
        return sqlParameters;
    }

    @Override
    public List<JdbcSourceSplit> enumerateSplits() {
        List<JdbcSourceSplit> splitList = super.enumerateSplits();
        this.finished = true;
        return splitList;
    }

    @Override
    public @Nullable Serializable serializableState() {
        return null;
    }

    @Override
    public PreparedSplitterEnumerator restoreState(Serializable state) {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PreparedSplitterEnumerator)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PreparedSplitterEnumerator that = (PreparedSplitterEnumerator) o;
        return Objects.deepEquals(sqlParameters, that.sqlParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.deepHashCode(sqlParameters));
    }
}
