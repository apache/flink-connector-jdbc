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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.datastream.source.split.CheckpointedOffset;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** A sql base template split enumerator. */
public abstract class SqlSplitterEnumerator implements SplitterEnumerator {
    public static final Logger LOG = LoggerFactory.getLogger(SqlSplitterEnumerator.class);

    private final char[] currentId = "0000000000".toCharArray();

    private final String sqlTemplate;

    protected SqlSplitterEnumerator(String sqlTemplate) {
        this.sqlTemplate = Preconditions.checkNotNull(sqlTemplate);
    }

    @VisibleForTesting
    protected abstract Serializable[][] getSqlParameters();

    protected String getSqlTemplate() {
        return this.sqlTemplate;
    }

    @Override
    public List<JdbcSourceSplit> enumerateSplits() {
        Serializable[][] params = getSqlParameters();
        int paramLength = params.length;
        List<JdbcSourceSplit> splitList = new ArrayList<>(paramLength);
        if (paramLength == 0) {
            splitList.add(createSplit(null));
        } else {
            for (Serializable[] paramArr : params) {
                splitList.add(createSplit(paramArr));
            }
        }
        return splitList;
    }

    @Override
    public List<String> lineageQueries() {
        return Collections.singletonList(getSqlTemplate());
    }

    protected JdbcSourceSplit createSplit(Serializable[] paramArr) {
        return new JdbcSourceSplit(
                getNextId(), getSqlTemplate(), paramArr, new CheckpointedOffset());
    }

    protected final String getNextId() {
        // because we just increment numbers, we increment the char representation directly,
        // rather than incrementing an integer and converting it to a string representation
        // every time again (requires quite some expensive conversion logic).
        incrementCharArrayByOne(currentId, currentId.length - 1);
        return new String(currentId);
    }

    private static void incrementCharArrayByOne(char[] array, int pos) {
        char c = array[pos];
        c++;

        if (c > '9') {
            c = '0';
            incrementCharArrayByOne(array, pos - 1);
        }
        array[pos] = c;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SqlSplitterEnumerator)) {
            return false;
        }
        SqlSplitterEnumerator that = (SqlSplitterEnumerator) o;
        return Objects.equals(sqlTemplate, that.sqlTemplate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sqlTemplate);
    }
}
