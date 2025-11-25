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

import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/** Interface for jdbc sql split enumerator. */
public interface SplitterEnumerator extends AutoCloseable, Serializable {

    void start(JdbcConnectionProvider connectionProvider);

    void close();

    boolean hasFinishSplits();

    List<JdbcSourceSplit> enumerateSplits();

    default List<JdbcSourceSplit> enumerateSplits(@Nonnull Supplier<Boolean> splitGettable) {
        return enumerateSplits(splitGettable.get());
    }

    default List<JdbcSourceSplit> enumerateSplits(Boolean splitGettable) {
        if (!splitGettable) {
            return Collections.emptyList();
        }
        return enumerateSplits();
    }

    List<String> lineageQueries();

    Serializable serializableState();

    SplitterEnumerator restoreState(Serializable state);
}
