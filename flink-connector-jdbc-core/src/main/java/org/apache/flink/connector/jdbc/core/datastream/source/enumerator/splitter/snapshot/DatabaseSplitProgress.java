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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Checkpointed progress of a {@link DatabaseSplitterEnumerator}: the per-table progress of every
 * table splitter that had started, plus the computed-but-not-yet-emitted splits. A restored
 * enumerator resumes each table from its own progress instead of re-emitting every split.
 */
@Internal
final class DatabaseSplitProgress implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<TableSplitProgress> tableProgresses;
    private final List<JdbcSourceSplit> pendingSplits;

    DatabaseSplitProgress(
            List<TableSplitProgress> tableProgresses, List<JdbcSourceSplit> pendingSplits) {
        this.tableProgresses = Collections.unmodifiableList(new ArrayList<>(tableProgresses));
        this.pendingSplits = Collections.unmodifiableList(new ArrayList<>(pendingSplits));
    }

    List<TableSplitProgress> tableProgresses() {
        return tableProgresses;
    }

    List<JdbcSourceSplit> pendingSplits() {
        return pendingSplits;
    }
}
