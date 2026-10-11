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
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableBounds;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Checkpointed progress of a single {@link TableSplitterEnumerator}: where the chunk computation
 * cursor stands, plus the computed-but-not-yet-emitted bounds. A restored splitter resumes from
 * this progress instead of re-emitting every split.
 */
@Internal
final class TableSplitProgress implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;
    private final boolean boundsInitialized;
    private final boolean finished;
    @Nullable private final TableBounds tableMinMax;
    @Nullable private final Object currentLowerBound;
    private final List<TableBounds> pendingBounds;

    TableSplitProgress(
            TableId tableId,
            boolean boundsInitialized,
            boolean finished,
            @Nullable TableBounds tableMinMax,
            @Nullable Object currentLowerBound,
            List<TableBounds> pendingBounds) {
        this.tableId = tableId;
        this.boundsInitialized = boundsInitialized;
        this.finished = finished;
        this.tableMinMax = tableMinMax;
        this.currentLowerBound = currentLowerBound;
        this.pendingBounds = Collections.unmodifiableList(new ArrayList<>(pendingBounds));
    }

    TableId tableId() {
        return tableId;
    }

    boolean boundsInitialized() {
        return boundsInitialized;
    }

    boolean finished() {
        return finished;
    }

    @Nullable
    TableBounds tableMinMax() {
        return tableMinMax;
    }

    @Nullable
    Object currentLowerBound() {
        return currentLowerBound;
    }

    List<TableBounds> pendingBounds() {
        return pendingBounds;
    }
}
