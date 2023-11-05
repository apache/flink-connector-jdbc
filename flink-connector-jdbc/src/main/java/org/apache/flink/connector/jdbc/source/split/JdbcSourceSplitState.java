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

package org.apache.flink.connector.jdbc.source.split;

import org.apache.flink.util.FlinkRuntimeException;

import java.io.Serializable;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The class is hold the state information of {@link JdbcSourceSplit}. */
public class JdbcSourceSplitState<SplitT extends JdbcSourceSplit> implements Serializable {

    private final SplitT split;

    // The two fields are not enabled for using now.
    private Long offset;
    private Long recordsToSkipAfterOffset;

    public JdbcSourceSplitState(SplitT split) {
        this.split = checkNotNull(split);

        final Optional<CheckpointedOffset> readerPosition = split.getReaderPosition();
        if (readerPosition.isPresent()) {
            this.offset = readerPosition.get().getOffset();
            this.recordsToSkipAfterOffset = readerPosition.get().getRecordsAfterOffset();
        } else {
            this.offset = null;
            this.recordsToSkipAfterOffset = null;
        }
    }

    public long getOffset() {
        return offset;
    }

    public long getRecordsToSkipAfterOffset() {
        return recordsToSkipAfterOffset;
    }

    public void setOffset(long offset) {
        // we skip sanity / boundary checks here for efficiency.
        // illegal boundaries will eventually be caught when constructing the split on checkpoint.
        this.offset = offset;
    }

    public void setRecordsToSkipAfterOffset(long recordsToSkipAfterOffset) {
        // we skip sanity / boundary checks here for efficiency.
        // illegal boundaries will eventually be caught when constructing the split on checkpoint.
        this.recordsToSkipAfterOffset = recordsToSkipAfterOffset;
    }

    public void setPosition(long offset, long recordsToSkipAfterOffset) {
        // we skip sanity / boundary checks here for efficiency.
        // illegal boundaries will eventually be caught when constructing the split on checkpoint.
        this.offset = offset;
        this.recordsToSkipAfterOffset = recordsToSkipAfterOffset;
    }

    public void setPosition(CheckpointedOffset offset) {
        this.offset = offset.getOffset();
        this.recordsToSkipAfterOffset = offset.getRecordsAfterOffset();
    }

    /** Use the current row count as the starting row count to create a new FileSourceSplit. */
    @SuppressWarnings("unchecked")
    public SplitT toJdbcSourceSplit() {
        final CheckpointedOffset position =
                (offset == null && recordsToSkipAfterOffset == 0)
                        ? null
                        : new CheckpointedOffset(offset, recordsToSkipAfterOffset);

        final JdbcSourceSplit updatedSplit = split.updateWithCheckpointedPosition(position);

        // some sanity checks to avoid surprises and not accidentally lose split information
        if (updatedSplit == null) {
            throw new FlinkRuntimeException(
                    "Split returned 'null' in updateWithCheckpointedPosition(): " + split);
        }
        if (updatedSplit.getClass() != split.getClass()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Split returned different type in updateWithCheckpointedPosition(). "
                                    + "Split type is %s, returned type is %s",
                            split.getClass().getName(), updatedSplit.getClass().getName()));
        }

        return (SplitT) updatedSplit;
    }
}
