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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Objects;

/**
 * The class to hold the offset for checkpointed state. Note: The current class is not enabled for
 * using.
 */
@PublicEvolving
public class CheckpointedOffset implements Serializable {

    public static final int NO_OFFSET = 0;
    private final long offset;
    private final long recordsAfterOffset;

    public CheckpointedOffset(long offset, long recordsAfterOffset) {
        this.offset = offset;
        this.recordsAfterOffset = recordsAfterOffset;
    }

    public CheckpointedOffset() {
        this(0, 0);
    }

    public long getOffset() {
        return offset;
    }

    public long getRecordsAfterOffset() {
        return recordsAfterOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointedOffset that = (CheckpointedOffset) o;
        return offset == that.offset && recordsAfterOffset == that.recordsAfterOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, recordsAfterOffset);
    }

    @Override
    public String toString() {
        return "CheckpointedOffset{"
                + "offset="
                + offset
                + ", recordsAfterOffset="
                + recordsAfterOffset
                + '}';
    }
}
