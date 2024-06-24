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

package org.apache.flink.connector.jdbc.source.reader;

import org.apache.flink.connector.jdbc.source.split.CheckpointedOffset;

/**
 * Util class to represent the record with the corresponding information.
 *
 * @param <E> The type of the record.
 */
public class RecordAndOffset<E> {

    public static final long NO_OFFSET = CheckpointedOffset.NO_OFFSET;

    final E record;
    // The two fields are not enabled for using now.
    final long offset;
    final long recordSkipCount;

    public RecordAndOffset(E record, long offset, long recordSkipCount) {
        this.record = record;
        this.offset = offset;
        this.recordSkipCount = recordSkipCount;
    }

    // ------------------------------------------------------------------------

    public E getRecord() {
        return record;
    }

    public long getOffset() {
        return offset;
    }

    public long getRecordSkipCount() {
        return recordSkipCount;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format("%s @ %d", record, offset);
    }
}
