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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.connector.jdbc.JdbcTestCheckpoint;
import org.apache.flink.connector.jdbc.templates.BooksTable;
import org.apache.flink.connector.jdbc.templates.BooksTable.BookEntry;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.connector.jdbc.xa.JdbcXaSinkTestBase.TEST_SINK_CONTEXT;

class JdbcXaSinkTestHelper implements AutoCloseable {

    private final JdbcXaSinkFunction<BookEntry> sink;
    private final XaSinkStateHandler state;
    private final BooksTable.BookEntry[] testData;

    JdbcXaSinkTestHelper(
            JdbcXaSinkFunction<BookEntry> sink,
            XaSinkStateHandler stateHandler,
            BooksTable.BookEntry[] testData) {
        this.sink = Preconditions.checkNotNull(sink);
        this.state = Preconditions.checkNotNull(stateHandler);
        this.testData = testData;
    }

    void emitAndCheckpoint(JdbcTestCheckpoint cp) throws Exception {
        emitAndSnapshot(cp);
        notifyCheckpointComplete(cp.id);
    }

    void emitAndSnapshot(JdbcTestCheckpoint checkpoint) throws Exception {
        emit(checkpoint);
        snapshotState(checkpoint.id);
    }

    void emit(JdbcTestCheckpoint checkpoint) throws java.io.IOException {
        for (int i = 0; i < checkpoint.dataItemsIdx.length; i++) {
            emit(testData[checkpoint.dataItemsIdx[i]]);
        }
    }

    void emit(BookEntry entry) throws java.io.IOException {
        sink.invoke(entry, TEST_SINK_CONTEXT);
    }

    @Override
    public void close() throws Exception {
        sink.close();
    }

    void notifyCheckpointComplete(long checkpointId) {
        sink.notifyCheckpointComplete(checkpointId);
    }

    void snapshotState(long id) throws Exception {
        sink.snapshotState(getSnapshotContext(id));
    }

    private static FunctionSnapshotContext getSnapshotContext(long id) {
        return new FunctionSnapshotContext() {
            @Override
            public long getCheckpointId() {
                return id;
            }

            @Override
            public long getCheckpointTimestamp() {
                return 0;
            }
        };
    }

    JdbcXaSinkFunction<BookEntry> getSinkFunction() {
        return sink;
    }

    XaSinkStateHandler getState() {
        return state;
    }
}
