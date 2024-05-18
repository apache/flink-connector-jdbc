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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplitState;

/**
 * The JDBC resorce emitter.
 *
 * @param <T> The type of the record.
 * @param <SplitT> The type of JDBC split.
 */
public class JdbcRecordEmitter<T, SplitT extends JdbcSourceSplit>
        implements RecordEmitter<RecordAndOffset<T>, T, JdbcSourceSplitState<SplitT>> {

    @Override
    public void emitRecord(
            RecordAndOffset<T> element,
            SourceOutput<T> output,
            JdbcSourceSplitState<SplitT> splitState)
            throws Exception {

        output.collect(element.getRecord());
        splitState.setPosition(element.getOffset(), element.getRecordSkipCount());
    }
}
