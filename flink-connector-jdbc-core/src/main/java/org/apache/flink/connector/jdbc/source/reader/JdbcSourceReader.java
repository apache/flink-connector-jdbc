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

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplitState;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The JDBC source reader.
 *
 * @param <OUT> The type of the record readed from the source.
 */
public class JdbcSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                RecordAndOffset<OUT>, OUT, JdbcSourceSplit, JdbcSourceSplitState<JdbcSourceSplit>> {

    public JdbcSourceReader(
            Supplier<SplitReader<RecordAndOffset<OUT>, JdbcSourceSplit>> splitReaderSupplier,
            Configuration config,
            SourceReaderContext context) {
        super(splitReaderSupplier, new JdbcRecordEmitter<>(), config, context);
    }

    @Override
    protected void onSplitFinished(
            Map<String, JdbcSourceSplitState<JdbcSourceSplit>> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected JdbcSourceSplitState<JdbcSourceSplit> initializedState(JdbcSourceSplit split) {
        return new JdbcSourceSplitState<>(split);
    }

    @Override
    protected JdbcSourceSplit toSplitType(
            String splitId, JdbcSourceSplitState<JdbcSourceSplit> splitState) {
        return splitState.toJdbcSourceSplit();
    }
}
