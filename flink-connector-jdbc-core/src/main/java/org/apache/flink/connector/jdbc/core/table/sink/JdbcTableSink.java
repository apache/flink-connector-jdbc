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

package org.apache.flink.connector.jdbc.core.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.jdbc.core.datastream.sink.writer.JdbcWriter;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Sink V2 behind {@link JdbcDynamicTableSink}: an at-least-once {@link Sink} that runs a {@link
 * JdbcWriter} over a {@link JdbcOutputFormat} the {@link JdbcOutputFormatBuilder} builds for every
 * writer.
 *
 * <p>It implements neither {@code SupportsCommitter} nor {@code SupportsWriterState}: the table
 * path has nothing to commit and no state to keep.
 */
@Internal
public final class JdbcTableSink implements Sink<RowData>, LineageVertexProvider {

    private static final long serialVersionUID = 1L;

    private final JdbcOutputFormatBuilder outputFormatBuilder;

    JdbcTableSink(JdbcOutputFormatBuilder outputFormatBuilder) {
        this.outputFormatBuilder =
                checkNotNull(outputFormatBuilder, "outputFormatBuilder must be defined");
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        return new JdbcWriter<>(
                outputFormatBuilder.build(), JdbcOutputSerializer.of(context), context);
    }

    @Override
    public LineageVertex getLineageVertex() {
        return outputFormatBuilder.build().getLineageVertex();
    }
}
