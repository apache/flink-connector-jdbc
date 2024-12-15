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

package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.lineage.LineageUtils;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/** A generic SinkFunction for JDBC. */
@Internal
public class GenericJdbcSinkFunction<T> extends RichSinkFunction<T>
        implements LineageVertexProvider, CheckpointedFunction, InputTypeConfigurable {
    private final JdbcOutputFormat<T, ?, ?> outputFormat;
    private JdbcOutputSerializer<T> serializer;

    public GenericJdbcSinkFunction(@Nonnull JdbcOutputFormat<T, ?, ?> outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Recheck if execution config change
        serializer.withObjectReuseEnabled(
                getRuntimeContext().getExecutionConfig().isObjectReuseEnabled());
        outputFormat.open(serializer);
    }

    @Override
    public void invoke(T value, Context context) throws IOException {
        outputFormat.writeRecord(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.flush();
    }

    @Override
    public void close() {
        outputFormat.close();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        this.serializer =
                JdbcOutputSerializer.of(
                        ((TypeInformation<T>) type).createSerializer(executionConfig));
    }

    @Override
    public LineageVertex getLineageVertex() {
        Optional<String> nameOpt = LineageUtils.nameOf(outputFormat.toString());
        String namespace = LineageUtils.namespaceOf(outputFormat.connectionProvider);
        LineageDataset dataset =
                LineageUtils.datasetOf(nameOpt.orElse(""), namespace, Collections.emptyList());
        return LineageUtils.lineageVertexOf(Collections.singleton(dataset));
    }
}
