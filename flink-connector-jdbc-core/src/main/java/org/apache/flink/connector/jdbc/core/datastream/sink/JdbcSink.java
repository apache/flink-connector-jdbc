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

package org.apache.flink.connector.jdbc.core.datastream.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.committer.JdbcCommitable;
import org.apache.flink.connector.jdbc.core.datastream.sink.committer.JdbcCommitableSerializer;
import org.apache.flink.connector.jdbc.core.datastream.sink.committer.JdbcCommitter;
import org.apache.flink.connector.jdbc.core.datastream.sink.writer.JdbcWriter;
import org.apache.flink.connector.jdbc.core.datastream.sink.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.core.datastream.sink.writer.JdbcWriterStateSerializer;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.apache.flink.connector.jdbc.lineage.LineageUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Flink Sink to produce data into a jdbc database.
 *
 * @see JdbcSinkBuilder on how to construct a JdbcSink
 */
@PublicEvolving
public class JdbcSink<IN>
        implements LineageVertexProvider,
                StatefulSink<IN, JdbcWriterState>,
                TwoPhaseCommittingSink<IN, JdbcCommitable> {

    private final DeliveryGuarantee deliveryGuarantee;
    private final JdbcConnectionProvider connectionProvider;
    private final JdbcExecutionOptions executionOptions;
    private final JdbcExactlyOnceOptions exactlyOnceOptions;
    private final JdbcQueryStatement<IN> queryStatement;

    public JdbcSink(
            DeliveryGuarantee deliveryGuarantee,
            JdbcConnectionProvider connectionProvider,
            JdbcExecutionOptions executionOptions,
            JdbcExactlyOnceOptions exactlyOnceOptions,
            JdbcQueryStatement<IN> queryStatement) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.connectionProvider = connectionProvider;
        this.executionOptions = executionOptions;
        this.exactlyOnceOptions = exactlyOnceOptions;
        this.queryStatement = queryStatement;
    }

    public static <IN> JdbcSinkBuilder<IN> builder() {
        return new JdbcSinkBuilder<>();
    }

    @Override
    @Internal
    public JdbcWriter<IN> createWriter(InitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    @Internal
    public JdbcWriter<IN> restoreWriter(
            InitContext context, Collection<JdbcWriterState> recoveredState) throws IOException {
        JdbcOutputSerializer<IN> outputSerializer =
                JdbcOutputSerializer.of(
                        context.createInputSerializer(), context.isObjectReuseEnabled());
        return new JdbcWriter<>(
                this.connectionProvider,
                this.executionOptions,
                this.exactlyOnceOptions,
                this.queryStatement,
                outputSerializer,
                deliveryGuarantee,
                recoveredState,
                context);
    }

    @Override
    @Internal
    public Committer<JdbcCommitable> createCommitter() throws IOException {
        return new JdbcCommitter(deliveryGuarantee, connectionProvider, exactlyOnceOptions);
    }

    @Override
    @Internal
    public SimpleVersionedSerializer<JdbcCommitable> getCommittableSerializer() {
        return new JdbcCommitableSerializer();
    }

    @Override
    @Internal
    public SimpleVersionedSerializer<JdbcWriterState> getWriterStateSerializer() {
        return new JdbcWriterStateSerializer();
    }

    @Override
    public LineageVertex getLineageVertex() {
        Optional<String> nameOpt = LineageUtils.nameOf(queryStatement.query());
        String namespace = LineageUtils.namespaceOf(connectionProvider);
        LineageDataset dataset =
                LineageUtils.datasetOf(nameOpt.orElse(""), namespace, Collections.emptyList());
        return LineageUtils.lineageVertexOf(Collections.singleton(dataset));
    }
}
