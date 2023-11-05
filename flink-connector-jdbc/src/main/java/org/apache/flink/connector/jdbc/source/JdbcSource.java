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

package org.apache.flink.connector.jdbc.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.source.enumerator.JdbcSourceEnumStateSerializer;
import org.apache.flink.connector.jdbc.source.enumerator.JdbcSourceEnumerator;
import org.apache.flink.connector.jdbc.source.enumerator.JdbcSourceEnumeratorState;
import org.apache.flink.connector.jdbc.source.enumerator.JdbcSqlSplitEnumeratorBase;
import org.apache.flink.connector.jdbc.source.reader.JdbcSourceReader;
import org.apache.flink.connector.jdbc.source.reader.JdbcSourceSplitReader;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;

/** JDBC source. */
@PublicEvolving
public class JdbcSource<OUT>
        implements Source<OUT, JdbcSourceSplit, JdbcSourceEnumeratorState>,
                ResultTypeQueryable<OUT> {

    private final Boundedness boundedness;
    private final TypeInformation<OUT> typeInformation;

    private final Configuration configuration;
    private final JdbcSqlSplitEnumeratorBase.Provider<JdbcSourceSplit> sqlSplitEnumeratorProvider;

    protected JdbcConnectionProvider connectionProvider;
    private final ResultExtractor<OUT> resultExtractor;
    private final DeliveryGuarantee deliveryGuarantee;

    JdbcSource(
            Configuration configuration,
            JdbcConnectionProvider connectionProvider,
            JdbcSqlSplitEnumeratorBase.Provider<JdbcSourceSplit> sqlSplitEnumeratorProvider,
            ResultExtractor<OUT> resultExtractor,
            TypeInformation<OUT> typeInformation,
            DeliveryGuarantee deliveryGuarantee) {
        this.configuration = Preconditions.checkNotNull(configuration);
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.sqlSplitEnumeratorProvider = Preconditions.checkNotNull(sqlSplitEnumeratorProvider);
        this.resultExtractor = Preconditions.checkNotNull(resultExtractor);
        this.deliveryGuarantee = Preconditions.checkNotNull(deliveryGuarantee);
        this.typeInformation = Preconditions.checkNotNull(typeInformation);
        this.boundedness = Boundedness.BOUNDED;
    }

    JdbcSource(
            Configuration configuration,
            JdbcConnectionProvider connectionProvider,
            JdbcSqlSplitEnumeratorBase.Provider<JdbcSourceSplit> sqlSplitEnumeratorProvider,
            ResultExtractor<OUT> resultExtractor,
            TypeInformation<OUT> typeInformation) {
        this(
                configuration,
                connectionProvider,
                sqlSplitEnumeratorProvider,
                resultExtractor,
                typeInformation,
                DeliveryGuarantee.NONE);
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<OUT, JdbcSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new JdbcSourceReader<>(
                () ->
                        new JdbcSourceSplitReader<>(
                                readerContext,
                                configuration,
                                typeInformation,
                                connectionProvider,
                                deliveryGuarantee,
                                resultExtractor),
                configuration,
                readerContext);
    }

    @Override
    public SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> enumContext) throws Exception {
        return new JdbcSourceEnumerator(
                enumContext, sqlSplitEnumeratorProvider.create(), new ArrayList<>());
    }

    @Override
    public SplitEnumerator<JdbcSourceSplit, JdbcSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> enumContext,
            JdbcSourceEnumeratorState checkpoint)
            throws Exception {
        Serializable optionalUserDefinedSplitEnumeratorState =
                checkpoint.getOptionalUserDefinedSplitEnumeratorState();
        return new JdbcSourceEnumerator(
                enumContext,
                sqlSplitEnumeratorProvider.restore(optionalUserDefinedSplitEnumeratorState),
                checkpoint.getRemainingSplits());
    }

    @Override
    public SimpleVersionedSerializer<JdbcSourceSplit> getSplitSerializer() {
        return new JdbcSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<JdbcSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new JdbcSourceEnumStateSerializer((JdbcSourceSplitSerializer) getSplitSerializer());
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return typeInformation;
    }

    public static <OUT> JdbcSourceBuilder<OUT> builder() {
        return new JdbcSourceBuilder<>();
    }

    // ---- Visible for testing methods. ---

    @VisibleForTesting
    public JdbcSqlSplitEnumeratorBase.Provider<JdbcSourceSplit> getSqlSplitEnumeratorProvider() {
        return sqlSplitEnumeratorProvider;
    }

    @VisibleForTesting
    public TypeInformation<OUT> getTypeInformation() {
        return typeInformation;
    }

    @VisibleForTesting
    public Configuration getConfiguration() {
        return configuration;
    }

    @VisibleForTesting
    public ResultExtractor<OUT> getResultExtractor() {
        return resultExtractor;
    }

    @VisibleForTesting
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcSource<?> that = (JdbcSource<?>) o;
        return boundedness == that.boundedness
                && Objects.equals(typeInformation, that.typeInformation)
                && Objects.equals(configuration, that.configuration)
                && Objects.equals(sqlSplitEnumeratorProvider, that.sqlSplitEnumeratorProvider)
                && Objects.equals(connectionProvider, that.connectionProvider)
                && Objects.equals(resultExtractor, that.resultExtractor)
                && deliveryGuarantee == that.deliveryGuarantee;
    }
}
