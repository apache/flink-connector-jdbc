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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.datastream.source.split.CheckpointedOffset;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/** A split enumerator based on sql-parameters grains. */
public final class SqlTemplateSplitEnumerator extends JdbcSqlSplitEnumeratorBase<JdbcSourceSplit> {

    public static final Logger LOG = LoggerFactory.getLogger(SqlTemplateSplitEnumerator.class);

    private final String sqlTemplate;

    private final JdbcParameterValuesProvider parameterValuesProvider;

    public SqlTemplateSplitEnumerator(
            Serializable userDefinedState,
            String sqlTemplate,
            @Nullable JdbcParameterValuesProvider parametersProvider) {
        super(userDefinedState);
        this.sqlTemplate = Preconditions.checkNotNull(sqlTemplate);
        this.parameterValuesProvider = parametersProvider;
    }

    @Override
    @Nonnull
    public List<JdbcSourceSplit> enumerateSplits(@Nonnull Supplier<Boolean> splitGettable)
            throws RuntimeException {

        if (!splitGettable.get()) {
            LOG.info(
                    "The current split is over max splits capacity of {}.",
                    JdbcSourceEnumerator.class.getSimpleName());
            return Collections.emptyList();
        }

        if (parameterValuesProvider == null) {
            return Collections.singletonList(
                    new JdbcSourceSplit(getNextId(), sqlTemplate, null, new CheckpointedOffset()));
        }

        if (optionalSqlSplitEnumeratorState != null) {
            parameterValuesProvider.setOptionalState(optionalSqlSplitEnumeratorState);
        }
        Serializable[][] parameters = parameterValuesProvider.getParameterValues();

        // update state
        optionalSqlSplitEnumeratorState = parameterValuesProvider.getLatestOptionalState();

        if (parameters == null) {
            return Collections.singletonList(
                    new JdbcSourceSplit(getNextId(), sqlTemplate, null, new CheckpointedOffset()));
        }

        List<JdbcSourceSplit> jdbcSourceSplitList = new ArrayList<>(parameters.length);
        for (Serializable[] paramArr : parameters) {
            jdbcSourceSplitList.add(
                    new JdbcSourceSplit(
                            getNextId(), sqlTemplate, paramArr, new CheckpointedOffset()));
        }
        return jdbcSourceSplitList;
    }

    @VisibleForTesting
    public String getSqlTemplate() {
        return sqlTemplate;
    }

    @VisibleForTesting
    public JdbcParameterValuesProvider getParameterValuesProvider() {
        return parameterValuesProvider;
    }

    /** The {@link TemplateSqlSplitEnumeratorProvider} for {@link SqlTemplateSplitEnumerator}. */
    public static class TemplateSqlSplitEnumeratorProvider
            implements JdbcSqlSplitEnumeratorBase.Provider<JdbcSourceSplit> {

        private String sqlTemplate;

        private @Nullable Serializable optionalSqlSplitEnumeratorState;

        private @Nullable JdbcParameterValuesProvider parameterValuesProvider;

        public TemplateSqlSplitEnumeratorProvider setSqlTemplate(String sqlTemplate) {
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(sqlTemplate),
                    "sqlTemplate must not be empty.");
            this.sqlTemplate = sqlTemplate;
            return this;
        }

        public TemplateSqlSplitEnumeratorProvider setParameterValuesProvider(
                @Nullable JdbcParameterValuesProvider parameterValuesProvider) {
            this.parameterValuesProvider = parameterValuesProvider;
            return this;
        }

        public TemplateSqlSplitEnumeratorProvider setOptionalSqlSplitEnumeratorState(
                Serializable optionalSqlSplitEnumeratorState) {
            this.optionalSqlSplitEnumeratorState = optionalSqlSplitEnumeratorState;
            return this;
        }

        @Override
        public JdbcSqlSplitEnumeratorBase<JdbcSourceSplit> create() {
            return new SqlTemplateSplitEnumerator(
                    this.optionalSqlSplitEnumeratorState, sqlTemplate, parameterValuesProvider);
        }

        @Override
        public JdbcSqlSplitEnumeratorBase<JdbcSourceSplit> restore(
                @Nullable Serializable optionalSqlSplitEnumeratorState) {
            return new SqlTemplateSplitEnumerator(
                    optionalSqlSplitEnumeratorState, sqlTemplate, parameterValuesProvider);
        }
    }
}
