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

package org.apache.flink.connector.jdbc.source.enumerator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A split enumerator based on sql-parameters grains. */
public final class SqlTemplateSplitEnumerator extends JdbcSqlSplitEnumeratorBase<JdbcSourceSplit> {

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
    public List<JdbcSourceSplit> enumerateSplits() throws IOException {
        if (parameterValuesProvider == null) {
            return Collections.singletonList(
                    new JdbcSourceSplit(getNextId(), sqlTemplate, null, 0, null));
        }
        Serializable[][] parameters = parameterValuesProvider.getParameterValues();
        if (parameters == null) {
            return Collections.singletonList(
                    new JdbcSourceSplit(getNextId(), sqlTemplate, null, 0, null));
        }

        List<JdbcSourceSplit> jdbcSourceSplitList = new ArrayList<>(parameters.length);
        for (Serializable[] paramArr : parameters) {
            jdbcSourceSplitList.add(
                    new JdbcSourceSplit(getNextId(), sqlTemplate, paramArr, 0, null));
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
