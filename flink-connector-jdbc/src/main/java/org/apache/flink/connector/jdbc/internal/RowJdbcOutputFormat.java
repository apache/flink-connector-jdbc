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
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;

import java.util.HashMap;

import static org.apache.flink.connector.jdbc.utils.JdbcUtils.setRecordToStatement;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A JDBC Row outputFormat that supports batching records before writing records to database. */
@Internal
public class RowJdbcOutputFormat<In>
        extends JdbcOutputFormat<In, Row, JdbcBatchStatementExecutor<Row>> {

    public RowJdbcOutputFormat(
            @Nonnull JdbcConnectionProvider connectionProvider,
            @Nonnull JdbcExecutionOptions executionOptions,
            @Nonnull
                    StatementExecutorFactory<JdbcBatchStatementExecutor<Row>>
                            statementExecutorFactory) {
        super(connectionProvider, executionOptions, statementExecutorFactory);
    }

    static JdbcBatchStatementExecutor<Row> createSimpleRowExecutor(String sql, int[] fieldTypes) {
        return JdbcBatchStatementExecutor.simple(sql, createRowJdbcStatementBuilder(fieldTypes));
    }

    /**
     * Creates a {@link JdbcStatementBuilder} for {@link Row} using the provided SQL types array.
     * Uses {@link JdbcUtils#setRecordToStatement}
     */
    static JdbcStatementBuilder<Row> createRowJdbcStatementBuilder(int[] types) {
        return (st, record) -> setRecordToStatement(st, types, record);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for a {@link JdbcOutputFormat} using Row. */
    public static class Builder {
        private InternalJdbcConnectionOptions options;

        private JdbcDmlOptions.JdbcDmlOptionsBuilder dmlOptionsBuilder = JdbcDmlOptions.builder();
        private JdbcExecutionOptions.Builder executionOptionsBuilder =
                JdbcExecutionOptions.builder();

        /** required, jdbc options. */
        public Builder setOptions(InternalJdbcConnectionOptions options) {
            this.options = options;
            this.dmlOptionsBuilder
                    .withTableName(options.getTableName())
                    .withDialect(options.getDialect());
            return this;
        }

        /** required, field names of this jdbc sink. */
        public Builder setFieldNames(String[] fieldNames) {
            this.dmlOptionsBuilder.withFieldNames(fieldNames);
            return this;
        }

        /** required, upsert unique keys. */
        public Builder setKeyFields(String[] keyFields) {
            this.dmlOptionsBuilder.withKeyFields(keyFields);
            return this;
        }

        /** required, field types of this jdbc sink. */
        public Builder setFieldTypes(int[] fieldTypes) {
            this.dmlOptionsBuilder.withFieldTypes(fieldTypes);
            return this;
        }

        /**
         * optional, flush max size (includes all append, upsert and delete records), over this
         * number of records, will flush data.
         */
        public Builder setFlushMaxSize(int flushMaxSize) {
            executionOptionsBuilder.withBatchSize(flushMaxSize);
            return this;
        }

        /** optional, flush interval mills, over this time, asynchronous threads will flush data. */
        public Builder setFlushIntervalMills(long flushIntervalMills) {
            executionOptionsBuilder.withBatchIntervalMs(flushIntervalMills);
            return this;
        }

        /** optional, max retry times for jdbc connector. */
        public Builder setMaxRetryTimes(int maxRetryTimes) {
            executionOptionsBuilder.withMaxRetries(maxRetryTimes);
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured JdbcUpsertOutputFormat
         */
        public JdbcOutputFormat<Row, Row, JdbcBatchStatementExecutor<Row>> build() {
            checkNotNull(options, "No options supplied.");

            JdbcDmlOptions dml = this.dmlOptionsBuilder.build();
            // warn: don't close over builder fields
            String sql =
                    FieldNamedPreparedStatementImpl.parseNamedStatement(
                            options.getDialect()
                                    .getInsertIntoStatement(
                                            dml.getTableName(), dml.getFieldNames()),
                            new HashMap<>());
            return new RowJdbcOutputFormat<>(
                    new SimpleJdbcConnectionProvider(options),
                    executionOptionsBuilder.build(),
                    () -> createSimpleRowExecutor(sql, dml.getFieldTypes()));
        }
    }
}
