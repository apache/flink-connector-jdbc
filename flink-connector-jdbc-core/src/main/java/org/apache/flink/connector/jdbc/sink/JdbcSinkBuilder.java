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

package org.apache.flink.connector.jdbc.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.PoolingXaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.SimpleXaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.XaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.datasource.statements.SimpleJdbcQueryStatement;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Builder to construct {@link JdbcSink}. */
@PublicEvolving
public class JdbcSinkBuilder<IN> {

    private JdbcExecutionOptions executionOptions;
    private JdbcQueryStatement<IN> queryStatement;

    public JdbcSinkBuilder() {
        this.executionOptions = JdbcExecutionOptions.defaults();
    }

    public JdbcSinkBuilder<IN> withExecutionOptions(JdbcExecutionOptions executionOptions) {
        this.executionOptions = checkNotNull(executionOptions, "executionOptions cannot be null");
        return this;
    }

    public JdbcSinkBuilder<IN> withQueryStatement(JdbcQueryStatement<IN> queryStatement) {
        this.queryStatement = queryStatement;
        return this;
    }

    public JdbcSinkBuilder<IN> withQueryStatement(
            String query, JdbcStatementBuilder<IN> statement) {
        this.queryStatement = new SimpleJdbcQueryStatement<>(query, statement);
        return this;
    }

    public JdbcSink<IN> buildAtLeastOnce(JdbcConnectionOptions connectionOptions) {
        checkNotNull(connectionOptions, "connectionOptions cannot be null");

        return buildAtLeastOnce(new SimpleJdbcConnectionProvider(connectionOptions));
    }

    public JdbcSink<IN> buildAtLeastOnce(JdbcConnectionProvider connectionProvider) {
        checkNotNull(connectionProvider, "connectionProvider cannot be null");

        return build(
                DeliveryGuarantee.AT_LEAST_ONCE,
                JdbcExactlyOnceOptions.defaults(),
                checkNotNull(connectionProvider, "connectionProvider cannot be null"));
    }

    public JdbcSink<IN> buildExactlyOnce(
            JdbcExactlyOnceOptions exactlyOnceOptions,
            SerializableSupplier<XADataSource> dataSourceSupplier) {

        checkNotNull(exactlyOnceOptions, "exactlyOnceOptions cannot be null");
        checkNotNull(dataSourceSupplier, "dataSourceSupplier cannot be null");
        XaConnectionProvider connectionProvider =
                exactlyOnceOptions.isTransactionPerConnection()
                        ? PoolingXaConnectionProvider.from(
                                dataSourceSupplier, exactlyOnceOptions.getTimeoutSec())
                        : SimpleXaConnectionProvider.from(
                                dataSourceSupplier, exactlyOnceOptions.getTimeoutSec());
        return buildExactlyOnce(exactlyOnceOptions, connectionProvider);
    }

    public JdbcSink<IN> buildExactlyOnce(
            JdbcExactlyOnceOptions exactlyOnceOptions, XaConnectionProvider connectionProvider) {

        return build(
                DeliveryGuarantee.EXACTLY_ONCE,
                checkNotNull(exactlyOnceOptions, "exactlyOnceOptions cannot be null"),
                checkNotNull(connectionProvider, "connectionProvider cannot be null"));
    }

    private JdbcSink<IN> build(
            DeliveryGuarantee deliveryGuarantee,
            JdbcExactlyOnceOptions exactlyOnceOptions,
            JdbcConnectionProvider connectionProvider) {

        return new JdbcSink<>(
                checkNotNull(deliveryGuarantee, "deliveryGuarantee cannot be null"),
                checkNotNull(connectionProvider, "connectionProvider cannot be null"),
                checkNotNull(executionOptions, "executionOptions cannot be null"),
                checkNotNull(exactlyOnceOptions, "exactlyOnceOptions cannot be null"),
                checkNotNull(queryStatement, "queryStatement cannot be null"));
    }
}
