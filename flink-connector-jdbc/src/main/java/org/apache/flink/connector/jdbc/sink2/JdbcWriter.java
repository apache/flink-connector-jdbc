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

package org.apache.flink.connector.jdbc.sink2;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.sink2.statement.JdbcQueryStatement;

import java.io.IOException;
import java.util.function.Function;

class JdbcWriter<IN> implements SinkWriter<IN> {

    private final JdbcOutputFormat<IN, IN, JdbcBatchStatementExecutor<IN>> jdbcOutput;

    JdbcWriter(
            JdbcConnectionProvider connectionProvider,
            JdbcExecutionOptions executionOptions,
            JdbcQueryStatement<IN> queryStatement)
            throws IOException {

        this.jdbcOutput =
                new JdbcOutputFormat<>(
                        connectionProvider,
                        executionOptions,
                        context ->
                                JdbcBatchStatementExecutor.simple(
                                        queryStatement.query(),
                                        queryStatement::map,
                                        Function.identity()),
                        JdbcOutputFormat.RecordExtractor.identity());

        this.jdbcOutput.open(new ExecutionConfig());
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        this.jdbcOutput.writeRecord(element);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        this.jdbcOutput.flush();
    }

    @Override
    public void close() throws Exception {
        this.jdbcOutput.close();
    }
}
