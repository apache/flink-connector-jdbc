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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.function.Function;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.ROW_TYPE_INFO;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_BOOKS;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_BOOKS_SPLIT_BY_ID;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.utils.JdbcUtils.setRecordToStatement;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;

/** Tests using both {@link JdbcInputFormat} and {@link JdbcOutputFormat}. */
class JdbcFullTest extends JdbcDataTestBase {

    @Test
    void testWithoutParallelism() throws Exception {
        runTest(false);
    }

    @Test
    void testWithParallelism() throws Exception {
        runTest(true);
    }

    @Test
    void testEnrichedClassCastException() {
        String expectedMsg = "field index: 3, field value: 11.11.";
        try {
            JdbcOutputFormat jdbcOutputFormat =
                    JdbcOutputFormat.builder()
                            .setOptions(
                                    InternalJdbcConnectionOptions.builder()
                                            .setDBUrl(getMetadata().getJdbcUrl())
                                            .setTableName(OUTPUT_TABLE)
                                            .build())
                            .setFieldNames(new String[] {"id", "title", "author", "price", "qty"})
                            .setFieldTypes(
                                    new int[] {
                                        Types.INTEGER,
                                        Types.VARCHAR,
                                        Types.VARCHAR,
                                        Types.DOUBLE,
                                        Types.INTEGER
                                    })
                            .setKeyFields(null)
                            .build();
            RuntimeContext context = Mockito.mock(RuntimeContext.class);
            ExecutionConfig config = Mockito.mock(ExecutionConfig.class);
            doReturn(config).when(context).getExecutionConfig();
            doReturn(true).when(config).isObjectReuseEnabled();
            jdbcOutputFormat.setRuntimeContext(context);

            jdbcOutputFormat.open(1, 1);
            Row inputRow = Row.of(1001, "Java public for dummies", "Tan Ah Teck", "11.11", 11);
            jdbcOutputFormat.writeRecord(Tuple2.of(true, inputRow));
            jdbcOutputFormat.close();
        } catch (Exception e) {
            assertThat(findThrowable(e, ClassCastException.class)).isPresent();
            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
        }
    }

    private void runTest(boolean exploitParallelism) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        JdbcInputFormat.JdbcInputFormatBuilder inputBuilder =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO);

        if (exploitParallelism) {
            final int fetchSize = 1;
            final long min = TEST_DATA[0].id;
            final long max = TEST_DATA[TEST_DATA.length - fetchSize].id;
            // use a "splittable" query to exploit parallelism
            inputBuilder =
                    inputBuilder
                            .setQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID)
                            .setParametersProvider(
                                    new JdbcNumericBetweenParametersProvider(min, max)
                                            .ofBatchSize(fetchSize));
        }
        DataSet<Row> source = environment.createInput(inputBuilder.finish());

        // NOTE: in this case (with Derby driver) setSqlTypes could be skipped, but
        // some databases don't null values correctly when no column type was specified
        // in PreparedStatement.setObject (see its javadoc for more details)
        org.apache.flink.connector.jdbc.JdbcConnectionOptions connectionOptions =
                new org.apache.flink.connector.jdbc.JdbcConnectionOptions
                                .JdbcConnectionOptionsBuilder()
                        .withUrl(getMetadata().getJdbcUrl())
                        .withDriverName(getMetadata().getDriverClass())
                        .build();

        JdbcOutputFormat jdbcOutputFormat =
                new JdbcOutputFormat<>(
                        new SimpleJdbcConnectionProvider(connectionOptions),
                        JdbcExecutionOptions.defaults(),
                        ctx ->
                                createSimpleRowExecutor(
                                        String.format(INSERT_TEMPLATE, OUTPUT_TABLE),
                                        new int[] {
                                            Types.INTEGER,
                                            Types.VARCHAR,
                                            Types.VARCHAR,
                                            Types.DOUBLE,
                                            Types.INTEGER
                                        },
                                        ctx.getExecutionConfig().isObjectReuseEnabled()),
                        JdbcOutputFormat.RecordExtractor.identity());

        source.output(jdbcOutputFormat);
        environment.execute();

        try (Connection dbConn = DriverManager.getConnection(getMetadata().getJdbcUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS);
                ResultSet resultSet = statement.executeQuery()) {
            int count = 0;
            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(TEST_DATA.length);
        }
    }

    @AfterEach
    void clearOutputTable() throws Exception {
        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + OUTPUT_TABLE);
        }
    }

    private static JdbcBatchStatementExecutor<Row> createSimpleRowExecutor(
            String sql, int[] fieldTypes, boolean objectReuse) {
        JdbcStatementBuilder<Row> builder =
                (st, record) -> setRecordToStatement(st, fieldTypes, record);
        return JdbcBatchStatementExecutor.simple(
                sql, builder, objectReuse ? Row::copy : Function.identity());
    }
}
