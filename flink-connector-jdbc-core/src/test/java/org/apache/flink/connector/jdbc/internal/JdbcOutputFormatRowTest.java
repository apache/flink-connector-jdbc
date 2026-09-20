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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.utils.JdbcUtils.setRecordToStatement;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link JdbcOutputFormat} writing {@link Row} records through {@link JdbcUtils}. */
class JdbcOutputFormatRowTest extends JdbcDataTestBase {

    @Test
    void testEnrichedClassCastException() throws Exception {
        String expectedMsg = "field index: 3, field value: 11.11.";
        JdbcOutputFormat<Row, Row, ?> jdbcOutputFormat =
                new JdbcOutputFormat<>(
                        new SimpleJdbcConnectionProvider(
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(getMetadata().getJdbcUrl())
                                        .withDriverName(getMetadata().getDriverClass())
                                        .build()),
                        JdbcExecutionOptions.defaults(),
                        () ->
                                createSimpleRowExecutor(
                                        String.format(INSERT_TEMPLATE, OUTPUT_TABLE),
                                        new int[] {
                                            Types.INTEGER,
                                            Types.VARCHAR,
                                            Types.VARCHAR,
                                            Types.DOUBLE,
                                            Types.INTEGER
                                        }));
        jdbcOutputFormat.open(
                JdbcOutputSerializer.of(getSerializer(TypeInformation.of(Row.class), true)));
        // "11.11" is a String where the statement expects a DOUBLE; close() flushes the buffered
        // batch
        jdbcOutputFormat.writeRecord(
                Row.of(1001, "Java public for dummies", "Tan Ah Teck", "11.11", 11));

        assertThatThrownBy(jdbcOutputFormat::close)
                .satisfies(
                        e -> {
                            assertThat(findThrowable(e, ClassCastException.class)).isPresent();
                            assertThat(findThrowableWithMessage(e, expectedMsg)).isPresent();
                        });
    }

    private static JdbcBatchStatementExecutor<Row> createSimpleRowExecutor(
            String sql, int[] fieldTypes) {
        JdbcStatementBuilder<Row> builder =
                (st, record) -> setRecordToStatement(st, fieldTypes, record);
        return JdbcBatchStatementExecutor.simple(sql, builder);
    }
}
