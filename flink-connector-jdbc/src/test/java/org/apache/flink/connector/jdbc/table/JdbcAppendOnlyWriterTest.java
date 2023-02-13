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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;

import static org.apache.flink.connector.jdbc.JdbcDataTestBase.toRow;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;

/** Test for the Append only mode. */
class JdbcAppendOnlyWriterTest extends JdbcTestBase {

    private JdbcOutputFormat format;
    private String[] fieldNames;

    @BeforeEach
    void setup() {
        fieldNames = new String[] {"id", "title", "author", "price", "qty"};
    }

    @Test
    void testMaxRetry() throws Exception {
        assertThatThrownBy(
                        () -> {
                            format =
                                    JdbcOutputFormat.builder()
                                            .setOptions(
                                                    InternalJdbcConnectionOptions.builder()
                                                            .setDBUrl(getMetadata().getJdbcUrl())
                                                            .setDialect(
                                                                    JdbcDialectLoader.load(
                                                                            getMetadata()
                                                                                    .getJdbcUrl(),
                                                                            getClass()
                                                                                    .getClassLoader()))
                                                            .setTableName(OUTPUT_TABLE)
                                                            .build())
                                            .setFieldNames(fieldNames)
                                            .setKeyFields(null)
                                            .build();
                            RuntimeContext context = Mockito.mock(RuntimeContext.class);
                            ExecutionConfig config = Mockito.mock(ExecutionConfig.class);
                            doReturn(config).when(context).getExecutionConfig();
                            doReturn(true).when(config).isObjectReuseEnabled();
                            format.setRuntimeContext(context);
                            format.open(0, 1);

                            // alter table schema to trigger retry logic after failure.
                            alterTable();
                            for (TestEntry entry : TEST_DATA) {
                                format.writeRecord(Tuple2.of(true, toRow(entry)));
                            }

                            // after retry default times, throws a BatchUpdateException.
                            format.flush();
                        })
                .isInstanceOf(IOException.class);
    }

    private void alterTable() throws Exception {
        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute("ALTER  TABLE " + OUTPUT_TABLE + " DROP COLUMN " + fieldNames[1]);
        }
    }

    @AfterEach
    void clear() throws Exception {
        if (format != null) {
            try {
                format.close();
            } catch (RuntimeException e) {
                // ignore exception when close.
            }
        }
        format = null;
        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + OUTPUT_TABLE);
        }
    }
}
