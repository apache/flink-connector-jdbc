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

package org.apache.flink.connector.jdbc.postgres.table;

import org.apache.flink.connector.jdbc.core.table.sink.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.postgres.database.dialect.PostgresDialect;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Table Sink ITCase for {@link PostgresDialect}.
 *
 * <p>This test class inherits all parent tests which validate standard JDBC batching behavior.
 * Additional tests are provided to specifically validate PostgreSQL UNNEST optimization.
 */
class PostgresDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements PostgresTestBase {

    /**
     * Test UPSERT operations with UNNEST enabled.
     * This validates that the UNNEST optimization produces correct results.
     * Standard batching is already validated by the inherited parent test.
     */
    @Test
    void testUpsertWithUnnest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String viewName = "testData";
        tEnv.createTemporaryView(
                viewName, tEnv.fromValues(testData()).as("id", "num", "text", "ts"));

        String tableName = "upsertSinkUnnest";
        List<String> options = Arrays.asList(
                "'sink.buffer-flush.max-rows' = '2'",
                "'sink.buffer-flush.interval' = '0'",
                "'sink.max-retries' = '0'",
                "'sink.postgres.unnest.enabled' = 'true'");

        tEnv.executeSql(
                upsertOutputTable.getCreateQueryForFlink(getMetadata(), tableName, options));

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s "
                                        + " SELECT cnt, COUNT(len) AS lencnt, cTag, MAX(ts) AS ts "
                                        + " FROM ( "
                                        + "  SELECT len, COUNT(id) as cnt, cTag, MAX(ts) AS ts "
                                        + "  FROM (SELECT id, CHAR_LENGTH(text) AS len, (CASE WHEN id > 0 THEN 1 ELSE 0 END) cTag, ts FROM %s) "
                                        + "  GROUP BY len, cTag "
                                        + " ) "
                                        + " GROUP BY cnt, cTag",
                                tableName, viewName))
                .await();

        Map<Integer, Row> mapTestData = testDataMap();
        assertThat(upsertOutputTable.selectAllTable(getMetadata()))
                .containsExactlyInAnyOrder(
                        Row.of(1L, 5L, 1, mapTestData.get(6).getField(3)),
                        Row.of(7L, 1L, 1, mapTestData.get(21).getField(3)),
                        Row.of(9L, 1L, 1, mapTestData.get(15).getField(3)));
    }
}

