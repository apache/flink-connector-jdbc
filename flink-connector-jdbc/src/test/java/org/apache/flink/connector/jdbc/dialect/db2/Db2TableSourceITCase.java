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

package org.apache.flink.connector.jdbc.dialect.db2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** The Table Source ITCase for {@link Db2Dialect}. */
public class Db2TableSourceITCase extends Db2TestBaseITCase {

    private static final String INPUT_TABLE = "sql_test_table";
    private static StreamExecutionEnvironment env;
    private static TableEnvironment tEnv;

    @BeforeEach
    public void before() throws ClassNotFoundException, SQLException {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        try (Connection conn = getJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE "
                            + INPUT_TABLE
                            + " (\n"
                            + "    ID INTEGER NOT NULL,\n"
                            + "    BOOLEAN_C BOOLEAN,\n"
                            + "    SMALL_C SMALLINT,\n"
                            + "    INT_C INTEGER,\n"
                            + "    BIG_C BIGINT,\n"
                            + "    REAL_C REAL,\n"
                            + "    DOUBLE_C DOUBLE,\n"
                            + "    NUMERIC_C NUMERIC(10, 5),\n"
                            + "    DECIMAL_C DECIMAL(10, 1),\n"
                            + "    VARCHAR_C VARCHAR(200),\n"
                            + "    CHAR_C CHAR,\n"
                            + "    CHARACTER_C CHAR(3),\n"
                            + "    TIMESTAMP_C TIMESTAMP,\n"
                            + "    DATE_C DATE,\n"
                            + "    TIME_C TIME,\n"
                            + "    DEFAULT_NUMERIC_C NUMERIC,\n"
                            + "    TIMESTAMP_PRECISION_C TIMESTAMP(9),\n"
                            + "    PRIMARY KEY (ID)\n"
                            + ");");
            statement.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES (\n"
                            + "    1, TRUE, 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443,\n"
                            + "    'Hello World', 'a', 'abc', '2020-07-17 18:00:22.123', '2020-07-17', '18:00:22', 500,\n"
                            + "    '2020-07-17 18:00:22.123456789');");
            statement.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES (\n"
                            + "    2, FALSE, 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443,\n"
                            + "    'Hello World', 'a', 'abc', '2020-07-17 18:00:22.123', '2020-07-17', '18:00:22', 500,\n"
                            + "    '2020-07-17 18:00:22.123456789');");
        }
    }

    @AfterEach
    public void after() throws ClassNotFoundException, SQLException {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        try (Connection conn = getJdbcConnection();
                Statement statement = conn.createStatement()) {
            statement.executeUpdate("DROP TABLE " + INPUT_TABLE);
        }
    }

    @Test
    public void testJdbcSource() throws Exception {
        createFlinkTable();
        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, true, 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4, Hello World, a, abc, 2020-07-17T18:00:22.123, 2020-07-17, 18:00:22, 500, 2020-07-17T18:00:22.123456789]",
                                "+I[2, false, 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4, Hello World, a, abc, 2020-07-17T18:00:22.123, 2020-07-17, 18:00:22, 500, 2020-07-17T18:00:22.123456789]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testProject() throws Exception {
        createFlinkTable();
        Iterator<Row> collected =
                tEnv.executeSql("SELECT ID,TIMESTAMP_C,DECIMAL_C FROM " + INPUT_TABLE).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 2020-07-17T18:00:22.123, 404.4]",
                                "+I[2, 2020-07-17T18:00:22.123, 404.4]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testFilter() throws Exception {
        createFlinkTable();
        Iterator<Row> collected =
                tEnv.executeSql(
                                "SELECT ID,TIMESTAMP_C,DECIMAL_C FROM "
                                        + INPUT_TABLE
                                        + " WHERE ID = 1")
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of("+I[1, 2020-07-17T18:00:22.123, 404.4]").collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    private void createFlinkTable() {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + " ("
                        + "    ID INTEGER NOT NULL,\n"
                        + "    BOOLEAN_C BOOLEAN NOT NULL,\n"
                        + "    SMALL_C SMALLINT,\n"
                        + "    INT_C INTEGER,\n"
                        + "    BIG_C BIGINT,\n"
                        + "    REAL_C FLOAT,\n"
                        + "    DOUBLE_C DOUBLE,\n"
                        + "    NUMERIC_C DECIMAL(10, 5),\n"
                        + "    DECIMAL_C DECIMAL(10, 1),\n"
                        + "    VARCHAR_C STRING,\n"
                        + "    CHAR_C STRING,\n"
                        + "    CHARACTER_C STRING,\n"
                        + "    TIMESTAMP_C TIMESTAMP(3),\n"
                        + "    DATE_C DATE,\n"
                        + "    TIME_C TIME(0),\n"
                        + "    DEFAULT_NUMERIC_C DECIMAL,\n"
                        + "    TIMESTAMP_PRECISION_C TIMESTAMP(9)\n"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + DB2_CONTAINER.getJdbcUrl()
                        + "',"
                        + "  'table-name'='"
                        + INPUT_TABLE
                        + "',"
                        + "  'username'='"
                        + DB2_CONTAINER.getUsername()
                        + "',"
                        + "  'password'='"
                        + DB2_CONTAINER.getPassword()
                        + "'"
                        + ")");
    }
}
