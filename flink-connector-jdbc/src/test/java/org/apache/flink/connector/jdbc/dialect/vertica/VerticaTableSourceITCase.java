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

package org.apache.flink.connector.jdbc.dialect.vertica;

import org.apache.flink.connector.jdbc.databases.vertica.VerticaDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

/** The Table Source ITCase for {@link VerticaDialect}. */
public class VerticaTableSourceITCase extends AbstractTestBase implements VerticaDatabase {

    private static final String INPUT_TABLE = "sql_test_table";

    private static TableEnvironment tEnv;

    @BeforeAll
    static void beforeAll() throws ClassNotFoundException, SQLException {
        try (Connection conn = VerticaDatabase.getConnection();
                Statement stat = conn.createStatement()) {
            stat.executeUpdate(
                    "CREATE TABLE "
                            + INPUT_TABLE
                            + " ("
                            + "id INT NOT NULL,"
                            + "tiny_int TINYINT,"
                            + "small_int SMALLINT,"
                            + "big_int BIGINT,"
                            + "float_col REAL,"
                            + "double_col FLOAT ,"
                            + "decimal_col DECIMAL(10, 4) NOT NULL,"
                            + "bool BOOLEAN NOT NULL,"
                            + "date_col DATE NOT NULL,"
                            + "time_col TIME(5) NOT NULL,"
                            + "datetime_col DATETIME,"
                            + "char_col CHAR NOT NULL,"
                            + "varchar_col VARCHAR(30) NOT NULL,"
                            + "text_col LONG VARCHAR,"
                            + "binary_col BINARY(10)"
                            + ")");
            stat.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES ("
                            + "1, 2, 4, 10000000000, 1.12345, 2.12345678791, 100.1234, 0, "
                            + "'1997-01-01', '05:20:20.222','2023-02-02 09:30:00.222',"
                            + "'a', 'Hello World', 'World Hello', cast('a' as binary))");
            stat.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES ("
                            + "2, 2, 4, 10000000000, -1.12345, 2.12345678791, 101.1234, 1, "
                            + "'1997-01-02', '05:20:20.222','2023-02-02 09:30:00.222',"
                            + "'a', 'Hello World', 'World Hello', cast('a' as binary))");
        }
    }

    @AfterAll
    static void afterAll() throws Exception {
        try (Connection conn = VerticaDatabase.getConnection();
                Statement stat = conn.createStatement()) {
            stat.executeUpdate("DROP TABLE " + INPUT_TABLE);
        }
    }

    @BeforeEach
    void before() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);

        createFlinkTable();
    }

    @Test
    void testJdbcSource() {
        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 2, 4, 10000000000, 1.12345, 2.12345678791, 100.1234, false, "
                                        + "1997-01-01, 05:20:20, 2023-02-02T09:30:00.222, "
                                        + "a, Hello World, World Hello, [97, 0, 0, 0, 0, 0, 0, 0, 0, 0]]",
                                "+I[2, 2, 4, 10000000000, -1.12345, 2.12345678791, 101.1234, true, "
                                        + "1997-01-02, 05:20:20, 2023-02-02T09:30:00.222, "
                                        + "a, Hello World, World Hello, [97, 0, 0, 0, 0, 0, 0, 0, 0, 0]]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testProject() {
        Iterator<Row> collected =
                tEnv.executeSql("SELECT id,datetime_col,decimal_col FROM " + INPUT_TABLE).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 2023-02-02T09:30:00.222, 100.1234]",
                                "+I[2, 2023-02-02T09:30:00.222, 101.1234]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testFilter() {
        Iterator<Row> collected =
                tEnv.executeSql(
                                "SELECT id,datetime_col,decimal_col FROM "
                                        + INPUT_TABLE
                                        + " WHERE id = 1")
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of("+I[1, 2023-02-02T09:30:00.222, 100.1234]").collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    private void createFlinkTable() {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + " ("
                        + "id BIGINT NOT NULL,"
                        + "tiny_int BIGINT,"
                        + "small_int BIGINT,"
                        + "big_int BIGINT,"
                        + "float_col DOUBLE,"
                        + "double_col DOUBLE ,"
                        + "decimal_col DECIMAL(10, 4) NOT NULL,"
                        + "bool BOOLEAN NOT NULL,"
                        + "date_col DATE NOT NULL,"
                        + "time_col TIME(0) NOT NULL,"
                        + "datetime_col TIMESTAMP,"
                        + "char_col STRING NOT NULL,"
                        + "varchar_col STRING NOT NULL,"
                        + "text_col STRING,"
                        + "binary_col BINARY"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + getMetadata().getJdbcUrl()
                        + "',"
                        + "  'table-name'='"
                        + INPUT_TABLE
                        + "',"
                        + "  'username'='"
                        + getMetadata().getUsername()
                        + "',"
                        + "  'password'='"
                        + getMetadata().getPassword()
                        + "'"
                        + ")");
    }
}
