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

package org.apache.flink.connector.jdbc.databases.sqlserver.table;

import org.apache.flink.connector.jdbc.databases.sqlserver.SqlServerTestBase;
import org.apache.flink.connector.jdbc.databases.sqlserver.dialect.SqlServerDialect;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.assertj.core.api.Assertions.assertThat;

/** The Table Source ITCase for {@link SqlServerDialect}. */
class SqlServerTableSourceITCase extends AbstractTestBase implements SqlServerTestBase {

    private static final TableRow INPUT_TABLE =
            tableRow(
                    "sql_test_table",
                    field("id", dbType("INT"), DataTypes.INT().notNull()),
                    field("tiny_int", dbType("TINYINT"), DataTypes.TINYINT()),
                    field("small_int", dbType("SMALLINT"), DataTypes.SMALLINT()),
                    field("big_int", dbType("BIGINT"), DataTypes.BIGINT().notNull()),
                    field("float_col", dbType("REAL"), DataTypes.FLOAT()),
                    field("double_col", dbType("FLOAT"), DataTypes.DOUBLE()),
                    field("decimal_col", dbType("DECIMAL(10, 4)"), DataTypes.DECIMAL(10, 4)),
                    field("bool", dbType("BIT"), DataTypes.BOOLEAN()),
                    field("date_col", dbType("DATE"), DataTypes.DATE()),
                    field("time_col", dbType("TIME(5)"), DataTypes.TIME(0)),
                    field("datetime_col", dbType("DATETIME"), DataTypes.TIMESTAMP()),
                    field(
                            "datetime2_col",
                            dbType("DATETIME2"),
                            DataTypes.TIMESTAMP_WITH_TIME_ZONE()),
                    field("char_col", dbType("CHAR"), DataTypes.STRING()),
                    field("nchar_col", dbType("NCHAR(3)"), DataTypes.STRING()),
                    field("varchar2_col", dbType("VARCHAR(30)"), DataTypes.STRING()),
                    field("nvarchar2_col", dbType("NVARCHAR(30)"), DataTypes.STRING()),
                    field("text_col", dbType("TEXT"), DataTypes.STRING()),
                    field("ntext_col", dbType("NTEXT"), DataTypes.STRING()),
                    field("binary_col", dbType("BINARY(10)"), DataTypes.BYTES()));

    private static final String INPUT_TABLE_NAME = INPUT_TABLE.getTableName();

    private static StreamExecutionEnvironment env;
    private static TableEnvironment tEnv;

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(INPUT_TABLE);
    }

    @BeforeEach
    void before() throws SQLException {

        try (Connection conn = getMetadata().getConnection()) {
            INPUT_TABLE.insertIntoTableValues(
                    conn,
                    "1, 2, 4, 10000000000, 1.12345, 2.12345678791, 100.1234, 0, "
                            + "'1997-01-01', '05:20:20.222','2020-01-01 15:35:00.123',"
                            + "'2020-01-01 15:35:00.1234567', 'a', 'abc', 'abcdef', 'xyz',"
                            + "'Hello World', 'World Hello', 1024");
            INPUT_TABLE.insertIntoTableValues(
                    conn,
                    "2, 2, 4, 10000000000, 1.12345, 2.12345678791, 101.1234, 1, "
                            + "'1997-01-02', '05:20:20.222','2020-01-01 15:36:01.123',"
                            + "'2020-01-01 15:36:01.1234567', 'a', 'abc', 'abcdef', 'xyz',"
                            + "'Hey Leonard', 'World Hello', 1024");
        }
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    void testJdbcSource() throws Exception {
        createFlinkTable();
        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE_NAME).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 2, 4, 10000000000, 1.12345, 2.12345678791, 100.1234, false, "
                                        + "1997-01-01, 05:20:20, 2020-01-01T15:35:00.123, "
                                        + "2020-01-01T15:35:00.123456700, a, abc, abcdef, xyz, "
                                        + "Hello World, World Hello, [0, 0, 0, 0, 0, 0, 0, 0, 4, 0]]",
                                "+I[2, 2, 4, 10000000000, 1.12345, 2.12345678791, 101.1234, true, "
                                        + "1997-01-02, 05:20:20, 2020-01-01T15:36:01.123, "
                                        + "2020-01-01T15:36:01.123456700, a, abc, abcdef, xyz, "
                                        + "Hey Leonard, World Hello, [0, 0, 0, 0, 0, 0, 0, 0, 4, 0]]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testProject() throws Exception {
        createFlinkTable();
        Iterator<Row> collected =
                tEnv.executeSql("SELECT id,datetime_col,decimal_col FROM " + INPUT_TABLE_NAME)
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 2020-01-01T15:35:00.123, 100.1234]",
                                "+I[2, 2020-01-01T15:36:01.123, 101.1234]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testFilter() throws Exception {
        createFlinkTable();
        Iterator<Row> collected =
                tEnv.executeSql(
                                "SELECT id,datetime_col,decimal_col FROM "
                                        + INPUT_TABLE_NAME
                                        + " WHERE id = 1")
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of("+I[1, 2020-01-01T15:35:00.123, 100.1234]").collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    private void createFlinkTable() {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE_NAME
                        + " ("
                        + "id INT NOT NULL,"
                        + "tiny_int TINYINT,"
                        + "small_int SMALLINT,"
                        + "big_int BIGINT,"
                        + "float_col FLOAT,"
                        + "double_col DOUBLE ,"
                        + "decimal_col DECIMAL(10, 4) NOT NULL,"
                        + "bool BOOLEAN NOT NULL,"
                        + "date_col DATE NOT NULL,"
                        + "time_col TIME(0) NOT NULL,"
                        + "datetime_col TIMESTAMP,"
                        + "datetime2_col TIMESTAMP WITHOUT TIME ZONE,"
                        + "char_col STRING NOT NULL,"
                        + "nchar_col STRING NOT NULL,"
                        + "varchar2_col STRING NOT NULL,"
                        + "nvarchar2_col STRING NOT NULL,"
                        + "text_col STRING,"
                        + "ntext_col STRING,"
                        + "binary_col BYTES"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + getMetadata().getJdbcUrl()
                        + "',"
                        + "  'table-name'='"
                        + INPUT_TABLE_NAME
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
