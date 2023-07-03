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

import org.apache.flink.connector.jdbc.databases.mysql.MySqlDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static java.lang.String.join;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test unsigned type conversion between Flink and JDBC driver mysql, the test underlying use MySQL
 * to mock a DB.
 */
class UnsignedTypeConversionITCase extends AbstractTestBase implements MySqlDatabase {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(UnsignedTypeConversionITCase.class);

    private static final String TABLE_NAME = "unsigned_test";
    private static final List<String> COLUMNS =
            Arrays.asList(
                    "tiny_c",
                    "tiny_un_c",
                    "small_c",
                    "small_un_c",
                    "int_c",
                    "int_un_c",
                    "big_c",
                    "big_un_c");

    private static final Object[] ROW =
            new Object[] {
                (byte) 127,
                (short) 255,
                (short) 32767,
                65535,
                2147483647,
                4294967295L,
                9223372036854775807L,
                new BigDecimal("18446744073709551615")
            };

    @Test
    void testUnsignedType() throws Exception {
        try (Connection con = getMetadata().getConnection()) {
            StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            TableEnvironment tableEnv = StreamTableEnvironment.create(sEnv);
            createMysqlTable(con);
            createFlinkTable(tableEnv);
            prepareData(tableEnv);

            // write data to db
            tableEnv.executeSql(
                            format("insert into jdbc_sink select %s from data", join(",", COLUMNS)))
                    .await();

            // read data from db using jdbc connection and compare
            try (PreparedStatement ps =
                    con.prepareStatement(
                            format("select %s from %s", join(",", COLUMNS), TABLE_NAME))) {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    for (int i = 0; i < ROW.length; i++) {
                        assertThat(resultSet.getObject(i + 1, ROW[i].getClass())).isEqualTo(ROW[i]);
                    }
                }
            }

            // read data from db using flink and compare
            String sql = format("select %s from jdbc_source", join(",", COLUMNS));
            CloseableIterator<Row> collected = tableEnv.executeSql(sql).collect();
            List<Row> result = CollectionUtil.iteratorToList(collected);
            assertThat(result).containsOnly(Row.ofKind(RowKind.INSERT, ROW));
        }
    }

    private void createMysqlTable(Connection con) throws SQLException {
        try (PreparedStatement ps =
                con.prepareStatement(
                        "create table "
                                + TABLE_NAME
                                + " ("
                                + " tiny_c TINYINT,"
                                + " tiny_un_c TINYINT UNSIGNED,"
                                + " small_c SMALLINT,"
                                + " small_un_c SMALLINT UNSIGNED,"
                                + " int_c INTEGER ,"
                                + " int_un_c INTEGER UNSIGNED,"
                                + " big_c BIGINT,"
                                + " big_un_c BIGINT UNSIGNED);")) {
            ps.execute();
        }
    }

    private void createFlinkTable(TableEnvironment tableEnv) {
        String commonDDL =
                "create table %s ("
                        + "tiny_c TINYINT,"
                        + "tiny_un_c SMALLINT,"
                        + "small_c SMALLINT,"
                        + "small_un_c INT,"
                        + "int_c INT,"
                        + "int_un_c BIGINT,"
                        + "big_c BIGINT,"
                        + "big_un_c DECIMAL(20, 0)) with("
                        + " 'connector' = 'jdbc',"
                        + " 'url' = '"
                        + getMetadata().getJdbcUrlWithCredentials()
                        + "',"
                        + " 'table-name' = '"
                        + TABLE_NAME
                        + "'"
                        + ")";
        tableEnv.executeSql(format(commonDDL, "jdbc_source"));
        tableEnv.executeSql(format(commonDDL, "jdbc_sink"));
    }

    private void prepareData(TableEnvironment tableEnv) {
        Table dataTable =
                tableEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("tiny_c", DataTypes.TINYINT().notNull()),
                                DataTypes.FIELD("tiny_un_c", DataTypes.SMALLINT().notNull()),
                                DataTypes.FIELD("small_c", DataTypes.SMALLINT().notNull()),
                                DataTypes.FIELD("small_un_c", DataTypes.INT().notNull()),
                                DataTypes.FIELD("int_c", DataTypes.INT().notNull()),
                                DataTypes.FIELD("int_un_c", DataTypes.BIGINT().notNull()),
                                DataTypes.FIELD("big_c", DataTypes.BIGINT().notNull()),
                                DataTypes.FIELD("big_un_c", DataTypes.DECIMAL(20, 0).notNull())),
                        Row.of(ROW));
        tableEnv.createTemporaryView("data", dataTable);
    }
}
