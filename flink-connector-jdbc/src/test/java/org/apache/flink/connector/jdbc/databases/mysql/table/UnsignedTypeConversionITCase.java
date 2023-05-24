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

package org.apache.flink.connector.jdbc.databases.mysql.table;

import org.apache.flink.connector.jdbc.databases.mysql.MySqlTestBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;
import static java.lang.String.join;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test unsigned type conversion between Flink and JDBC driver mysql, the test underlying use MySQL
 * to mock a DB.
 */
class UnsignedTypeConversionITCase extends AbstractTestBase implements MySqlTestBase {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(UnsignedTypeConversionITCase.class);
    private static final String TABLE_SOURCE = "jdbc_source";
    private static final String TABLE_SINK = "jdbc_sink";
    private static final String TABLE_DATA = "data";
    private static final TableRow TABLE =
            tableRow(
                    "unsigned_test",
                    field("tiny_c", dbType("TINYINT"), DataTypes.TINYINT().notNull()),
                    field("tiny_un_c", dbType("TINYINT UNSIGNED"), DataTypes.SMALLINT().notNull()),
                    field("small_c", dbType("SMALLINT"), DataTypes.SMALLINT().notNull()),
                    field("small_un_c", dbType("SMALLINT UNSIGNED"), DataTypes.INT().notNull()),
                    field("int_c", dbType("INTEGER"), DataTypes.INT().notNull()),
                    field("int_un_c", dbType("INTEGER UNSIGNED"), DataTypes.BIGINT().notNull()),
                    field("big_c", dbType("BIGINT"), DataTypes.BIGINT().notNull()),
                    field(
                            "big_un_c",
                            dbType("BIGINT UNSIGNED"),
                            DataTypes.DECIMAL(20, 0).notNull()));

    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TABLE);
    }

    private static final Row ROW =
            Row.of(
                    (byte) 127,
                    (short) 255,
                    (short) 32767,
                    65535,
                    2147483647,
                    4294967295L,
                    9223372036854775807L,
                    new BigDecimal("18446744073709551615"));

    @Test
    void testUnsignedType() throws Exception {
        try (Connection con = getMetadata().getConnection()) {
            StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            TableEnvironment tableEnv = StreamTableEnvironment.create(sEnv);
            createFlinkTable(tableEnv);
            prepareData(tableEnv);

            // write data to db
            String columns = join(",", TABLE.getTableFields());
            tableEnv.executeSql(
                            format(
                                    "insert into %s select %s from %s",
                                    TABLE_SINK, columns, TABLE_DATA))
                    .await();

            // read data from db using jdbc connection and compare
            List<Row> selectAll = TABLE.selectAllTable(con);
            assertThat(selectAll).containsOnly(ROW);

            // read data from db using flink and compare
            String sql = format("select %s from %s", columns, TABLE_SOURCE);
            CloseableIterator<Row> collected = tableEnv.executeSql(sql).collect();
            List<Row> result = CollectionUtil.iteratorToList(collected);
            assertThat(result).containsOnly(ROW);
        }
    }

    private void createFlinkTable(TableEnvironment tableEnv) {
        tableEnv.executeSql(TABLE.getCreateQueryForFlink(getMetadata(), TABLE_SOURCE));
        tableEnv.executeSql(TABLE.getCreateQueryForFlink(getMetadata(), TABLE_SINK));
    }

    private void prepareData(TableEnvironment tableEnv) {
        Table dataTable = tableEnv.fromValues(DataTypes.ROW(TABLE.getTableDataFields()), ROW);
        tableEnv.createTemporaryView(TABLE_DATA, dataTable);
    }
}
