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

package org.apache.flink.connector.jdbc.oracle.table;

import org.apache.flink.connector.jdbc.oracle.OracleTestBase;
import org.apache.flink.connector.jdbc.oracle.database.dialect.OracleDialect;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableBuilder;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/** The Table Sink ITCase for {@link OracleDialect}. */
public class OracleDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements OracleTestBase {

    @Override
    protected TableRow createUpsertOutputTable() {
        return TableBuilder.tableRow(
                "dynamicSinkForUpsert",
                TableBuilder.pkField(
                        "cnt", TableBuilder.dbType("NUMBER"), DataTypes.BIGINT().notNull()),
                TableBuilder.field(
                        "lencnt", TableBuilder.dbType("NUMBER"), DataTypes.BIGINT().notNull()),
                TableBuilder.pkField("cTag", DataTypes.INT().notNull()),
                TableBuilder.field("ts", TableBuilder.dbType("TIMESTAMP"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createAppendOutputTable() {
        return TableBuilder.tableRow(
                "dynamicSinkForAppend",
                TableBuilder.field("id", DataTypes.INT().notNull()),
                TableBuilder.field(
                        "num", TableBuilder.dbType("NUMBER"), DataTypes.BIGINT().notNull()),
                TableBuilder.field("ts", TableBuilder.dbType("TIMESTAMP"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createBatchOutputTable() {
        return TableBuilder.tableRow(
                "dynamicSinkForBatch",
                TableBuilder.field("NAME", DataTypes.VARCHAR(20).notNull()),
                TableBuilder.field(
                        "SCORE", TableBuilder.dbType("NUMBER"), DataTypes.BIGINT().notNull()));
    }

    @Override
    protected TableRow createRealOutputTable() {
        return TableBuilder.tableRow(
                "REAL_TABLE",
                TableBuilder.field("real_data", TableBuilder.dbType("REAL"), DataTypes.FLOAT()));
    }

    @Override
    protected TableRow createCheckpointOutputTable() {
        return TableBuilder.tableRow(
                "checkpointTable",
                TableBuilder.field(
                        "id", TableBuilder.dbType("NUMBER"), DataTypes.BIGINT().notNull()));
    }

    @Override
    protected List<Row> testUserData() {
        return Arrays.asList(
                Row.of(
                        "user1",
                        "Tom",
                        "tom123@gmail.com",
                        new BigDecimal("8.1"),
                        new BigDecimal("16.2")),
                Row.of(
                        "user3",
                        "Bailey",
                        "bailey@qq.com",
                        new BigDecimal("9.99"),
                        new BigDecimal("19.98")),
                Row.of(
                        "user4",
                        "Tina",
                        "tina@gmail.com",
                        new BigDecimal("11.3"),
                        new BigDecimal("22.6")));
    }
}
