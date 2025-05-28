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

package org.apache.flink.connector.jdbc.clickhouse.table;

import org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase;
import org.apache.flink.connector.jdbc.clickhouse.database.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.core.table.source.JdbcDynamicTableSourceITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableBuilder;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/** The Table Source ITCase for {@link ClickHouseDialect}. */
class ClickHouseDynamicTableSourceITCase extends JdbcDynamicTableSourceITCase
        implements ClickHouseTestBase {

    @Override
    protected TableRow createInputTable() {
        return TableBuilder.tableRow(
                "jdbDynamicTableSource",
                TableBuilder.pkField(
                        "id", TableBuilder.dbType("Int64"), DataTypes.BIGINT().notNull()),
                TableBuilder.field(
                        "decimal_col",
                        TableBuilder.dbType("Decimal(10,4)"),
                        DataTypes.DECIMAL(10, 4)),
                TableBuilder.field(
                        "timestamp6_col",
                        TableBuilder.dbType("DateTime64(6)"),
                        DataTypes.TIMESTAMP(6)),
                // other fields
                TableBuilder.field("float_col", TableBuilder.dbType("Float32"), DataTypes.FLOAT()),
                TableBuilder.field(
                        "double_col", TableBuilder.dbType("Float64"), DataTypes.DOUBLE()),
                TableBuilder.field(
                        "binary_float_col", TableBuilder.dbType("Float32"), DataTypes.FLOAT()),
                TableBuilder.field(
                        "binary_double_col", TableBuilder.dbType("Float64"), DataTypes.DOUBLE()),
                TableBuilder.field("char_col", TableBuilder.dbType("String"), DataTypes.CHAR(1)),
                TableBuilder.field(
                        "string_col", TableBuilder.dbType("String"), DataTypes.VARCHAR(3)),
                TableBuilder.field(
                        "string2_col", TableBuilder.dbType("String"), DataTypes.VARCHAR(30)),
                TableBuilder.field("date_col", TableBuilder.dbType("Date"), DataTypes.DATE()),
                TableBuilder.field(
                        "dt9_col", TableBuilder.dbType("DateTime64(9)"), DataTypes.TIMESTAMP(9)),
                TableBuilder.field("clob_col", TableBuilder.dbType("String"), DataTypes.STRING()));
    }

    @Override
    protected List<Row> getTestData() {
        return Arrays.asList(
                Row.of(
                        1L,
                        BigDecimal.valueOf(100.1234),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                        1.12345,
                        2.1234567879,
                        1.175E-10,
                        1.79769E+40,
                        "a",
                        "abc",
                        "abcdef",
                        LocalDate.parse("1997-01-01"),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456789"),
                        "Hello World"),
                Row.of(
                        2L,
                        BigDecimal.valueOf(101.1234),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456"),
                        1.12345,
                        2.12345678790,
                        1.175E-10,
                        1.79769E+40,
                        "a",
                        "abc",
                        "abcdef",
                        LocalDate.parse("1997-01-02"),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456789"),
                        "Hey Leonard"));
    }
}
