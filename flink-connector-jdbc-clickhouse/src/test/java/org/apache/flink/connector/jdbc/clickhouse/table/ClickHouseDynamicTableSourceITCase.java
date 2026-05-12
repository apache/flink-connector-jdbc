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
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase.tableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;

/** The Table Source ITCase for {@link ClickHouseDialect}. */
class ClickHouseDynamicTableSourceITCase extends JdbcDynamicTableSourceITCase
        implements ClickHouseTestBase {

    @Override
    protected TableRow createInputTable() {
        return tableRow(
                "jdbDynamicTableSource",
                field("id", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("decimal_col", dbType("Decimal(10, 4)"), DataTypes.DECIMAL(10, 4)),
                field("timestamp6_col", dbType("DateTime64(6)"), DataTypes.TIMESTAMP(6)),
                field("int_col", dbType("Int32"), DataTypes.INT()),
                field("smallint_col", dbType("Int16"), DataTypes.SMALLINT()),
                field("tinyint_col", dbType("Int8"), DataTypes.TINYINT()),
                field("bigint_col", dbType("Int64"), DataTypes.BIGINT()),
                field("float_col", dbType("Float32"), DataTypes.FLOAT()),
                field("double_col", dbType("Float64"), DataTypes.DOUBLE()),
                field("string_col", dbType("String"), DataTypes.STRING()),
                field("date_col", dbType("Date"), DataTypes.DATE()),
                field("timestamp_col", dbType("DateTime(0)"), DataTypes.TIMESTAMP()),
                field("nullable_bool_col", dbType("Nullable(Bool)"), DataTypes.BOOLEAN()),
                field("nullable_int_col", dbType("Nullable(Int32)"), DataTypes.INT()),
                field("nullable_string_col", dbType("Nullable(String)"), DataTypes.STRING()));
    }

    @Override
    protected List<Row> getTestData() {
        return Arrays.asList(
                Row.of(
                        1L,
                        BigDecimal.valueOf(100.1234),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                        42,
                        (short) 11,
                        (byte) 1,
                        999L,
                        1.175E-37F,
                        1.79769E308D,
                        "hello",
                        LocalDate.parse("2020-01-01"),
                        LocalDateTime.parse("2020-01-01T15:35:00"),
                        true,
                        null,
                        null),
                Row.of(
                        2L,
                        BigDecimal.valueOf(101.1234),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456"),
                        -42,
                        (short) -7,
                        (byte) -2,
                        -999L,
                        -1.175E-37F,
                        -1.79769E308D,
                        "world",
                        LocalDate.parse("2020-01-01"),
                        LocalDateTime.parse("2020-01-01T15:36:01"),
                        null,
                        123,
                        "optional"));
    }
}
