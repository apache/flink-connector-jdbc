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

package org.apache.flink.connector.jdbc.databases.db2.table;

import org.apache.flink.connector.jdbc.databases.db2.Db2TestBase;
import org.apache.flink.connector.jdbc.databases.db2.dialect.Db2Dialect;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSourceITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;

/** The Table Source ITCase for {@link Db2Dialect}. */
public class Db2DynamicTableSourceITCase extends JdbcDynamicTableSourceITCase
        implements Db2TestBase {

    @Override
    protected TableRow createInputTable() {
        return tableRow(
                "jdbDynamicTableSource",
                field("id", dbType("BIGINT"), DataTypes.BIGINT().notNull()),
                field("decimal_col", dbType("NUMERIC(10, 4)"), DataTypes.DECIMAL(10, 4)),
                field("timestamp6_col", dbType("TIMESTAMP(6)"), DataTypes.TIMESTAMP(6)),
                // other fields are covered by the base class
                field("boolean_c", dbType("BOOLEAN"), DataTypes.BOOLEAN()),
                field("small_c", dbType("SMALLINT"), DataTypes.SMALLINT()),
                field("int_c", dbType("INTEGER"), DataTypes.INT()),
                field("big_c", dbType("BIGINT"), DataTypes.BIGINT()),
                field("real_c", dbType("REAL"), DataTypes.FLOAT()),
                field("double_c", dbType("DOUBLE"), DataTypes.DOUBLE()),
                field("numeric_c", dbType("NUMERIC(10, 5)"), DataTypes.DECIMAL(10, 5)),
                field("decimal_c", dbType("DECIMAL(10, 1)"), DataTypes.DECIMAL(10, 1)),
                field("varchar_c", dbType("VARCHAR(200)"), DataTypes.STRING()),
                field("char_c", dbType("CHAR"), DataTypes.CHAR(1)),
                field("character_c", dbType("CHAR(3)"), DataTypes.CHAR(3)),
                field("timestamp_c", dbType("TIMESTAMP"), DataTypes.TIMESTAMP(3)),
                field("date_c", dbType("DATE"), DataTypes.DATE()),
                field("time_c", dbType("TIME"), DataTypes.TIME(0)),
                field("default_numeric_c", dbType("NUMERIC"), DataTypes.DECIMAL(10, 0)),
                field("timestamp_precision_c", dbType("TIMESTAMP(9)"), DataTypes.TIMESTAMP(9)));
    }

    @Override
    protected List<Row> getTestData() {
        return Arrays.asList(
                Row.of(
                        1L,
                        BigDecimal.valueOf(100.1234),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                        true,
                        (short) 32767,
                        65535,
                        2147483647L,
                        5.5f,
                        6.6d,
                        BigDecimal.valueOf(123.12345),
                        BigDecimal.valueOf(404.4),
                        "Hello World",
                        "a",
                        "abc",
                        LocalDateTime.parse("2020-07-17T18:00:22.123"),
                        LocalDate.parse("2020-07-17"),
                        LocalTime.parse("18:00:22"),
                        BigDecimal.valueOf(500),
                        LocalDateTime.parse("2020-07-17T18:00:22.123456789")),
                Row.of(
                        2L,
                        BigDecimal.valueOf(101.1234),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456"),
                        false,
                        (short) 32767,
                        65535,
                        2147483647L,
                        5.5f,
                        6.6d,
                        BigDecimal.valueOf(123.12345),
                        BigDecimal.valueOf(404.4),
                        "Hello World",
                        "a",
                        "abc",
                        LocalDateTime.parse("2020-07-17T18:00:22.123"),
                        LocalDate.parse("2020-07-17"),
                        LocalTime.parse("18:00:22"),
                        BigDecimal.valueOf(500),
                        LocalDateTime.parse("2020-07-17T18:00:22.123456789")));
    }
}
