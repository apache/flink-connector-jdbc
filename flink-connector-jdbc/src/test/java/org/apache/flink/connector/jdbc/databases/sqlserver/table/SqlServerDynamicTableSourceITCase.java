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
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSourceITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The Table Source ITCase for {@link SqlServerDialect}. */
public class SqlServerDynamicTableSourceITCase extends JdbcDynamicTableSourceITCase
        implements SqlServerTestBase {

    @Override
    protected TableRow createInputTable() {
        return tableRow(
                "jdbDynamicTableSource",
                field("id", DataTypes.BIGINT().notNull()),
                field("decimal_col", DataTypes.DECIMAL(10, 4)),
                field("timestamp6_col", dbType("DATETIME2"), DataTypes.TIMESTAMP(6)),
                // other fields
                field("tiny_int", dbType("TINYINT"), DataTypes.TINYINT()),
                field("small_int", dbType("SMALLINT"), DataTypes.SMALLINT()),
                field("big_int", dbType("BIGINT"), DataTypes.BIGINT().notNull()),
                field("float_col", dbType("REAL"), DataTypes.FLOAT()),
                field("double_col", dbType("FLOAT"), DataTypes.DOUBLE()),
                field("bool", dbType("BIT"), DataTypes.BOOLEAN()),
                field("date_col", dbType("DATE"), DataTypes.DATE()),
                field("time_col", dbType("TIME(3)"), DataTypes.TIME()),
                field("datetime_col", dbType("DATETIME"), DataTypes.TIMESTAMP()),
                field("datetime2_col", dbType("DATETIME2"), DataTypes.TIMESTAMP()),
                field("char_col", dbType("CHAR"), DataTypes.STRING()),
                field("nchar_col", dbType("NCHAR(3)"), DataTypes.STRING()),
                field("varchar2_col", dbType("VARCHAR(30)"), DataTypes.STRING()),
                field("nvarchar2_col", dbType("NVARCHAR(30)"), DataTypes.STRING()),
                field("text_col", dbType("TEXT"), DataTypes.STRING()),
                field("ntext_col", dbType("NTEXT"), DataTypes.STRING()));
    }

    protected List<Row> getTestData() {
        return Arrays.asList(
                Row.of(
                        1L,
                        BigDecimal.valueOf(100.1234),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                        Byte.decode("2"),
                        Short.decode("4"),
                        10000000000L,
                        1.12345F,
                        2.12345678791D,
                        false,
                        LocalDate.parse("1997-01-01"),
                        LocalTime.parse("05:20:20"),
                        LocalDateTime.parse("2020-01-01T15:35:00.123"),
                        LocalDateTime.parse("2020-01-01T15:35:00.1234567"),
                        "a",
                        "abc",
                        "abcdef",
                        "xyz",
                        "Hello World",
                        "World Hello"),
                Row.of(
                        2L,
                        BigDecimal.valueOf(101.1234),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456"),
                        Byte.decode("2"),
                        Short.decode("4"),
                        10000000000L,
                        1.12345F,
                        2.12345678791D,
                        true,
                        LocalDate.parse("1997-01-02"),
                        LocalTime.parse("05:20:20"),
                        LocalDateTime.parse("2020-01-01T15:36:01.123"),
                        LocalDateTime.parse("2020-01-01T15:36:01.1234567"),
                        "a",
                        "abc",
                        "abcdef",
                        "xyz",
                        "Hey Leonard",
                        "World Hello"));
    }

    @Test
    @Override
    public void testLimit() {
        assertThatThrownBy(super::testLimit)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("SqlServerDialect does not support limit clause");
    }
}
