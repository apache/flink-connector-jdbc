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

package org.apache.flink.connector.jdbc.databases.oracle.table;

import org.apache.flink.connector.jdbc.databases.oracle.OracleTestBase;
import org.apache.flink.connector.jdbc.databases.oracle.dialect.OracleDialect;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSourceITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;

/** The Table Source ITCase for {@link OracleDialect}. */
public class OracleDynamicTableSourceITCase extends JdbcDynamicTableSourceITCase
        implements OracleTestBase {

    @Override
    protected TableRow createInputTable() {
        return tableRow(
                "jdbDynamicTableSource",
                field("id", dbType("INTEGER"), DataTypes.BIGINT().notNull()),
                field("decimal_col", DataTypes.DECIMAL(10, 4)),
                field("timestamp6_col", dbType("TIMESTAMP"), DataTypes.TIMESTAMP(6)),
                // other fields
                field("float_col", dbType("FLOAT"), DataTypes.FLOAT()),
                field("double_col", dbType("DOUBLE PRECISION"), DataTypes.DOUBLE()),
                field("binary_float_col", dbType("BINARY_FLOAT"), DataTypes.FLOAT()),
                field("binary_double_col", dbType("BINARY_DOUBLE"), DataTypes.DOUBLE()),
                field("char_col", dbType("CHAR"), DataTypes.CHAR(1)),
                field("nchar_col", dbType("NCHAR(3)"), DataTypes.VARCHAR(3)),
                field("varchar2_col", dbType("VARCHAR2(30)"), DataTypes.VARCHAR(30)),
                field("date_col", dbType("DATE"), DataTypes.DATE()),
                field("timestamp9_col", dbType("TIMESTAMP(9)"), DataTypes.TIMESTAMP(9)),
                field("clob_col", dbType("CLOB"), DataTypes.STRING()));
    }

    protected List<Row> getTestData() {
        return Arrays.asList(
                Row.of(
                        1L,
                        BigDecimal.valueOf(100.1234),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                        1.12345F,
                        2.12345678790D,
                        1.175E-10F,
                        1.79769E+40D,
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
                        1.12345F,
                        2.12345678790D,
                        1.175E-10F,
                        1.79769E+40D,
                        "a",
                        "abc",
                        "abcdef",
                        LocalDate.parse("1997-01-02"),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456789"),
                        "Hey Leonard"));
    }
}
