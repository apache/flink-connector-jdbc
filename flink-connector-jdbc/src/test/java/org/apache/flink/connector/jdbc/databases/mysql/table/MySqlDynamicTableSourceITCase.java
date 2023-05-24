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
import org.apache.flink.connector.jdbc.databases.mysql.dialect.MySqlDialect;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSourceITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;

/** The Table Source ITCase for {@link MySqlDialect}. */
public class MySqlDynamicTableSourceITCase extends JdbcDynamicTableSourceITCase
        implements MySqlTestBase {

    @Override
    protected TableRow createInputTable() {
        return tableRow(
                "jdbDynamicTableSource",
                field("id", DataTypes.BIGINT().notNull()),
                field("decimal_col", DataTypes.DECIMAL(10, 4)),
                field("timestamp6_col", DataTypes.TIMESTAMP(6)),
                // other fields
                field("real_col", dbType("REAL"), DataTypes.DOUBLE()),
                field("double_col", DataTypes.DOUBLE()),
                field("time_col", dbType("TIME"), DataTypes.TIME()),
                field("timestamp9_col", DataTypes.TIMESTAMP(6)));
    }

    protected List<Row> getTestData() {
        return Arrays.asList(
                Row.of(
                        1L,
                        BigDecimal.valueOf(100.1234),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                        1.175E-37D,
                        1.79769E308D,
                        LocalTime.parse("15:35"),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456")),
                Row.of(
                        2L,
                        BigDecimal.valueOf(101.1234),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456"),
                        -1.175E-37D,
                        -1.79769E308,
                        LocalTime.parse("15:36:01"),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456")));
    }
}
