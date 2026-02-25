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
                field("id", DataTypes.BIGINT().notNull()),
                field("decimal_col", DataTypes.DECIMAL(10, 4)),
                field("timestamp6_col", DataTypes.TIMESTAMP(6)),
                // other fields
                field("real_col", dbType("Float32"), DataTypes.FLOAT()),
                field("double_col", dbType("Float64"), DataTypes.DOUBLE()));
    }

    @Override
    protected List<Row> getTestData() {
        return Arrays.asList(
                Row.of(
                        1L,
                        BigDecimal.valueOf(100.1234),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                        1.175E-37F,
                        1.79769E308D),
                Row.of(
                        2L,
                        BigDecimal.valueOf(101.1234),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456"),
                        -1.175E-37F,
                        -1.79769E308));
    }

    @Override
    public String timestampFilterExpression() {
        return "timestamp6_col > TO_TIMESTAMP_LTZ('2020-01-01 15:35:00') "
                + "AND timestamp6_col < TO_TIMESTAMP_LTZ('2020-01-01 15:35:01')";
    }
}
