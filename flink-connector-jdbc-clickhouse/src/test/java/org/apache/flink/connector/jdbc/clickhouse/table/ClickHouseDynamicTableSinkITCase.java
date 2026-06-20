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
import org.apache.flink.connector.jdbc.core.table.sink.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase.tableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;
import static org.assertj.core.api.Assertions.assertThat;

/** The Table Sink ITCase for {@link ClickHouseDialect}. */
class ClickHouseDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements ClickHouseTestBase {

    @Override
    protected TableRow createUpsertOutputTable() {
        return tableRow(
                "dynamicSinkForUpsert",
                pkField("cnt", dbType("Int64"), DataTypes.BIGINT().notNull()),
                pkField("lencnt", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("cTag", dbType("Int32"), DataTypes.INT().notNull()),
                field("ts", dbType("DateTime64(6)"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createAppendOutputTable() {
        return tableRow(
                "dynamicSinkForAppend",
                pkField("id", dbType("Int32"), DataTypes.INT().notNull()),
                field("num", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("ts", dbType("DateTime64(6)"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected TableRow createBatchOutputTable() {
        return tableRow(
                "dynamicSinkForBatch",
                field("NAME", dbType("String"), DataTypes.VARCHAR(20).notNull()),
                field("SCORE", dbType("Int64"), DataTypes.BIGINT().notNull()));
    }

    @Override
    protected TableRow createRealOutputTable() {
        return tableRow("REAL_TABLE", field("real_data", dbType("Float32"), DataTypes.FLOAT()));
    }

    @Override
    protected TableRow createCheckpointOutputTable() {
        return tableRow(
                "checkpointTable", field("id", dbType("Int64"), DataTypes.BIGINT().notNull()));
    }

    @Override
    protected TableRow createUserOutputTable() {
        return tableRow(
                "USER_TABLE",
                pkField("user_id", dbType("String"), DataTypes.VARCHAR(20).notNull()),
                pkField("user_name", dbType("String"), DataTypes.VARCHAR(20).notNull()),
                field("email", dbType("String"), DataTypes.VARCHAR(255)),
                field("balance", dbType("Decimal(18, 2)"), DataTypes.DECIMAL(18, 2)),
                field("entry_dttm", dbType("DateTime64(3)"), DataTypes.TIMESTAMP()),
                field(
                        "other_emails_array",
                        dbType("Array(String)"),
                        DataTypes.ARRAY(DataTypes.STRING())),
                field("other_ids_array", dbType("Array(Int32)"), DataTypes.ARRAY(DataTypes.INT())),
                field(
                        "add_info_map",
                        dbType("Map(Int32, String)"),
                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())),
                field("last_log_date", dbType("Date"), DataTypes.DATE()),
                field("balance2", dbType("Decimal(18, 2)"), DataTypes.DECIMAL(18, 2)));
    }

    @Override
    protected List<Row> testUserData() {
        return Arrays.asList(
                Row.of(
                        "user1",
                        "Tom",
                        "tom123@gmail.com",
                        new BigDecimal("8.10"),
                        LocalDateTime.parse("1999-06-08T10:12:11.301"),
                        new String[]{"tom3@gmail.com", "tom5@gmail.com"},
                        new Integer[]{81723, 12315},
                        new HashMap<Integer, String>() {
                            {
                                put(333, "A");
                                put(444, "B");
                            }
                        },
                        LocalDate.parse("2026-05-05"),
                        new BigDecimal("16.20")),
                Row.of(
                        "user3",
                        "Bailey",
                        "bailey@qq.com",
                        new BigDecimal("9.99"),
                        LocalDateTime.parse("1999-12-11T20:22:11.301"),
                        new String[]{"bll3@gmail.com", "bll5@gmail.com"},
                        new Integer[]{81623, 22371},
                        new HashMap<Integer, String>() {
                            {
                                put(111, "C");
                                put(666, "D");
                            }
                        },
                        LocalDate.parse("2026-05-10"),
                        new BigDecimal("19.98")),
                Row.of(
                        "user4",
                        "Tina",
                        "tina@gmail.com",
                        new BigDecimal("11.30"),
                        LocalDateTime.parse("2001-01-01T00:11:44.124"),
                        new String[]{"tina1@gmail.com", "tina6@gmail.com"},
                        new Integer[]{12415, 66423},
                        new HashMap<Integer, String>() {
                            {
                                put(999, "X");
                                put(222, "N");
                            }
                        },
                        LocalDate.parse("2026-01-05"),
                        new BigDecimal("22.60")));
    }

    @Override
    protected void testReadingFromChangelogSource() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.ofKind(
                                        RowKind.INSERT,
                                        "user1",
                                        "Tom",
                                        "tom123@gmail.com",
                                        new BigDecimal("8.10"),
                                        LocalDateTime.parse("1999-06-08T10:12:11.301"),
                                        new String[]{"tom3@gmail.com", "tom5@gmail.com"},
                                        new Integer[]{81723, 12315},
                                        new HashMap<Integer, String>() {
                                            {
                                                put(333, "A");
                                                put(444, "B");
                                            }
                                        },
                                        LocalDate.parse("2026-05-05")),
                                Row.ofKind(
                                        RowKind.INSERT,
                                        "user3",
                                        "Bailey",
                                        "bailey@qq.com",
                                        new BigDecimal("9.99"),
                                        LocalDateTime.parse("1999-12-11T20:22:11.301"),
                                        new String[]{"bll3@gmail.com", "bll5@gmail.com"},
                                        new Integer[]{81623, 22371},
                                        new HashMap<Integer, String>() {
                                            {
                                                put(111, "C");
                                                put(666, "D");
                                            }
                                        },
                                        LocalDate.parse("2026-05-10")),
                                Row.ofKind(
                                        RowKind.INSERT,
                                        "user4",
                                        "Tina",
                                        "tina@gmail.com",
                                        new BigDecimal("11.30"),
                                        LocalDateTime.parse("2001-01-01T00:11:44.124"),
                                        new String[]{"tina1@gmail.com", "tina6@gmail.com"},
                                        new Integer[]{12415, 66423},
                                        new HashMap<Integer, String>() {
                                            {
                                                put(999, "X");
                                                put(222, "N");
                                            }
                                        },
                                        LocalDate.parse("2026-01-05"))));

        String userTableLogs = "user_logs";
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s ( "
                                + "  user_id STRING, "
                                + "  user_name STRING, "
                                + "  email STRING, "
                                + "  balance DECIMAL(18,2), "
                                + "  entry_dttm TIMESTAMP(3), "
                                + "  other_emails_array ARRAY<STRING>, "
                                + "  other_ids_array ARRAY<INT>, "
                                + "  add_info_map MAP<INT, STRING>, "
                                + "  last_log_date DATE, "
                                + "  balance2 AS balance * 2 "
                                + ") WITH ( "
                                + " 'connector' = 'values', "
                                + " 'data-id' = '%s', "
                                + " 'changelog-mode' = 'I,UA,UB,D' "
                                + ")",
                        userTableLogs, dataId));

        String userTableSink = "user_sink";
        tEnv.executeSql(
                userOutputTable.getCreateQueryForFlink(
                        getMetadata(),
                        userTableSink,
                        Arrays.asList(
                                "'sink.buffer-flush.max-rows' = '2'",
                                "'sink.buffer-flush.interval' = '0'")));

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s SELECT * FROM %s", userTableSink, userTableLogs))
                .await();

        assertThat(userOutputTable.selectAllTable(getMetadata()))
                .containsExactlyInAnyOrderElementsOf(testUserData());
    }
}
