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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.rules.TestName;

/** Plan tests for JDBC connector, for example, testing projection push down. */
public class JdbcTablePlanTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    private TestInfo testInfo;

    @BeforeEach
    void setup(TestInfo testInfo) {
        this.testInfo = testInfo;
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE jdbc ("
                                + "id BIGINT,"
                                + "timestamp6_col TIMESTAMP(6),"
                                + "timestamp9_col TIMESTAMP(9),"
                                + "time_col TIME,"
                                + "real_col FLOAT,"
                                + "double_col DOUBLE,"
                                + "decimal_col DECIMAL(10, 4)"
                                + ") WITH ("
                                + "  'connector'='jdbc',"
                                + "  'url'='jdbc:derby:memory:test',"
                                + "  'table-name'='test_table'"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE  d ( "
                                + "ip varchar(20), type int, age int"
                                + ") WITH ("
                                + "  'connector'='jdbc',"
                                + "  'url'='jdbc:derby:memory:test1',"
                                + "  'table-name'='d'"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE table_with_weird_column_name ( "
                                + "ip varchar(20), type int, ```?age:` int"
                                + ") WITH ("
                                + "  'connector'='jdbc',"
                                + "  'url'='jdbc:derby:memory:test1',"
                                + "  'table-name'='d'"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE a ( "
                                + " ip string, proctime as proctime() "
                                + ") WITH ("
                                + "  'connector'='jdbc',"
                                + "  'url'='jdbc:derby:memory:test2',"
                                + "  'table-name'='a'"
                                + ")");
    }

    @Test
    void testProjectionPushDown() {
        util.verifyExecPlan("SELECT decimal_col, timestamp9_col, id FROM jdbc");
    }

    @Test
    void testLimitPushDown() {
        util.verifyExecPlan("SELECT id, time_col FROM jdbc LIMIT 3");
    }

    @Test
    void testFilterPushdown() {
        util.verifyExecPlan(
                "SELECT id, time_col, real_col FROM jdbc WHERE id = 900001 AND time_col <> TIME '11:11:11' OR double_col >= -1000.23");
    }

    /**
     * Note the join condition is not present in the optimized plan, see FLINK-34170, as it is
     * handled in the JDBC java code, where it adds the join conditions to the select statement
     * string.
     */
    @Test
    void testLookupJoin() {
        util.verifyExecPlan(
                "SELECT * FROM a LEFT JOIN d FOR SYSTEM_TIME AS OF a.proctime ON a.ip = d.ip");
    }

    @Test
    void testLookupJoinWithFilter() {
        util.verifyExecPlan(
                "SELECT * FROM a LEFT JOIN d FOR SYSTEM_TIME AS OF a.proctime ON d.type = 0 AND a.ip = d.ip");
    }

    @Test
    void testLookupJoinWithANDAndORFilter() {
        util.verifyExecPlan(
                "SELECT * FROM a LEFT JOIN d FOR SYSTEM_TIME AS OF a.proctime ON ((d.age = 50 AND d.type = 0) "
                        + "OR (d.type = 1 AND d.age = 40)) AND a.ip = d.ip");
    }

    @Test
    void testLookupJoinWith2ANDsAndORFilter() {
        util.verifyExecPlan(
                "SELECT * FROM a JOIN d FOR SYSTEM_TIME AS OF a.proctime "
                        + "ON ((50 > d.age AND d.type = 1 AND d.age > 0 ) "
                        + "OR (70 > d.age AND d.type = 6 AND d.age > 10)) AND a.ip = d.ip");
    }

    @Test
    void testLookupJoinWithORFilter() {
        util.verifyExecPlan(
                "SELECT * FROM a LEFT JOIN d FOR SYSTEM_TIME AS OF a.proctime ON (d.age = 50 OR d.type = 1) AND a.ip = d.ip");
    }

    @Test
    void testLookupJoinWithWeirdColumnNames() {
        util.verifyExecPlan(
                "SELECT * FROM a LEFT JOIN table_with_weird_column_name FOR SYSTEM_TIME AS OF a.proctime "
                        + "ON (table_with_weird_column_name.```?age:` = 50 OR table_with_weird_column_name.type = 1) "
                        + "AND a.ip = table_with_weird_column_name.ip");
    }

    /**
     * Get the test method name, in order to adapt to {@link TableTestBase} that has not migrated to
     * Junit5. Remove it when dropping support of Flink 1.18.
     */
    public TestName name() {
        return new TestName() {
            @Override
            public String getMethodName() {
                return testInfo.getTestMethod().get().getName();
            }
        };
    }
}
