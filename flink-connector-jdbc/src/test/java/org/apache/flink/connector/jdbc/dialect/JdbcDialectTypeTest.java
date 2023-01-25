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

package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for all DataTypes and Dialects of JDBC connector. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class JdbcDialectTypeTest {

    private static final String DDL_FORMAT =
            "CREATE TABLE T(\n"
                    + "f0 %s\n"
                    + ") WITH (\n"
                    + "  'connector'='jdbc',\n"
                    + "  'url'='jdbc:%s:memory:test',\n"
                    + "  'table-name'='myTable'\n"
                    + ")";

    protected abstract String testDialect();

    protected abstract List<TestItem> testData();

    protected TestItem createTestItem(String dataType) {
        return TestItem.of(testDialect(), dataType);
    }

    protected TestItem createTestItem(String dataType, String expectError) {
        return TestItem.of(testDialect(), dataType, expectError);
    }

    @ParameterizedTest
    @MethodSource("testData")
    void testDataTypeValidate(TestItem testItem) {
        String sqlDDL = String.format(DDL_FORMAT, testItem.dataTypeExpr, testItem.dialect);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(sqlDDL);

        if (testItem.expectError != null) {
            assertThatThrownBy(() -> tEnv.sqlQuery("SELECT * FROM T"))
                    .satisfies(anyCauseMatches(testItem.expectError));
        } else {
            tEnv.sqlQuery("SELECT * FROM T");
        }
    }

    // ~ Inner Class
    /** Test item for parameterized test. */
    public static class TestItem {
        private final String dialect;
        private final String dataTypeExpr;
        private final String expectError;

        private TestItem(String dialect, String dataTypeExpr, @Nullable String expectError) {
            this.dialect = dialect;
            this.dataTypeExpr = dataTypeExpr;
            this.expectError = expectError;
        }

        static TestItem of(String dialect, String dataTypeExpr) {
            return new TestItem(dialect, dataTypeExpr, null);
        }

        static TestItem of(String dialect, String dataTypeExpr, String expectError) {
            return new TestItem(dialect, dataTypeExpr, expectError);
        }

        @Override
        public String toString() {
            return String.format("Dialect: %s, DataType: %s", dialect, dataTypeExpr);
        }
    }
}
