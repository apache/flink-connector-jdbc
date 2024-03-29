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

package org.apache.flink.connector.jdbc.databases.trino.dialect;

import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeTest;

import java.util.Arrays;
import java.util.List;

/** The Oracle params for {@link JdbcDialectTypeTest}. */
class TrinoDialectTypeTest extends JdbcDialectTypeTest {

    @Override
    protected String testDialect() {
        return "trino";
    }

    @Override
    protected List<TestItem> testData() {
        return Arrays.asList(
                createTestItem("BOOLEAN"),
                createTestItem("TINYINT"),
                createTestItem("SMALLINT"),
                createTestItem("INTEGER"),
                createTestItem("BIGINT"),
                createTestItem("DOUBLE"),
                createTestItem("FLOAT"),
                createTestItem("DECIMAL(10, 4)"),
                createTestItem("DECIMAL(38, 18)"),
                createTestItem("VARCHAR"),
                createTestItem("CHAR"),
                createTestItem("VARBINARY"),
                createTestItem("DATE"),
                createTestItem("TIME"),
                createTestItem("TIMESTAMP(3)"),
                createTestItem("TIMESTAMP WITHOUT TIME ZONE"),
                createTestItem("TIMESTAMP(9) WITHOUT TIME ZONE"),

                // Not valid data
                createTestItem("TIMESTAMP_LTZ(3)", "Unsupported type:TIMESTAMP_LTZ(3)"));
    }
}
