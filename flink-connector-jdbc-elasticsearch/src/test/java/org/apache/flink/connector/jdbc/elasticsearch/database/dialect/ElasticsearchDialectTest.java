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

package org.apache.flink.connector.jdbc.elasticsearch.database.dialect;

import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectTest;
import org.apache.flink.connector.jdbc.elasticsearch.ElasticsearchTestBase;

import java.util.Arrays;
import java.util.List;

/** The Elasticsearch params for {@link JdbcDialectTest}. */
class ElasticsearchDialectTest extends JdbcDialectTest implements ElasticsearchTestBase {

    @Override
    protected List<TestItem> testData() {
        return Arrays.asList(
                createTestItem("VARCHAR"),
                createTestItem("BOOLEAN"),
                createTestItem("TINYINT"),
                createTestItem("SMALLINT"),
                createTestItem("INTEGER"),
                createTestItem("BIGINT"),
                createTestItem("FLOAT"),
                createTestItem("DOUBLE"),
                createTestItem("DATE"),
                createTestItem("TIMESTAMP(3)"),
                createTestItem("TIMESTAMP WITHOUT TIME ZONE"),
                createTestItem("VARBINARY"),

                // Not valid data
                createTestItem("CHAR", "The Elasticsearch dialect doesn't support type: CHAR(1)."),
                createTestItem(
                        "BINARY", "The Elasticsearch dialect doesn't support type: BINARY(1)."),
                createTestItem("TIME", "The Elasticsearch dialect doesn't support type: TIME(0)."),
                createTestItem(
                        "VARBINARY(10)",
                        "The Elasticsearch dialect doesn't support type: VARBINARY(10)."),
                createTestItem(
                        "DECIMAL(10, 4)",
                        "The Elasticsearch dialect doesn't support type: DECIMAL(10, 4)."));
    }
}
