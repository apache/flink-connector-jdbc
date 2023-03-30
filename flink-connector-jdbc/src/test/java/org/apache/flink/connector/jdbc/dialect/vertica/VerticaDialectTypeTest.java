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

package org.apache.flink.connector.jdbc.dialect.vertica;

import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeTest;

import java.util.Arrays;
import java.util.List;

/** The Vertica params for {@link JdbcDialectTypeTest}. */
public class VerticaDialectTypeTest extends JdbcDialectTypeTest {

    @Override
    protected String testDialect() {
        return "vertica";
    }

    @Override
    protected List<TestItem> testData() {
        return Arrays.asList(
                createTestItem("BINARY"),
                createTestItem("VARBINARY"),
                createTestItem("BOOLEAN"),
                createTestItem("CHAR"),
                createTestItem("VARCHAR"),
                createTestItem("DATE"),
                createTestItem("TIME WITHOUT TIME ZONE"),
                createTestItem("TIMESTAMP WITHOUT TIME ZONE"),
                createTestItem("DOUBLE"),
                createTestItem("DECIMAL"),
                createTestItem("BIGINT"),

                // Not valid data
                createTestItem(
                        "TIMESTAMP_LTZ(3)",
                        "The Vertica dialect doesn't support type: TIMESTAMP_LTZ(3)."),
                createTestItem("TINYINT", "The Vertica dialect doesn't support type: TINYINT."),
                createTestItem("SMALLINT", "The Vertica dialect doesn't support type: SMALLINT."),
                createTestItem("INT", "The Vertica dialect doesn't support type: INT."),
                createTestItem("FLOAT", "The Vertica dialect doesn't support type: FLOAT."));
    }
}
