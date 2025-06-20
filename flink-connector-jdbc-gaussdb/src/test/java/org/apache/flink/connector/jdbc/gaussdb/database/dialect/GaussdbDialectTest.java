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

package org.apache.flink.connector.jdbc.gaussdb.database.dialect;

import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectTest;
import org.apache.flink.connector.jdbc.gaussdb.GaussdbTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The PostgresSql params for {@link JdbcDialectTest}.
 *
 * <p>Notes: The source code is based on PostgresDialectTest.
 */
class GaussdbDialectTest extends JdbcDialectTest implements GaussdbTestBase {

    @Override
    protected List<TestItem> testData() {
        return Arrays.asList(
                createTestItem("CHAR"),
                createTestItem("VARCHAR"),
                createTestItem("BOOLEAN"),
                createTestItem("TINYINT"),
                createTestItem("SMALLINT"),
                createTestItem("INTEGER"),
                createTestItem("BIGINT"),
                createTestItem("FLOAT"),
                createTestItem("DOUBLE"),
                createTestItem("DECIMAL(10, 4)"),
                createTestItem("DECIMAL(38, 18)"),
                createTestItem("DATE"),
                createTestItem("TIME"),
                createTestItem("TIMESTAMP(3)"),
                createTestItem("TIMESTAMP WITHOUT TIME ZONE"),
                createTestItem("VARBINARY"),
                createTestItem("ARRAY<INTEGER>"),

                // Not valid data
                createTestItem("BINARY", "The Gaussdb dialect doesn't support type: BINARY(1)."),
                createTestItem(
                        "VARBINARY(10)",
                        "The Gaussdb dialect doesn't support type: VARBINARY(10)."),
                createTestItem(
                        "TIMESTAMP(9) WITHOUT TIME ZONE",
                        "The precision of field 'f0' is out of the TIMESTAMP precision range [0, 6] supported by Gaussdb dialect."),
                createTestItem("TIMESTAMP_LTZ(3)", "Unsupported type:TIMESTAMP_LTZ(3)"));
    }

    @Test
    void testUpsertStatement() {
        GaussdbDialect dialect = new GaussdbDialect();
        final String tableName = "tbl";
        final String[] fieldNames = {
            "id", "name", "email", "ts", "field1", "field_2", "__field_3__"
        };
        final String[] doUpdatekeyFields = {"id", "__field_3__"};
        final String[] doNothingkeyFields = {
            "id", "name", "email", "ts", "field1", "field_2", "__field_3__"
        };

        assertThat(dialect.getUpsertStatement(tableName, fieldNames, doUpdatekeyFields).get())
                .isEqualTo(
                        "INSERT INTO tbl(id, name, email, ts, field1, field_2, __field_3__) VALUES (:id, :name, :email, :ts, :field1, :field_2, :__field_3__) ON DUPLICATE KEY UPDATE name=values(name), email=values(email), ts=values(ts), field1=values(field1), field_2=values(field_2)");
        assertThat(dialect.getUpsertStatement(tableName, fieldNames, doNothingkeyFields).get())
                .isEqualTo(
                        "INSERT INTO tbl(id, name, email, ts, field1, field_2, __field_3__) VALUES (:id, :name, :email, :ts, :field1, :field_2, :__field_3__) ON DUPLICATE KEY UPDATE ");
    }
}
