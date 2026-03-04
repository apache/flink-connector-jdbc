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

package org.apache.flink.connector.jdbc.postgres.database.dialect;

import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectTest;
import org.apache.flink.connector.jdbc.postgres.PostgresTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** The PostgresSql params for {@link JdbcDialectTest}. */
class PostgresDialectTest extends JdbcDialectTest implements PostgresTestBase {

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
                createTestItem("BINARY", "The PostgreSQL dialect doesn't support type: BINARY(1)."),
                createTestItem(
                        "VARBINARY(10)",
                        "The PostgreSQL dialect doesn't support type: VARBINARY(10)."),
                createTestItem(
                        "TIMESTAMP(9) WITHOUT TIME ZONE",
                        "The precision of field 'f0' is out of the TIMESTAMP precision range [0, 6] supported by PostgreSQL dialect."),
                createTestItem("TIMESTAMP_LTZ(3)", "Unsupported type:TIMESTAMP_LTZ(3)"));
    }

    @Test
    void testUpsertStatement() {
        PostgresDialect dialect = new PostgresDialect();
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
                        "INSERT INTO tbl(id, name, email, ts, field1, field_2, __field_3__) VALUES (:id, :name, :email, :ts, :field1, :field_2, :__field_3__) ON CONFLICT (id, __field_3__) DO UPDATE SET name=EXCLUDED.name, email=EXCLUDED.email, ts=EXCLUDED.ts, field1=EXCLUDED.field1, field_2=EXCLUDED.field_2");
        assertThat(dialect.getUpsertStatement(tableName, fieldNames, doNothingkeyFields).get())
                .isEqualTo(
                        "INSERT INTO tbl(id, name, email, ts, field1, field_2, __field_3__) VALUES (:id, :name, :email, :ts, :field1, :field_2, :__field_3__) ON CONFLICT (id, name, email, ts, field1, field_2, __field_3__) DO NOTHING");
    }

    @Test
    void testBatchInsertStatementWithUnnest() {
        PostgresDialect dialect = new PostgresDialect();
        final String tableName = "users";
        final String[] fieldNames = {"id", "name", "age"};
        final String[] fieldTypes = {"INTEGER", "VARCHAR", "INTEGER"};

        Optional<String> result =
                dialect.getBatchInsertStatement(tableName, fieldNames, fieldTypes);

        assertThat(result).isPresent();
        String sql = result.get();

        // Verify SQL structure
        assertThat(sql).contains("INSERT INTO users");
        assertThat(sql).contains("(id, name, age)");
        assertThat(sql).contains("SELECT * FROM UNNEST");
        assertThat(sql).contains("?::INTEGER[]");
        assertThat(sql).contains("?::VARCHAR[]");
        assertThat(sql).contains("AS t(id, name, age)");

        // Verify exact format
        assertThat(sql)
                .isEqualTo(
                        "INSERT INTO users (id, name, age) SELECT * FROM UNNEST(?::INTEGER[], ?::VARCHAR[], ?::INTEGER[]) AS t(id, name, age)");
    }

    @Test
    void testBatchInsertStatementEmptyFields() {
        PostgresDialect dialect = new PostgresDialect();
        final String tableName = "users";
        final String[] fieldNames = {};
        final String[] fieldTypes = {};

        // Should return empty when no fields
        Optional<String> result =
                dialect.getBatchInsertStatement(tableName, fieldNames, fieldTypes);
        assertThat(result).isEmpty();
    }

    @Test
    void testBatchUpsertStatementWithUnnest() {
        PostgresDialect dialect = new PostgresDialect();
        final String tableName = "users";
        final String[] fieldNames = {"id", "name", "age"};
        final String[] fieldTypes = {"INTEGER", "VARCHAR", "INTEGER"};
        final String[] uniqueKeyFields = {"id"};

        Optional<String> result =
                dialect.getBatchUpsertStatement(
                        tableName, fieldNames, fieldTypes, uniqueKeyFields);

        assertThat(result).isPresent();
        String sql = result.get();

        // Verify SQL structure
        assertThat(sql).contains("INSERT INTO users");
        assertThat(sql).contains("SELECT * FROM UNNEST");
        assertThat(sql).contains("ON CONFLICT (id)");
        assertThat(sql).contains("DO UPDATE SET");
        assertThat(sql).contains("name=EXCLUDED.name");
        assertThat(sql).contains("age=EXCLUDED.age");

        // Should not update key field
        assertThat(sql).doesNotContain("id=EXCLUDED.id");
    }

    @Test
    void testBatchUpsertStatementDoNothing() {
        PostgresDialect dialect = new PostgresDialect();
        final String tableName = "users";
        final String[] fieldNames = {"id"};
        final String[] fieldTypes = {"INTEGER"};
        final String[] uniqueKeyFields = {"id"};

        Optional<String> result =
                dialect.getBatchUpsertStatement(
                        tableName, fieldNames, fieldTypes, uniqueKeyFields);

        assertThat(result).isPresent();
        String sql = result.get();

        // When all fields are keys, should DO NOTHING
        assertThat(sql).contains("ON CONFLICT (id) DO NOTHING");
        assertThat(sql).doesNotContain("DO UPDATE SET");
    }

    @Test
    void testExtractBaseType() {
        PostgresDialect dialect = new PostgresDialect();

        // Test through getBatchInsertStatement which uses extractBaseType internally
        Optional<String> result;

        // VARCHAR(255) should become VARCHAR
        result =
                dialect.getBatchInsertStatement(
                        "t", new String[] {"col1"}, new String[] {"VARCHAR(255)"});
        assertThat(result).isPresent();
        assertThat(result.get()).contains("?::VARCHAR[]");

        // NUMERIC(10,2) should become NUMERIC
        result =
                dialect.getBatchInsertStatement(
                        "t", new String[] {"col1"}, new String[] {"NUMERIC(10,2)"});
        assertThat(result).isPresent();
        assertThat(result.get()).contains("?::NUMERIC[]");

        // TEXT[] should become TEXT
        result =
                dialect.getBatchInsertStatement(
                        "t", new String[] {"col1"}, new String[] {"TEXT[]"});
        assertThat(result).isPresent();
        assertThat(result.get()).contains("?::TEXT[]");
    }

    @Test
    void testBatchStatementQueryPlanStability() {
        PostgresDialect dialect = new PostgresDialect();
        final String tableName = "users";
        final String[] fieldNames = {"id", "name"};
        final String[] fieldTypes = {"INTEGER", "VARCHAR"};

        // Get SQL multiple times (simulating different batch sizes at runtime)
        Optional<String> sql1 = dialect.getBatchInsertStatement(tableName, fieldNames, fieldTypes);
        Optional<String> sql2 = dialect.getBatchInsertStatement(tableName, fieldNames, fieldTypes);
        Optional<String> sql3 = dialect.getBatchInsertStatement(tableName, fieldNames, fieldTypes);

        // All should be present and IDENTICAL
        assertThat(sql1).isPresent();
        assertThat(sql2).isPresent();
        assertThat(sql3).isPresent();
        assertThat(sql1.get()).isEqualTo(sql2.get());
        assertThat(sql2.get()).isEqualTo(sql3.get());

        // This is the key benefit - same SQL string = single query plan in pg_stat_statements
        // regardless of the number of rows in the batch (determined at runtime by array size)
    }

    @Test
    void testGetArrayTypeName() {
        PostgresDialect dialect = new PostgresDialect();

        // Test common types
        assertThat(dialect.getArrayTypeName(new org.apache.flink.table.types.logical.BooleanType()))
                .isEqualTo("boolean");
        assertThat(dialect.getArrayTypeName(new org.apache.flink.table.types.logical.IntType()))
                .isEqualTo("integer");
        assertThat(dialect.getArrayTypeName(new org.apache.flink.table.types.logical.BigIntType()))
                .isEqualTo("bigint");
        assertThat(dialect.getArrayTypeName(new org.apache.flink.table.types.logical.VarCharType()))
                .isEqualTo("varchar");
        assertThat(dialect.getArrayTypeName(new org.apache.flink.table.types.logical.DoubleType()))
                .isEqualTo("double precision");
    }
}
