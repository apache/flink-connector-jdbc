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

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link VerticaPreparedStatementTest}. */
public class VerticaPreparedStatementTest {

    private final JdbcDialect dialect =
            JdbcDialectLoader.load(
                    "jdbc:vertica://localhost:5433/test", getClass().getClassLoader());
    private final String[] fieldNames = new String[] {"id", "name", "ts", "field_2", "__field_3__"};
    private final String[] keyFields = new String[] {"id", "__field_3__"};
    private final String tableName = "tbl";

    @Test
    void testInsertStatement() {
        String insertStmt = dialect.getInsertIntoStatement(tableName, fieldNames);
        assertThat(insertStmt)
                .isEqualTo(
                        "INSERT INTO \"tbl\"(\"id\", \"name\", \"ts\", \"field_2\", \"__field_3__\") "
                                + "VALUES (:id, :name, :ts, :field_2, :__field_3__)");
    }

    @Test
    void testDeleteStatement() {
        String deleteStmt = dialect.getDeleteStatement(tableName, keyFields);
        assertThat(deleteStmt)
                .isEqualTo(
                        "DELETE FROM \"tbl\" WHERE \"id\" = :id AND \"__field_3__\" = :__field_3__");
    }

    @Test
    void testRowExistsStatement() {
        String rowExistStmt = dialect.getRowExistsStatement(tableName, keyFields);
        assertThat(rowExistStmt)
                .isEqualTo(
                        "SELECT 1 FROM \"tbl\" WHERE \"id\" = :id AND \"__field_3__\" = :__field_3__");
    }

    @Test
    void testUpdateStatement() {
        String updateStmt = dialect.getUpdateStatement(tableName, fieldNames, keyFields);
        assertThat(updateStmt)
                .isEqualTo(
                        "UPDATE \"tbl\" SET \"id\" = :id, \"name\" = :name, \"ts\" = :ts, "
                                + "\"field_2\" = :field_2, \"__field_3__\" = :__field_3__ "
                                + "WHERE \"id\" = :id AND \"__field_3__\" = :__field_3__");
    }

    @Test()
    void testUpsertStatement() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> dialect.getUpsertStatement(tableName, fieldNames, keyFields).get());
    }

    @Test
    void testSelectStatement() {
        String selectStmt = dialect.getSelectFromStatement(tableName, fieldNames, keyFields);
        assertThat(selectStmt)
                .isEqualTo(
                        "SELECT \"id\", \"name\", \"ts\", \"field_2\", \"__field_3__\" FROM \"tbl\" "
                                + "WHERE \"id\" = :id AND \"__field_3__\" = :__field_3__");
    }
}
