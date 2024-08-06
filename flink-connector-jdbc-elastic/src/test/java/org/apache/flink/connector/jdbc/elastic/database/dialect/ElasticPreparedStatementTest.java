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

package org.apache.flink.connector.jdbc.elastic.database.dialect;

import org.apache.flink.connector.jdbc.core.database.JdbcFactoryLoader;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ElasticPreparedStatementTest}. */
public class ElasticPreparedStatementTest {

    private final JdbcDialect dialect =
            JdbcFactoryLoader.loadDialect(
                    "jdbc:elasticsearch://localhost:9200/test", getClass().getClassLoader());

    private final String[] fieldNames =
            new String[] {"id", "name", "email", "ts", "field1", "field_2", "__field_3__"};
    private final String[] keyFields = new String[] {"id", "__field_3__"};
    private final String tableName = "tbl";

    @Test
    void testRowExistsStatement() {
        String rowExistStmt = dialect.getRowExistsStatement(tableName, keyFields);
        assertThat(rowExistStmt)
                .isEqualTo(
                        "SELECT 1 FROM \"tbl\" WHERE \"id\" = :id AND \"__field_3__\" = :__field_3__");
    }

    @Test
    void testSelectStatement() {
        String selectStmt = dialect.getSelectFromStatement(tableName, fieldNames, keyFields);
        assertThat(selectStmt)
                .isEqualTo(
                        "SELECT \"id\", \"name\", \"email\", \"ts\", \"field1\", \"field_2\", \"__field_3__\" "
                                + "FROM \"tbl\" "
                                + "WHERE \"id\" = :id AND \"__field_3__\" = :__field_3__");
    }
}
