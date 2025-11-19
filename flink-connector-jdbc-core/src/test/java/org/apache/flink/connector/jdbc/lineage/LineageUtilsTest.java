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

package org.apache.flink.connector.jdbc.lineage;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.SimpleJdbcQueryStatement;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LineageUtils}. */
class LineageUtilsTest {

    @Test
    void testGetNameFromJdbcQueryStatement() {
        SimpleJdbcQueryStatement<String> simpleStatement =
                new SimpleJdbcQueryStatement<String>("select * from test_table", null);

        Optional<String> tableNameOpt = LineageUtils.nameOf(simpleStatement, true);
        assertThat(tableNameOpt).isNotEmpty();
        assertThat(tableNameOpt.get()).isEqualTo("test_table");
    }

    @Test
    void testGetNamespaceFromJdbcConnectionProvider() {
        JdbcConnectionOptions options =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:test://localhost/test_db")
                        .build();
        SimpleJdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(options);
        String namespace = LineageUtils.namespaceOf(provider);
        assertThat(namespace).isEqualTo("test://localhost:10051");
    }
}
