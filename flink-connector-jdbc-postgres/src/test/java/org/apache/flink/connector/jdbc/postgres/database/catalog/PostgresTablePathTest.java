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

package org.apache.flink.connector.jdbc.postgres.database.catalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PostgresTablePath}. */
class PostgresTablePathTest {
    @Test
    void testToFlinkTableName() {
        assertThat(PostgresTablePath.toFlinkTableName("my_schema", "my_table"))
                .isEqualTo("my_schema.my_table");
        assertThat(PostgresTablePath.toFlinkTableName("postgres.my_schema", "my_table"))
                .isEqualTo("postgres.my_schema.my_table");
        assertThatThrownBy(() -> PostgresTablePath.toFlinkTableName("", "my_table"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Schema name is not valid. Null or empty is not allowed");
    }

    @Test
    void testFromFlinkTableName() {
        assertThat(PostgresTablePath.fromFlinkTableName("my_schema.my_table"))
                .isEqualTo(new PostgresTablePath("my_schema", "my_table"));
        assertThat(PostgresTablePath.fromFlinkTableName("my_table"))
                .isEqualTo(new PostgresTablePath("public", "my_table"));
        assertThatThrownBy(() -> PostgresTablePath.fromFlinkTableName("postgres.public.my_table"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Table name 'postgres.public.my_table' is not valid. The parsed length is 3");
        assertThatThrownBy(() -> PostgresTablePath.fromFlinkTableName(""))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Table name is not valid. Null or empty is not allowed");
    }
}
