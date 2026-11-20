/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableIdTest {

    @Test
    void testConstructorRejectsNulls() {
        assertThatThrownBy(() -> new TableId(null, "schema", "table"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new TableId("catalog", null, "table"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new TableId("catalog", "schema", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testAccessors() {
        TableId tableId = new TableId("catalog", "schema", "table");

        assertThat(tableId.catalogName()).isEqualTo("catalog");
        assertThat(tableId.schemaName()).isEqualTo("schema");
        assertThat(tableId.tableName()).isEqualTo("table");
    }

    @Test
    void testToString() {
        TableId tableId = new TableId("catalog", "schema", "table");

        assertThat(tableId).hasToString("catalog.schema.table");
    }

    @Test
    void testBuilder() {
        TableId tableId =
                TableId.builder()
                        .withCatalogName("catalog")
                        .withSchemaName("schema")
                        .withTableName("table")
                        .build();

        assertThat(tableId).isEqualTo(new TableId("catalog", "schema", "table"));
    }

    @Test
    void testEqualsAndHashCode() {
        TableId a = new TableId("catalog", "schema", "table");
        TableId b = new TableId("catalog", "schema", "table");
        TableId different = new TableId("catalog", "schema", "other");

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(different);
        assertThat(a).isNotEqualTo(null);
        assertThat(a).isNotEqualTo("not a TableId");
        assertThat(a).isEqualTo(a);
    }
}
