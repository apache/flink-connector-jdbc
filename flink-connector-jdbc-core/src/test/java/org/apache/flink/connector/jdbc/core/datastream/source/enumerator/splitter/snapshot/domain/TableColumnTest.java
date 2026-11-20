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

class TableColumnTest {

    @Test
    void testBuilderAndAccessors() {
        TableColumn column =
                TableColumn.builder()
                        .withColumnName("id")
                        .withColumnType("int8")
                        .withColumnPosition(1)
                        .withColumnNullable(false)
                        .withColumnPk(true)
                        .build();

        assertThat(column.columnName()).isEqualTo("id");
        assertThat(column.columnType()).isEqualTo("int8");
        assertThat(column.columnPosition()).isEqualTo(1);
        assertThat(column.columnNullable()).isFalse();
        assertThat(column.columnPrimaryKey()).isTrue();
    }

    @Test
    void testIsUuidColumnTypeIsCaseInsensitive() {
        assertThat(columnOfType("uuid").isUuidColumnType()).isTrue();
        assertThat(columnOfType("UUID").isUuidColumnType()).isTrue();
        assertThat(columnOfType("varchar").isUuidColumnType()).isFalse();
    }

    @Test
    void testEqualsAndHashCode() {
        TableColumn a = columnOfType("int8");
        TableColumn b = columnOfType("int8");
        TableColumn different = columnOfType("varchar");

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(different);
        assertThat(a).isNotEqualTo(null);
        assertThat(a).isNotEqualTo("not a TableColumn");
    }

    @Test
    void testToStringContainsFieldValues() {
        TableColumn column = columnOfType("int8");

        assertThat(column.toString()).contains("int8").contains("id");
    }

    private static TableColumn columnOfType(String columnType) {
        return TableColumn.builder()
                .withColumnName("id")
                .withColumnType(columnType)
                .withColumnPosition(1)
                .withColumnNullable(false)
                .withColumnPk(true)
                .build();
    }
}
