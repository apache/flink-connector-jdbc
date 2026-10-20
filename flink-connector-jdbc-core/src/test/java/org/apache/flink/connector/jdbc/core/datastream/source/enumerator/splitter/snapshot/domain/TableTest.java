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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TableTest {

    private static final TableId TABLE_ID = new TableId("catalog", "schema", "table");

    @Test
    void testAccessors() {
        Table table = new Table(TABLE_ID, setOf("p1", "p2"));

        assertThat(table.tableId()).isEqualTo(TABLE_ID);
        assertThat(table.partitions()).containsExactlyInAnyOrder("p1", "p2");
    }

    @Test
    void testEqualsAndHashCode() {
        Table a = new Table(TABLE_ID, Collections.singleton("p1"));
        Table b = new Table(TABLE_ID, Collections.singleton("p1"));
        Table differentPartitions = new Table(TABLE_ID, Collections.singleton("p2"));
        Table differentTableId =
                new Table(new TableId("catalog", "schema", "other"), Collections.singleton("p1"));

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(differentPartitions);
        assertThat(a).isNotEqualTo(differentTableId);
        assertThat(a).isNotEqualTo(null);
        assertThat(a).isNotEqualTo("not a Table");
    }

    @Test
    void testToStringContainsTableIdAndPartitions() {
        Table table = new Table(TABLE_ID, Collections.singleton("p1"));

        assertThat(table.toString()).contains("catalog.schema.table").contains("p1");
    }

    private static Set<String> setOf(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }
}
