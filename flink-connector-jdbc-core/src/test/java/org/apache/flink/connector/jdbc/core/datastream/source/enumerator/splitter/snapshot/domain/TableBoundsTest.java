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

import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThat;

class TableBoundsTest {

    @Test
    void testOfIsNotEmpty() {
        TableBounds bounds = TableBounds.of(1L, 10L);

        assertThat(bounds.isEmpty()).isFalse();
        assertThat(bounds.lowerBound()).isEqualTo(1L);
        assertThat(bounds.upperBound()).isEqualTo(10L);
    }

    @Test
    void testEmpty() {
        TableBounds bounds = TableBounds.empty();

        assertThat(bounds.isEmpty()).isTrue();
        assertThat(bounds.lowerBound()).isNull();
        assertThat(bounds.upperBound()).isNull();
    }

    @Test
    void testGetBoundsAsParamsForEmptyIsNull() {
        assertThat(TableBounds.empty().getBoundsAsParams()).isNull();
    }

    @Test
    void testGetBoundsAsParamsWithBothBounds() {
        Serializable[] params = TableBounds.of(1L, 10L).getBoundsAsParams();

        assertThat(params).containsExactly(1L, 10L);
    }

    @Test
    void testGetBoundsAsParamsWithOnlyLowerBound() {
        Serializable[] params = TableBounds.of(10L, null).getBoundsAsParams();

        assertThat(params).containsExactly(10L);
    }

    @Test
    void testGetBoundsAsParamsWithOnlyUpperBound() {
        Serializable[] params = TableBounds.of(null, 10L).getBoundsAsParams();

        assertThat(params).containsExactly(10L);
    }

    @Test
    void testEqualsAndHashCode() {
        TableBounds a = TableBounds.of(1L, 10L);
        TableBounds b = TableBounds.of(1L, 10L);
        TableBounds different = TableBounds.of(1L, 20L);

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(different);
        assertThat(a).isNotEqualTo(null);
        assertThat(a).isNotEqualTo("not a TableBounds");
    }

    @Test
    void testToStringForEmpty() {
        assertThat(TableBounds.empty()).hasToString("[]");
    }

    @Test
    void testToStringWithBothBounds() {
        assertThat(TableBounds.of(1L, 10L)).hasToString("[1,10]");
    }

    @Test
    void testToStringWithOnlyLowerBound() {
        assertThat(TableBounds.of(1L, null)).hasToString("[1,]");
    }

    @Test
    void testToStringWithOnlyUpperBound() {
        assertThat(TableBounds.of(null, 10L)).hasToString("[,10]");
    }
}
