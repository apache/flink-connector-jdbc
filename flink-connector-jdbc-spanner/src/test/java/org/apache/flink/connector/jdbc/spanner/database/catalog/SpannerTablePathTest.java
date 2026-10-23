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

package org.apache.flink.connector.jdbc.spanner.database.catalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link SpannerTablePath}. */
class SpannerTablePathTest {
    @Test
    void testToFlinkTableName() {
        assertThat(SpannerTablePath.toFlinkTableName("test_schema", "test_table"))
                .isEqualTo("test_schema.test_table");
        assertThat(SpannerTablePath.toFlinkTableName("", "test_table")).isEqualTo("test_table");
        assertThat(SpannerTablePath.toFlinkTableName(null, "test_table")).isEqualTo("test_table");
    }

    @Test
    void testFromFlinkTableName() {
        assertThat(SpannerTablePath.fromFlinkTableName("test_schema.test_table"))
                .isEqualTo(new SpannerTablePath("test_schema", "test_table"));
        assertThat(SpannerTablePath.fromFlinkTableName("test_table"))
                .isEqualTo(new SpannerTablePath("", "test_table"));
        assertThatThrownBy(
                        () -> SpannerTablePath.fromFlinkTableName("test_db.test_schema.test_table"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Table name 'test_db.test_schema.test_table' is not valid. The parsed length is 3");
        assertThatThrownBy(() -> SpannerTablePath.fromFlinkTableName(""))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Table name is not valid. Null or empty is not allowed");
    }
}
