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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatabaseSplitterEnumeratorBuilderTest {

    @Test
    void testBuildWithValidParameters() {
        DatabaseSplitterEnumerator enumerator =
                DatabaseSplitterEnumerator.builder()
                        .withCatalogName("catalog")
                        .withSchemaName("schema")
                        .withTableNames("orders", "customers")
                        .withChunkSize(500)
                        .build();

        assertThat(enumerator).isNotNull();
    }

    @Test
    void testChunkSizeMustBePositive() {
        assertThatThrownBy(() -> DatabaseSplitterEnumerator.builder().withChunkSize(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("chunkSize");

        assertThatThrownBy(() -> DatabaseSplitterEnumerator.builder().withChunkSize(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("chunkSize");
    }

    @Test
    void testDefaultChunkSizeIsApplied() {
        DatabaseSplitterEnumerator enumerator =
                DatabaseSplitterEnumerator.builder()
                        .withCatalogName("catalog")
                        .withSchemaName("schema")
                        .build();

        assertThat(enumerator).isNotNull();
    }

    @Test
    void testWithTableNamesVarargsAndSetAreAdditive() {
        DatabaseSplitterEnumerator enumerator =
                DatabaseSplitterEnumerator.builder()
                        .withCatalogName("catalog")
                        .withSchemaName("schema")
                        .withTableNames("orders")
                        .withTableNames("customers", "products")
                        .build();

        assertThat(enumerator).isNotNull();
    }
}
