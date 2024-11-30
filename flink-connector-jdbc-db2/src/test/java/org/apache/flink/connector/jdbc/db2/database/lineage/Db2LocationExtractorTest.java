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

package org.apache.flink.connector.jdbc.db2.database.lineage;

import org.apache.flink.connector.jdbc.lineage.JdbcLocation;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for {@link org.apache.flink.connector.jdbc.db2.database.lineage.Db2LocationExtractor}.
 */
public class Db2LocationExtractorTest {
    private static final String BASE_URL = "db2://10.1.0.0:50000/BLUDB";
    private final Db2LocationExtractor extractor = new Db2LocationExtractor();

    @Test
    public void testURL() throws Exception {
        assertThat(extractor.isDefinedAt(BASE_URL)).isTrue();
        verify(extractor.extract(BASE_URL, new Properties()));
    }

    private void verify(JdbcLocation jdbcLocation) {
        assertThat(jdbcLocation.getScheme()).isEqualTo("db2");
        assertThat(jdbcLocation.getAuthority()).isEqualTo(Optional.of("10.1.0.0:50000"));
        assertThat(jdbcLocation.getInstance()).isEqualTo(Optional.empty());
        assertThat(jdbcLocation.getDatabase()).isEqualTo(Optional.of("BLUDB"));
    }
}
