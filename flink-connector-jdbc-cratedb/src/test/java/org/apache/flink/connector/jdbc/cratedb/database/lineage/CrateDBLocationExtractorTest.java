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

package org.apache.flink.connector.jdbc.cratedb.database.lineage;

import org.apache.flink.connector.jdbc.lineage.JdbcLocation;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** E2E test for {@link CrateLocationExtractor}. */
public class CrateDBLocationExtractorTest {
    private static final String BASE_URL = "crate://10.1.0.0:5432";
    private static final String MULTI_HOST_URL = "crate://10.1.0.0:5432,10.1.0.1:5432";

    private final CrateLocationExtractor extractor = new CrateLocationExtractor();

    @Test
    public void testURL() throws Exception {
        assertThat(extractor.isDefinedAt(BASE_URL)).isTrue();
        verify(extractor.extract(BASE_URL, new Properties()), "10.1.0.0:5432");
    }

    @Test
    public void testMultiHostURL() throws Exception {
        assertThat(extractor.isDefinedAt(MULTI_HOST_URL)).isTrue();
        verify(extractor.extract(MULTI_HOST_URL, new Properties()), "10.1.0.0:5432,10.1.0.1:5432");
    }

    private void verify(JdbcLocation jdbcLocation, String host) {
        assertThat(jdbcLocation.getScheme()).isEqualTo("crate");
        assertThat(jdbcLocation.getAuthority()).isEqualTo(Optional.of(host));
        assertThat(jdbcLocation.getInstance()).isEqualTo(Optional.empty());
        assertThat(jdbcLocation.getDatabase()).isEqualTo(Optional.empty());
    }
}
