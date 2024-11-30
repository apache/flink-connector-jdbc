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

package org.apache.flink.connector.jdbc.oceanbase.database.lineage;

import org.apache.flink.connector.jdbc.lineage.JdbcLocation;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link OceanBaseLocationExtractor}. */
public class OceanBaseLocationExtractorTest {
    private static final String BASE_URL = "oceanbase://10.1.0.0:1001/databasename";
    private static final String HA_MODE_URL = "oceanbase:hamode://10.1.0.0:1001/databasename";
    private final OceanBaseLocationExtractor extractor = new OceanBaseLocationExtractor();

    public static Collection<String> parameters() {
        return Arrays.asList(BASE_URL, HA_MODE_URL);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testURL(String url) throws Exception {
        assertThat(extractor.isDefinedAt(url)).isTrue();
        verify(extractor.extract(url, new Properties()));
    }

    private void verify(JdbcLocation jdbcLocation) {
        assertThat(jdbcLocation.getScheme()).isEqualTo("oceanbase");
        assertThat(jdbcLocation.getAuthority()).isEqualTo(Optional.of("10.1.0.0:1001"));
        assertThat(jdbcLocation.getInstance()).isEqualTo(Optional.empty());
        assertThat(jdbcLocation.getDatabase()).isEqualTo(Optional.of("databasename"));
    }
}
