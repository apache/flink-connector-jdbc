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

package org.apache.flink.connector.jdbc.postgres.database.lineage;

import org.apache.flink.connector.jdbc.lineage.JdbcLocation;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PostgresLocationExtractor}. */
public class PostgresLocationExtractorTest {
    private static final String BASE_URL = "postgres:databasename";
    private static final String HOST_URL = "postgres://10.1.0.0/databasename";
    private static final String NORMAL_URL = "postgres://10.1.0.0:1234/databasename";

    private final PostgresLocationExtractor extractor = new PostgresLocationExtractor();

    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[][] {
                    {BASE_URL, Optional.of("localhost:5432")},
                    {HOST_URL, Optional.of("10.1.0.0:5432")},
                    {NORMAL_URL, Optional.of("10.1.0.0:1234")}
                });
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testURL(String url, Optional<String> authority) throws Exception {
        assertThat(extractor.isDefinedAt(url)).isTrue();
        verify(extractor.extract(url, new Properties()), authority);
    }

    private void verify(JdbcLocation jdbcLocation, Optional<String> authority) {
        assertThat(jdbcLocation.getScheme()).isEqualTo("postgres");
        assertThat(jdbcLocation.getAuthority()).isEqualTo(authority);
        assertThat(jdbcLocation.getInstance()).isEqualTo(Optional.empty());
        assertThat(jdbcLocation.getDatabase()).isEqualTo(Optional.of("databasename"));
    }
}
