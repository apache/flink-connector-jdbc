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

package org.apache.flink.connector.jdbc.spanner.database.lineage;

import org.apache.flink.connector.jdbc.lineage.JdbcUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SpannerJdbcExtractor}. */
class SpannerJdbcExtractorTest {

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                // Cloud Spanner with the default host
                "jdbc:cloudspanner:/projects/my-project/instances/my-instance/databases/my-db"
                        + "|spanner://my-project:my-instance/my-db",
                // Cloud Spanner with an explicit host and connection properties
                "jdbc:cloudspanner://spanner.googleapis.com/projects/my-project/instances/"
                        + "my-instance/databases/my-db;credentials=/path/to/key.json"
                        + "|spanner://my-project:my-instance/my-db",
                // Spanner emulator
                "jdbc:cloudspanner://localhost:9010/projects/test-project/instances/"
                        + "test-instance/databases/test-db;autoConfigEmulator=true"
                        + "|spanner://test-project:test-instance/test-db",
                // Catalog base-url without a database
                "jdbc:cloudspanner://localhost:9010/projects/test-project/instances/"
                        + "test-instance/databases/;autoConfigEmulator=true"
                        + "|spanner://test-project:test-instance",
            })
    void testNamespace(String jdbcUrl, String expectedNamespace) {
        assertThat(JdbcUtils.getJdbcNamespace(jdbcUrl, new Properties()))
                .isEqualTo(expectedNamespace);
    }
}
