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

package org.apache.flink.connector.jdbc.mysql.lineage;

import org.apache.flink.connector.jdbc.lineage.JdbcLocation;
import org.apache.flink.connector.jdbc.mysql.database.lineage.MySqlLocationExtractor;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** E2E test for {@link MySqlLocationExtractor}. */
public class MySqlLocationExtractorTest {
    private static final String BASE_URL = "mysql://10.1.0.0:33060/databasename";
    private static final String LOAD_BALANCE_URL =
            "mysql:loadbalance://10.1.0.0:33060/databasename";
    private static final String REPLICATION_URL = "mysql:replication://10.1.0.0:33060/databasename";
    private static final String BASE_SRV_URL = "mysql+srv://10.1.0.0:33060/databasename";
    private static final String LOAD_BALANCE_SRV_URL =
            "mysql+srv:loadbalance://10.1.0.0:33060/databasename";
    private static final String REPLICATION_SRV_URL =
            "mysql+srv:replication://10.1.0.0:33060/databasename";

    private final MySqlLocationExtractor extractor = new MySqlLocationExtractor();

    public static Collection<String> parameters() {
        return Arrays.asList(
                BASE_URL,
                LOAD_BALANCE_URL,
                REPLICATION_URL,
                BASE_SRV_URL,
                LOAD_BALANCE_SRV_URL,
                REPLICATION_SRV_URL);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testURL(String url) throws Exception {
        assertThat(extractor.isDefinedAt(url)).isTrue();
        verify(extractor.extract(url, new Properties()));
    }

    private void verify(JdbcLocation jdbcLocation) {
        assertThat(jdbcLocation.getScheme()).isEqualTo("mysql");
        assertThat(jdbcLocation.getAuthority()).isEqualTo(Optional.of("10.1.0.0:33060"));
        assertThat(jdbcLocation.getInstance()).isEqualTo(Optional.empty());
        assertThat(jdbcLocation.getDatabase()).isEqualTo(Optional.of("databasename"));
    }
}
