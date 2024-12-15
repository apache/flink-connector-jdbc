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

package org.apache.flink.connector.jdbc.sqlserver.database.lineage;

import org.apache.flink.connector.jdbc.lineage.JdbcLocation;

import org.junit.Test;

import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlServerLocationExtractor}. */
public class SqlServerLocationExtractorTest {
    private static final String BASE_URL =
            "sqlserver://localhost;encrypt=true;integratedSecurity=true;";
    private static final String INSTANCE_URL =
            "sqlserver://localhost;encrypt=true;instanceName=instance1;integratedSecurity=true;";
    private static final String NORMAL_URL =
            "sqlserver://localhost:1433;encrypt=true;databaseName=AdventureWorks;integratedSecurity=true;";
    private final SqlServerLocationExtractor extractor = new SqlServerLocationExtractor();

    @Test
    public void testBaseURL() throws Exception {
        assertThat(extractor.isDefinedAt(BASE_URL)).isTrue();
        JdbcLocation jdbcLocation = extractor.extract(BASE_URL, new Properties());
        assertThat(jdbcLocation.getScheme()).isEqualTo("sqlserver");
        assertThat(jdbcLocation.getAuthority()).isEqualTo(Optional.of("localhost:1433"));
        assertThat(jdbcLocation.getInstance()).isEqualTo(Optional.empty());
        assertThat(jdbcLocation.getDatabase()).isEqualTo(Optional.empty());
    }

    @Test
    public void testInstanceURL() throws Exception {
        assertThat(extractor.isDefinedAt(INSTANCE_URL)).isTrue();
        JdbcLocation jdbcLocation = extractor.extract(INSTANCE_URL, new Properties());
        assertThat(jdbcLocation.getScheme()).isEqualTo("sqlserver");
        assertThat(jdbcLocation.getAuthority()).isEqualTo(Optional.of("localhost:1433"));
        assertThat(jdbcLocation.getInstance()).isEqualTo(Optional.of("instance1"));
        assertThat(jdbcLocation.getDatabase()).isEqualTo(Optional.empty());
    }

    @Test
    public void testNormalURL() throws Exception {
        assertThat(extractor.isDefinedAt(NORMAL_URL)).isTrue();
        JdbcLocation jdbcLocation = extractor.extract(NORMAL_URL, new Properties());
        assertThat(jdbcLocation.getScheme()).isEqualTo("sqlserver");
        assertThat(jdbcLocation.getAuthority()).isEqualTo(Optional.of("localhost:1433"));
        assertThat(jdbcLocation.getInstance()).isEqualTo(Optional.empty());
        assertThat(jdbcLocation.getDatabase()).isEqualTo(Optional.of("AdventureWorks"));
    }
}
