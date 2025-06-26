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

package org.apache.flink.connector.jdbc.lineage;

import io.openlineage.client.utils.jdbc.JdbcExtractor;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JdbcUtils}. */
class JdbcUtilsTest {
    private static final String TEST_JDBC_URL = "jdbc:test://localhost/testdb";
    private static final String UNSUPPORTED_JDBC_URL = "jdbc:unsupported://localhost:8990/testdb";

    @Test
    void testGetTestJdbcExtractor() throws Exception {
        JdbcExtractor jdbcExtractor = JdbcUtils.getExtractor("test://localhost/testdb");
        assertThat(jdbcExtractor).isInstanceOf(TestJdbcExtractor.class);
    }

    @Test
    void testGetUnsupportedJdbcExtractor() {
        assertThatThrownBy(
                        () -> {
                            JdbcUtils.getExtractor("unsupported://localhost:8990/testdb");
                        })
                .isInstanceOf(URISyntaxException.class)
                .hasMessage("Unsupported JDBC URL: unsupported://localhost:8990/testdb");
    }

    @Test
    void testGetValidJdbcNamespace() {
        String test = JdbcUtils.getJdbcNamespace(TEST_JDBC_URL, new Properties());
        assertThat(test).isEqualTo("test://localhost:10051");
    }

    @Test
    void testUnsupportedJdbcNamespace() {
        String test = JdbcUtils.getJdbcNamespace(UNSUPPORTED_JDBC_URL, new Properties());
        assertThat(test).isEqualTo("unsupported://localhost:8990/testdb");
    }
}
