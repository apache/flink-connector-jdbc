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

package org.apache.flink.connector.jdbc.databases.oceanbase.dialect;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OceanBaseDialect}. */
public class OceanBaseDialectTest {

    @Test
    void testMysqlAppendDefaultUrlProperties() {
        OceanBaseDialect dialect = new OceanBaseDialect("mysql");
        String jdbcUrl = "jdbc:oceanbase://localhost:2883/foo";

        assertThat(dialect.appendDefaultUrlProperties(jdbcUrl))
                .isEqualTo(jdbcUrl + "?rewriteBatchedStatements=true");

        assertThat(dialect.appendDefaultUrlProperties(jdbcUrl + "?foo=bar"))
                .isEqualTo(jdbcUrl + "?foo=bar&rewriteBatchedStatements=true");

        assertThat(
                        dialect.appendDefaultUrlProperties(
                                jdbcUrl + "?foo=bar&rewriteBatchedStatements=false"))
                .isEqualTo(jdbcUrl + "?foo=bar&rewriteBatchedStatements=false");
    }

    @Test
    void testOracleAppendDefaultUrlProperties() {
        OceanBaseDialect dialect = new OceanBaseDialect("oracle");
        String jdbcUrl = "jdbc:oceanbase://localhost:2883/foo";

        assertThat(dialect.appendDefaultUrlProperties(jdbcUrl)).isEqualTo(jdbcUrl);
    }
}
