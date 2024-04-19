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

package org.apache.flink.connector.jdbc;

import org.apache.flink.connector.jdbc.fakedb.FakeDBUtils;

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.USER_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JdbcConnectionOptions}. */
class JdbcConnectionOptionsTest {
    @Test
    void testNullUrl() {
        assertThatThrownBy(
                        () ->
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(null)
                                        .withUsername("user")
                                        .withPassword("password")
                                        .withDriverName(FakeDBUtils.DRIVER1_CLASS_NAME)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testNoOptionalOptions() {
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(FakeDBUtils.TEST_DB_URL)
                .build();
    }

    @Test
    void testInvalidCheckTimeoutSeconds() {
        assertThatThrownBy(
                        () ->
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(FakeDBUtils.TEST_DB_URL)
                                        .withUsername("user")
                                        .withPassword("password")
                                        .withDriverName(FakeDBUtils.DRIVER1_CLASS_NAME)
                                        .withConnectionCheckTimeoutSeconds(0)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testConnectionProperty() {
        // test for null connection properties
        JdbcConnectionOptions options =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(FakeDBUtils.TEST_DB_URL)
                        .build();
        assertThat(options.getProperties()).isEmpty();

        // test for useful connection properties
        options =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(FakeDBUtils.TEST_DB_URL)
                        .withProperty("keyA", "valueA")
                        .build();
        assertThat(options.getProperties()).hasSize(1);

        options =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(FakeDBUtils.TEST_DB_URL)
                        .withUsername("user")
                        .withProperty("keyA", "valueA")
                        .build();
        assertThat(options.getProperties()).hasSize(2);
        assertThat(options.getProperties()).hasFieldOrPropertyWithValue(USER_KEY, "user");
    }
}
