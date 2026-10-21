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

package org.apache.flink.connector.jdbc.core.datastream.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.util.function.SerializableSupplier;

import org.junit.jupiter.api.Test;

import javax.sql.XADataSource;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the checks {@link JdbcSinkBuilder} runs on the client. */
class JdbcSinkBuilderTest {

    private static final String INSERT = "insert into t (id) values (?)";

    /** The checks run before any connection is made, so no data source is needed. */
    private static final SerializableSupplier<XADataSource> NO_DATA_SOURCE = () -> null;

    @Test
    void testExactlyOnceRejectsRetries() {
        assertThatThrownBy(
                        () ->
                                JdbcSink.<Integer>builder()
                                        .withQueryStatement(INSERT, (ps, id) -> ps.setInt(1, id))
                                        .withExecutionOptions(
                                                JdbcExecutionOptions.builder()
                                                        .withMaxRetries(1)
                                                        .build())
                                        .buildExactlyOnce(
                                                JdbcExactlyOnceOptions.defaults(), NO_DATA_SOURCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetries");
    }

    @Test
    void testExactlyOnceWithoutRetriesBuilds() {
        assertThatCode(
                        () ->
                                JdbcSink.<Integer>builder()
                                        .withQueryStatement(INSERT, (ps, id) -> ps.setInt(1, id))
                                        .withExecutionOptions(
                                                JdbcExecutionOptions.builder()
                                                        .withMaxRetries(0)
                                                        .build())
                                        .buildExactlyOnce(
                                                JdbcExactlyOnceOptions.defaults(), NO_DATA_SOURCE))
                .doesNotThrowAnyException();
    }

    /** {@code JdbcExecutionOptions.defaults()} retries three times. */
    @Test
    void testExactlyOnceRejectsTheDefaultExecutionOptions() {
        assertThatThrownBy(
                        () ->
                                JdbcSink.<Integer>builder()
                                        .withQueryStatement(INSERT, (ps, id) -> ps.setInt(1, id))
                                        .buildExactlyOnce(
                                                JdbcExactlyOnceOptions.defaults(), NO_DATA_SOURCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetries");
    }

    @Test
    void testAtLeastOnceAllowsRetries() {
        assertThatCode(
                        () ->
                                JdbcSink.<Integer>builder()
                                        .withQueryStatement(INSERT, (ps, id) -> ps.setInt(1, id))
                                        .withExecutionOptions(
                                                JdbcExecutionOptions.builder()
                                                        .withMaxRetries(3)
                                                        .build())
                                        .buildAtLeastOnce(
                                                new JdbcConnectionOptions
                                                                .JdbcConnectionOptionsBuilder()
                                                        .withUrl("jdbc:derby:memory:builder")
                                                        .build()))
                .doesNotThrowAnyException();
    }
}
