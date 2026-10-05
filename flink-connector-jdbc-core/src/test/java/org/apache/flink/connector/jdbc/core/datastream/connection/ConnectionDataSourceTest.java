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

package org.apache.flink.connector.jdbc.core.datastream.connection;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionDataSourceTest {

    @Test
    void testGetConnectionUsesDriverManagerWhenDriverNameIsNull() throws Exception {
        ConnectionOptions options =
                ConnectionOptions.builder().withUrl("jdbc:h2:mem:datasource_no_driver").build();
        ConnectionDataSource.DriverResolver resolver =
                () -> {
                    throw new AssertionError(
                            "Driver resolver should not be consulted when driverName is null");
                };
        ConnectionDataSource dataSource = new ConnectionDataSource(options, resolver);

        try (Connection connection = dataSource.getConnection()) {
            assertThat(connection.isValid(5)).isTrue();
        }
    }

    @Test
    void testGetConnectionDelegatesToDriverResolverWhenDriverNameIsSet() throws Exception {
        ConnectionOptions options =
                ConnectionOptions.builder()
                        .withUrl("jdbc:h2:mem:datasource_with_driver")
                        .withDriverName("org.h2.Driver")
                        .build();
        AtomicInteger resolveCalls = new AtomicInteger();
        ConnectionDataSource.DriverResolver resolver =
                () -> {
                    resolveCalls.incrementAndGet();
                    return new org.h2.Driver();
                };
        ConnectionDataSource dataSource = new ConnectionDataSource(options, resolver);

        try (Connection connection = dataSource.getConnection()) {
            assertThat(connection.isValid(5)).isTrue();
        }
        assertThat(resolveCalls).hasValue(1);
    }

    @Test
    void testGetConnectionWithCredentialsDelegatesToGetConnection() throws Exception {
        ConnectionOptions options =
                ConnectionOptions.builder()
                        .withUrl("jdbc:h2:mem:datasource_with_credentials")
                        .withDriverName("org.h2.Driver")
                        .build();
        AtomicInteger resolveCalls = new AtomicInteger();
        ConnectionDataSource.DriverResolver resolver =
                () -> {
                    resolveCalls.incrementAndGet();
                    return new org.h2.Driver();
                };
        ConnectionDataSource dataSource = new ConnectionDataSource(options, resolver);

        try (Connection connection = dataSource.getConnection("user", "pass")) {
            assertThat(connection.isValid(5)).isTrue();
        }
        assertThat(resolveCalls).hasValue(1);
    }

    @Test
    void testGetConnectionThrowsWhenResolvedDriverRejectsUrl() {
        ConnectionOptions options =
                ConnectionOptions.builder()
                        .withUrl("jdbc:h2:mem:datasource_rejected")
                        .withDriverName("does.not.matter")
                        .build();
        ConnectionDataSource dataSource =
                new ConnectionDataSource(options, NullConnectingDriver::new);

        assertThatThrownBy(dataSource::getConnection)
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("No suitable driver found");
    }

    @Test
    void testGetConnectionWrapsClassNotFoundExceptionFromResolver() {
        ConnectionOptions options =
                ConnectionOptions.builder()
                        .withUrl("jdbc:h2:mem:datasource_missing_class")
                        .withDriverName("does.not.exist")
                        .build();
        ConnectionDataSource.DriverResolver resolver =
                () -> {
                    throw new ClassNotFoundException("does.not.exist");
                };
        ConnectionDataSource dataSource = new ConnectionDataSource(options, resolver);

        assertThatThrownBy(dataSource::getConnection)
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Failed to load driver")
                .hasCauseInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void testLogWriterAndLoginTimeoutAreNoOps() throws SQLException {
        ConnectionOptions options =
                ConnectionOptions.builder().withUrl("jdbc:h2:mem:datasource_noops").build();
        ConnectionDataSource dataSource = new ConnectionDataSource(options, () -> null);

        assertThat(dataSource.getLogWriter()).isNull();
        dataSource.setLogWriter(null);
        dataSource.setLoginTimeout(30);
        assertThat(dataSource.getLoginTimeout()).isZero();
    }

    @Test
    void testGetParentLoggerThrowsUnsupported() {
        ConnectionOptions options =
                ConnectionOptions.builder().withUrl("jdbc:h2:mem:datasource_logger").build();
        ConnectionDataSource dataSource = new ConnectionDataSource(options, () -> null);

        assertThatThrownBy(dataSource::getParentLogger)
                .isInstanceOf(SQLFeatureNotSupportedException.class);
    }

    @Test
    void testUnwrapThrowsAndIsWrapperForIsFalse() {
        ConnectionOptions options =
                ConnectionOptions.builder().withUrl("jdbc:h2:mem:datasource_wrapper").build();
        ConnectionDataSource dataSource = new ConnectionDataSource(options, () -> null);

        assertThatThrownBy(() -> dataSource.unwrap(Driver.class)).isInstanceOf(SQLException.class);
        assertThat(dataSource.isWrapperFor(Driver.class)).isFalse();
    }
}
