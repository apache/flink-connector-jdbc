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

import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.Table;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableBounds;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AbstractConnectionProviderTest {

    private static final AtomicInteger DB_COUNTER = new AtomicInteger();

    @Test
    void testConstructorEstablishesConnectionButDoesNotInvokeHook() {
        TestConnectionProvider provider = newTestProvider();
        try {
            assertThat(provider.getConnection()).isNotNull();
            // The constructor bypasses onConnectionEstablished() on purpose: at this point a
            // subclass's own fields aren't initialized yet, so invoking an overridable hook here
            // would run against half-constructed state.
            assertThat(provider.onConnectionEstablishedCalls).hasValue(0);
        } finally {
            provider.closeConnection();
        }
    }

    @Test
    void testGetOrEstablishConnectionInvokesHookAndReusesValidConnection() throws Exception {
        TestConnectionProvider provider = newTestProvider();
        try {
            Connection first = provider.getOrEstablishConnection();
            Connection second = provider.getOrEstablishConnection();

            assertThat(second).isSameAs(first);
            assertThat(provider.onConnectionEstablishedCalls).hasValue(2);
        } finally {
            provider.closeConnection();
        }
    }

    @Test
    void testReestablishConnectionClosesOldAndOpensNew() throws Exception {
        TestConnectionProvider provider = newTestProvider();
        try {
            Connection first = provider.getConnection();
            Connection second = provider.reestablishConnection();

            assertThat(second).isNotSameAs(first);
            assertThat(first.isClosed()).isTrue();
            assertThat(provider.isConnectionValid()).isTrue();
        } finally {
            provider.closeConnection();
        }
    }

    @Test
    void testCloseConnectionIsIdempotentAndInvalidatesConnection() throws Exception {
        TestConnectionProvider provider = newTestProvider();

        provider.closeConnection();

        assertThat(provider.isConnectionValid()).isFalse();
        assertThat(provider.getConnection()).isNull();

        provider.closeConnection();
    }

    @Test
    void testConstructorThrowsConnectionExceptionWhenNoDriverAcceptsUrl() throws SQLException {
        NullConnectingDriver driver = new NullConnectingDriver();
        DriverManager.registerDriver(driver);
        try {
            ConnectionOptions options =
                    ConnectionOptions.builder()
                            .withUrl("jdbc:h2:mem:" + uniqueDbName())
                            .withDriverName(driver.getClass().getName())
                            .build();

            assertThatThrownBy(() -> new TestConnectionProvider(options))
                    .isInstanceOf(ConnectionException.class)
                    .hasMessageContaining("Failed to establish initial connection")
                    .cause()
                    .hasMessageContaining("No suitable driver found");
        } finally {
            DriverManager.deregisterDriver(driver);
        }
    }

    @Test
    void testGetTableColumnsReturnsRealMetadataWithPrimaryKeyFlag() throws Exception {
        TestConnectionProvider provider = newTestProvider();
        try {
            Connection connection = provider.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE ITEMS (ID BIGINT PRIMARY KEY, NAME VARCHAR(50) NOT NULL)");
            }
            TableId tableId =
                    TableId.builder()
                            .withCatalogName(connection.getCatalog())
                            .withSchemaName(connection.getSchema())
                            .withTableName("ITEMS")
                            .build();

            Set<TableColumn> columns = provider.getTableColumns(tableId);

            assertThat(columns).hasSize(2);
            TableColumn idColumn =
                    columns.stream()
                            .filter(c -> "ID".equals(c.columnName()))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            assertThat(idColumn.columnPrimaryKey()).isTrue();
            assertThat(idColumn.columnPosition()).isEqualTo(1);
            assertThat(idColumn.columnNullable()).isFalse();

            TableColumn nameColumn =
                    columns.stream()
                            .filter(c -> "NAME".equals(c.columnName()))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            assertThat(nameColumn.columnPrimaryKey()).isFalse();
            assertThat(nameColumn.columnNullable()).isFalse();
        } finally {
            provider.closeConnection();
        }
    }

    @Test
    void testQueryAndMapExecutesQueryAndAppliesMapper() {
        TestConnectionProvider provider = newTestProvider();
        try {
            Integer result =
                    provider.queryAndMap(
                            "SELECT 42",
                            rs -> {
                                rs.next();
                                return rs.getInt(1);
                            });

            assertThat(result).isEqualTo(42);
        } finally {
            provider.closeConnection();
        }
    }

    @Test
    void testPrepareQueryAndMapBindsParametersAndReusesCachedStatement() throws Exception {
        TestConnectionProvider provider = newTestProvider();
        try {
            try (Statement statement = provider.getConnection().createStatement()) {
                statement.execute("CREATE TABLE T (ID INT)");
                statement.execute("INSERT INTO T VALUES (1), (2), (3)");
            }

            Integer count =
                    provider.prepareQueryAndMap(
                            "SELECT COUNT(*) FROM T WHERE ID > ?",
                            ps -> ps.setInt(1, 1),
                            rs -> {
                                rs.next();
                                return rs.getInt(1);
                            });
            assertThat(count).isEqualTo(2);

            // Re-run the same query text a second time to exercise the prepared-statement cache.
            Integer countAgain =
                    provider.prepareQueryAndMap(
                            "SELECT COUNT(*) FROM T WHERE ID > ?",
                            ps -> ps.setInt(1, 0),
                            rs -> {
                                rs.next();
                                return rs.getInt(1);
                            });
            assertThat(countAgain).isEqualTo(3);
        } finally {
            provider.closeConnection();
        }
    }

    @Test
    void testWithQueryTimeoutRejectsZeroNegativeAndNullDurations() {
        TestConnectionProvider provider = newTestProvider();
        try {
            assertThatThrownBy(() -> provider.withQueryTimeout(Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> provider.withQueryTimeout(Duration.ofSeconds(-1)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> provider.withQueryTimeout(null))
                    .isInstanceOf(NullPointerException.class);
        } finally {
            provider.closeConnection();
        }
    }

    @Test
    void testMaxPoolSizeAndPoolNameDefaults() {
        TestConnectionProvider provider = newTestProvider();
        try {
            assertThat(provider.maxPoolSize()).isEqualTo(4);
            assertThat(provider.poolName()).isEqualTo("TestConnectionProvider-pool");
        } finally {
            provider.closeConnection();
        }
    }

    @Test
    void testGetOrCreatePoolCreatesPoolLazilyAndMemoizes() {
        TestConnectionProvider provider = newTestProvider();
        try {
            HikariDataSource first = provider.getOrCreatePool();
            HikariDataSource second = provider.getOrCreatePool();

            assertThat(first).isSameAs(second);
            assertThat(first.getMaximumPoolSize()).isEqualTo(4);
            assertThat(first.isClosed()).isFalse();
        } finally {
            provider.closeConnection();
        }
    }

    @Test
    void testCloseConnectionShutsDownOwnedPool() {
        TestConnectionProvider provider = newTestProvider();
        HikariDataSource pool = provider.getOrCreatePool();

        provider.closeConnection();

        assertThat(pool.isClosed()).isTrue();
    }

    @Test
    void testPooledConstructorBorrowsFromSharedPoolAndDoesNotOwnIt() throws Exception {
        TestConnectionProvider owner = newTestProvider();
        try {
            HikariDataSource pool = owner.getOrCreatePool();
            TestConnectionProvider borrower = new TestConnectionProvider(owner.jdbcOptions, pool);
            try {
                assertThat(borrower.getConnection()).isNotNull();
                assertThat(borrower.getConnection().isValid(5)).isTrue();
            } finally {
                borrower.closeConnection();
            }

            // The borrower doesn't own the pool, so closing it must not shut the pool down.
            assertThat(pool.isClosed()).isFalse();
        } finally {
            owner.closeConnection();
        }
    }

    private static String uniqueDbName() {
        return "abstract_provider_" + DB_COUNTER.incrementAndGet();
    }

    private static TestConnectionProvider newTestProvider() {
        ConnectionOptions options =
                ConnectionOptions.builder()
                        .withUrl("jdbc:h2:mem:" + uniqueDbName() + ";DB_CLOSE_DELAY=-1")
                        .withDriverName("org.h2.Driver")
                        .build();
        return new TestConnectionProvider(options);
    }

    /** Minimal concrete subclass exercising the shared connection lifecycle/pooling machinery. */
    private static final class TestConnectionProvider extends AbstractConnectionProvider {

        final AtomicInteger onConnectionEstablishedCalls = new AtomicInteger();

        TestConnectionProvider(ConnectionOptions jdbcOptions) {
            super(jdbcOptions);
        }

        TestConnectionProvider(ConnectionOptions jdbcOptions, HikariDataSource pool) {
            super(jdbcOptions, pool);
        }

        @Override
        protected void onConnectionEstablished() {
            onConnectionEstablishedCalls.incrementAndGet();
        }

        @Override
        public Set<Table> getTables(String catalog, String schema) {
            return Collections.emptySet();
        }

        @Override
        public String createQueryWithBounds(
                TableId tableId,
                Set<String> tableColumns,
                TableColumn pkColumn,
                TableBounds bounds) {
            return "";
        }

        @Override
        public TableBounds queryMinMax(TableId tableId, TableColumn column) {
            return TableBounds.empty();
        }

        @Override
        public Optional<Object> queryNextChunkMax(
                TableId tableId, TableColumn column, Object lowerBound, long chunkSize) {
            return Optional.empty();
        }

        @Override
        public ConnectionProvider newInstance() {
            return new TestConnectionProvider(jdbcOptions, getOrCreatePool());
        }
    }
}
