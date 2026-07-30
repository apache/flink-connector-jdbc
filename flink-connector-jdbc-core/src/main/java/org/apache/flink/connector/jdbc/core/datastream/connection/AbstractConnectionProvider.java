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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.util.Preconditions;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base {@link ConnectionProvider} that implements the connection lifecycle, pooling, and
 * prepared-statement plumbing shared by every dialect-specific provider. Subclasses only need to
 * implement the dialect-specific table discovery and bound-query building methods declared by
 * {@link ConnectionProvider} (e.g. {@code getTables}, {@code queryMinMax}, {@code
 * queryNextChunkMax}, {@code createQueryWithBounds}, {@code newInstance}).
 */
@Internal
public abstract class AbstractConnectionProvider implements ConnectionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractConnectionProvider.class);

    private static final int DEFAULT_POOL_SIZE = 4;
    private static final int MINIMUM_POOL_SIZE = 1;

    protected final ConnectionOptions jdbcOptions;
    private int queryTimeoutSeconds;
    private transient Driver loadedDriver;
    private transient Connection connection;
    private final Map<String, PreparedStatement> statementCache;
    private transient HikariDataSource connectionPool;
    private final boolean poolOwner;

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    protected AbstractConnectionProvider(ConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
        this.statementCache = new ConcurrentHashMap<>();
        this.queryTimeoutSeconds = jdbcOptions.getConnectionQueryTimeoutSeconds();
        this.poolOwner = true;
        try {
            // Establish the raw connection directly rather than via getOrEstablishConnection():
            // that method invokes the overridable onConnectionEstablished() hook, which a
            // subclass may depend on its own fields for — fields that aren't initialized yet
            // while this superclass constructor is still running.
            establishConnection();
        } catch (Exception e) {
            throw new ConnectionException(
                    "Failed to establish initial connection during connection provider construction.",
                    e);
        }
    }

    /**
     * Creates a pooled instance that borrows its connection from the given pool. This instance does
     * NOT own the pool and will not shut it down on close.
     */
    protected AbstractConnectionProvider(ConnectionOptions jdbcOptions, HikariDataSource pool) {
        this.jdbcOptions = jdbcOptions;
        this.statementCache = new ConcurrentHashMap<>();
        this.queryTimeoutSeconds = jdbcOptions.getConnectionQueryTimeoutSeconds();
        this.connectionPool = pool;
        this.poolOwner = false;
        try {
            this.connection = pool.getConnection();
        } catch (SQLException e) {
            throw new ConnectionException("Failed to borrow connection from pool.", e);
        }
    }

    protected synchronized HikariDataSource getOrCreatePool() {
        if (connectionPool == null) {
            connectionPool = createConnectionPool();
        }
        return connectionPool;
    }

    private HikariDataSource createConnectionPool() {
        HikariConfig config = new HikariConfig();
        // Provide a DataSource directly so HikariCP doesn't try to load the driver
        // via its own classloader. In Flink, the JDBC driver lives in the user
        // classloader and is not visible to HikariCP's threads. Using this driver
        // delegate guarantees connections are created with the same code path as
        // getOrEstablishConnection().
        config.setDataSource(new ConnectionDataSource(jdbcOptions, this::getLoadedDriver));
        config.setMinimumIdle(MINIMUM_POOL_SIZE);
        config.setMaximumPoolSize(maxPoolSize());
        config.setConnectionTimeout(
                Duration.ofSeconds(jdbcOptions.getConnectionCheckTimeoutSeconds()).toMillis());
        config.setPoolName(poolName());
        LOG.info("Creating HikariCP connection pool with maxPoolSize={}", maxPoolSize());
        return new HikariDataSource(config);
    }

    /** Maximum number of pooled connections. Override to change the pool size. */
    protected int maxPoolSize() {
        return DEFAULT_POOL_SIZE;
    }

    /** Name used for the HikariCP pool, shown in logs/metrics. */
    protected String poolName() {
        return getClass().getSimpleName() + "-pool";
    }

    /**
     * Hook invoked every time a connection is (re-)established, before it's handed back to the
     * caller. No-op by default; dialect-specific subclasses can override to re-apply
     * connection-scoped state (e.g. re-syncing a shared snapshot transaction id).
     */
    protected void onConnectionEstablished() throws SQLException {}

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Nonnull
    @Override
    public Properties getProperties() {
        return jdbcOptions.getProperties();
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return connection != null
                && !connection.isClosed()
                && connection.isValid(jdbcOptions.getConnectionCheckTimeoutSeconds());
    }

    private Driver loadDriver(String driverName) throws SQLException, ClassNotFoundException {
        Preconditions.checkNotNull(driverName);
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                return driver;
            }
        }
        // We could reach here for reasons:
        // * Class loader hell of DriverManager(see JDK-8146872).
        // * driver is not installed as a service provider.
        Class<?> clazz =
                Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            return (Driver) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new SQLException("Fail to create driver of class " + driverName, ex);
        }
    }

    private Driver getLoadedDriver() throws SQLException, ClassNotFoundException {
        if (loadedDriver == null) {
            loadedDriver = loadDriver(jdbcOptions.getDriverName());
        }
        return loadedDriver;
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (isConnectionValid()) {
            onConnectionEstablished();
            return connection;
        }
        establishConnection();
        onConnectionEstablished();
        return connection;
    }

    /**
     * Establishes a fresh {@link #connection}. Does not invoke {@link #onConnectionEstablished()}.
     */
    private Connection establishConnection() throws SQLException, ClassNotFoundException {
        String connectionUrl = jdbcOptions.getDbURL();
        if (jdbcOptions.getDriverName() == null) {
            connection = DriverManager.getConnection(connectionUrl, getProperties());
        } else {
            Driver driver = getLoadedDriver();
            connection = driver.connect(connectionUrl, getProperties());
            if (connection == null) {
                // Throw same exception as DriverManager.getConnection when no driver found to match
                // caller expectation.
                throw new SQLException("No suitable driver found for " + connectionUrl, "08001");
            }
        }
        return connection;
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }

    public void withQueryTimeout(Duration queryTimeout) {
        Objects.requireNonNull(queryTimeout, "queryTimeout must be provided");
        if (queryTimeout.isZero() || queryTimeout.isNegative()) {
            throw new IllegalArgumentException("queryTimeout must be positive");
        }
        this.queryTimeoutSeconds = (int) queryTimeout.getSeconds();
    }

    private Set<String> getPrimaryKeys(TableId tableId) {
        Set<String> primaryKeys = new HashSet<>();
        try (ResultSet rs =
                getOrEstablishConnection()
                        .getMetaData()
                        .getPrimaryKeys(
                                tableId.catalogName(), tableId.schemaName(), tableId.tableName())) {
            while (rs.next()) {
                primaryKeys.add(rs.getString(4));
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new ConnectionException("Failed to get primary keys for table " + tableId, e);
        }
        return primaryKeys;
    }

    @Override
    public Set<TableColumn> getTableColumns(TableId tableId) {
        Set<String> tablePrimaryKeys = getPrimaryKeys(tableId);
        Set<TableColumn> tableColumns = new LinkedHashSet<>();
        try (ResultSet rs =
                getOrEstablishConnection()
                        .getMetaData()
                        .getColumns(
                                tableId.catalogName(),
                                tableId.schemaName(),
                                tableId.tableName(),
                                (String) null)) {
            while (rs.next()) {
                String columnName = rs.getString(4);
                TableColumn column =
                        TableColumn.builder()
                                .withColumnName(columnName)
                                .withColumnType(rs.getString(6))
                                .withColumnPosition(rs.getInt(17))
                                .withColumnNullable(isNullable(rs.getInt(11)))
                                .withColumnPk(tablePrimaryKeys.contains(columnName))
                                .build();
                tableColumns.add(column);
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new ConnectionException(
                    String.format("Failed to get columns for table %s", tableId), e);
        }
        return tableColumns;
    }

    protected static boolean isNullable(int jdbcNullable) {
        return jdbcNullable == ResultSetMetaData.columnNullable
                || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
    }

    protected <T> T queryAndMap(String query, ResultSetMapper<T> mapper) {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        try (Statement statement = createStatement()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("running '{}' with {}s timeout", query, this.queryTimeoutSeconds);
            }

            try (ResultSet resultSet = statement.executeQuery(query)) {
                return mapper.apply(resultSet);
            }
        } catch (Exception e) {
            throw new ConnectionException(String.format("Failed executing query %s", query), e);
        }
    }

    private Statement createStatement() {
        try {
            final Statement statement = getOrEstablishConnection().createStatement();
            initializeStatement(statement);
            return statement;
        } catch (SQLException | ClassNotFoundException e) {
            throw new ConnectionException("Failed to create statement from factory", e);
        }
    }

    protected <T> T prepareQueryAndMap(
            String preparedQuery, StatementPreparer preparer, ResultSetMapper<T> mapper) {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        try {
            PreparedStatement statement = prepareQuery(preparedQuery, preparer);
            try (ResultSet resultSet = statement.executeQuery()) {
                return mapper.apply(resultSet);
            }
        } catch (Exception e) {
            throw new ConnectionException(
                    String.format("Failed executing query %s", preparedQuery), e);
        }
    }

    private PreparedStatement prepareQuery(String preparedQuery, StatementPreparer preparer) {
        try {
            PreparedStatement statement = this.createPreparedStatement(preparedQuery);
            preparer.accept(statement);
            return statement;
        } catch (SQLException e) {
            throw new ConnectionException("Failed to prepare query", e);
        }
    }

    private PreparedStatement createPreparedStatement(String preparedQueryString) {
        return this.statementCache.computeIfAbsent(
                preparedQueryString,
                (query) -> {
                    try {
                        LOG.trace(
                                "Inserting prepared statement '{}' that does not exist in the cache",
                                query);
                        PreparedStatement preparedStatement =
                                getOrEstablishConnection().prepareStatement(query);
                        initializeStatement(preparedStatement);
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(
                                    "PreparedStatement '{}' with {}s timeout",
                                    preparedQueryString,
                                    this.queryTimeoutSeconds);
                        }
                        return preparedStatement;
                    } catch (SQLException | ClassNotFoundException e) {
                        throw new ConnectionException(e);
                    }
                });
    }

    private void initializeStatement(Statement statement) {
        try {
            statement.setQueryTimeout(queryTimeoutSeconds);
        } catch (SQLException e) {
            throw new ConnectionException("Failed to add timeout to statement", e);
        }
    }

    private void closePreparedStatement(PreparedStatement statement) {
        LOG.trace("Closing prepared statement '{}' removed from cache", statement);
        try {
            statement.close();
        } catch (Exception e) {
            LOG.info("Exception while closing a prepared statement removed from cache", e);
        }
    }

    @Override
    public void closeConnection() {
        if (connection == null) {
            return;
        }
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Object> futureClose =
                executor.submit(
                        () -> {
                            this.connection.close();
                            LOG.info("Connection gracefully closed");
                            return null;
                        });

        try {
            futureClose.get(10L, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new ConnectionException(e.getCause());
        } catch (InterruptedException | TimeoutException e) {
            try {
                LOG.warn(
                        "Failed to close database connection by calling close(), attempting abort()");
                this.connection.abort(Runnable::run);
            } catch (SQLException ex) {
                throw new ConnectionException(ex);
            }
        } finally {
            executor.shutdownNow();
            try {
                if (this.connection != null
                        && !this.connection.isClosed()
                        && !connection.getAutoCommit()) {
                    this.connection.rollback();
                }
            } catch (SQLException e) {
                LOG.warn("Failed to rollback connection during close", e);
            } finally {
                this.statementCache.values().forEach(this::closePreparedStatement);
                this.statementCache.clear();
                this.connection = null;
            }
        }
        if (poolOwner && connectionPool != null) {
            LOG.info("Shutting down connection pool");
            connectionPool.close();
            connectionPool = null;
        }
    }
}
