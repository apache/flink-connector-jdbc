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

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

/**
 * Minimal {@link DataSource} that delegates connection creation to a {@link DriverResolver} instead
 * of resolving/loading the driver itself. Used to back a HikariCP pool so every pooled connection
 * is created via the same classloader-aware driver resolution as the rest of a {@link
 * ConnectionProvider} — in Flink, the JDBC driver lives in the user classloader, which HikariCP's
 * own driver-loading path may not see.
 */
@Internal
public class ConnectionDataSource implements DataSource {

    /** Resolves the JDBC {@link Driver} to use for new connections. */
    @Internal
    @FunctionalInterface
    public interface DriverResolver {
        Driver resolve() throws SQLException, ClassNotFoundException;
    }

    private final ConnectionOptions jdbcOptions;
    private final DriverResolver driverResolver;

    public ConnectionDataSource(ConnectionOptions jdbcOptions, DriverResolver driverResolver) {
        this.jdbcOptions = jdbcOptions;
        this.driverResolver = driverResolver;
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            String url = jdbcOptions.getDbURL();
            Properties props = jdbcOptions.getProperties();
            if (jdbcOptions.getDriverName() == null) {
                return DriverManager.getConnection(url, props);
            }
            Driver driver = driverResolver.resolve();
            Connection conn = driver.connect(url, props);
            if (conn == null) {
                throw new SQLException("No suitable driver found for " + url, "08001");
            }
            return conn;
        } catch (ClassNotFoundException e) {
            throw new SQLException("Failed to load driver", e);
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
        return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not a wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
