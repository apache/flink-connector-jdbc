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

package org.apache.flink.connector.jdbc.dameng.testutils;

import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.apache.flink.util.Preconditions.checkArgument;

/** DaMeng database container for testing. */
public class DaMengContainer extends JdbcDatabaseContainer<DaMengContainer> {
    private static final Logger LOG = LoggerFactory.getLogger(DaMengContainer.class);

    private static final String IMAGE = "hanyf/damengdb:8-kylin10-ubuntu";
    private static final String DEFAULT_USER = "SYSDBA";
    private static final String DEFAULT_PASSWORD = "SYSDBAPASS";
    private static final String DRIVER_CLASS_NAME = "dm.jdbc.driver.DmDriver";
    private static final int DEFAULT_PORT = 5236;
    private static final String DATABASE_NAME = "DAMENG";

    private long lockWaitTimeout = 0;
    private boolean xaActive = false;
    private volatile DmStatusLogger dmStatusLogger;

    public DaMengContainer() {
        this(IMAGE);
    }

    public DaMengContainer(String dockerImageName) {
        super(DockerImageName.parse(dockerImageName));
        withExposedPorts(DEFAULT_PORT);
        withStartupTimeout(Duration.of(240, ChronoUnit.SECONDS));
    }

    public DaMengContainer withXa() {
        this.xaActive = true;
        return this.self();
    }

    public DaMengContainer withLockWaitTimeout(long lockWaitTimeout) {
        checkArgument(lockWaitTimeout >= 0, "lockWaitTimeout should be greater than 0");
        this.lockWaitTimeout = lockWaitTimeout;
        return this.self();
    }

    @Override
    public void start() {
        super.start();
        // Configure database after startup
        prepareDb();

        if (lockWaitTimeout > 0) {
            this.dmStatusLogger =
                    new DmStatusLogger(
                            getJdbcUrl(), getUsername(), getPassword(), lockWaitTimeout / 2);
            dmStatusLogger.start();
        }
    }

    @Override
    public void stop() {
        try {
            if (dmStatusLogger != null) {
                dmStatusLogger.stop();
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        } finally {
            super.stop();
        }
    }

    private void prepareDb() {
        LOG.info("DaMeng is skipping database configuration");
    }

    @Override
    public String getDriverClassName() {
        return DRIVER_CLASS_NAME;
    }

    @Override
    public String getJdbcUrl() {
        String additionalUrlParams = "";
        return "jdbc:dm://" + getHost() + ":" + getMappedPort(DEFAULT_PORT) + "/" + DATABASE_NAME + additionalUrlParams;
    }

    @Override
    public String getUsername() {
        return DEFAULT_USER;
    }

    @Override
    public String getPassword() {
        return DEFAULT_PASSWORD;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1 FROM DUAL";
    }

    /** DaMeng status logger. */
    public static class DmStatusLogger {
        private static final Logger LOG = LoggerFactory.getLogger(DmStatusLogger.class);
        private final Thread thread;
        private volatile boolean running;

        private DmStatusLogger(String url, String user, String password, long intervalMs) {
            running = true;
            thread =
                    new Thread(
                            () -> {
                                LOG.info("Logging DaMeng status every {}ms", intervalMs);
                                try (Connection connection =
                                        DriverManager.getConnection(url, user, password)) {
                                    while (running) {
                                        Thread.sleep(intervalMs);
                                        queryAndLog(connection);
                                    }
                                } catch (Exception e) {
                                    LOG.warn("failed", e);
                                } finally {
                                    LOG.info("Logging DaMeng status stopped");
                                }
                            });
        }

        public void start() {
            thread.start();
        }

        public void stop() throws InterruptedException {
            running = false;
            thread.join();
        }

        private void queryAndLog(Connection connection) throws SQLException {
            try (Statement st = connection.createStatement()) {
                showBlockedTrx(st);
                showAllSessions(st);
                showEngineStatus(st);
                if (LOG.isDebugEnabled()) {
                    showSystemInfo(st);
                }
            }
        }

        private void showSystemInfo(Statement st) throws SQLException {
            LOG.debug("DaMeng System Info");
            try (ResultSet rs = st.executeQuery("SELECT * FROM V$INSTANCE")) {
                while (rs.next()) {
                    LOG.debug("Instance info: {}, {}, {}",
                            rs.getString("INSTANCE_NAME"),
                            rs.getString("VERSION"),
                            rs.getString("STARTUP_TIME"));
                }
            }
        }

        private void showEngineStatus(Statement st) throws SQLException {
            LOG.debug("Engine status");
            try (ResultSet rs = st.executeQuery("SELECT * FROM V$SYSSTAT")) {
                while (rs.next()) {
                    LOG.debug("{}: {}", rs.getString("NAME"), rs.getString("VALUE"));
                }
            }
        }

        private void showAllSessions(Statement st) throws SQLException {
            LOG.debug("All Sessions");
            try {
                // Try using the session view of Dameng database
                ResultSet rs = st.executeQuery("SELECT * FROM V$SESSIONS WHERE STATUS = 'ACTIVE'");
                while (rs.next()) {
                    LOG.debug(
                            "SID: {}, USER: {}, STATUS: {}, SCHEMA: {}, SQL_TEXT: {}",
                            rs.getString("SESSIONID"),
                            rs.getString("USERNAME"),
                            rs.getString("STATUS"),
                            rs.getString("SCHEMANAME"),
                            rs.getString("SQL_TEXT"));
                }
                rs.close();
            } catch (SQLException e) {
                LOG.warn("Failed to query active sessions: {}", e.getMessage());
                // If something goes wrong, you can try to query Dameng's other system views
                try {
                    LOG.debug("Attempting to query session information from alternative view");
                    ResultSet rs = st.executeQuery("SELECT * FROM V$DM_SESSION WHERE ROWNUM <= 10");
                    while (rs.next()) {
                        LOG.debug("Session info: ID={}, USER={}",
                                rs.getString(1),
                                rs.getString("USERNAME"));
                    }
                    rs.close();
                } catch (SQLException ex) {
                    LOG.warn("Failed to get session information: {}", ex.getMessage());
                }
            }
        }

        private void showBlockedTrx(Statement st) throws SQLException {
            LOG.debug("Blocked Transactions");
            try (ResultSet rs =
                    st.executeQuery(
                            "SELECT HOLDING_SESSION, WAITING_SESSION, LOCK_TYPE, OBJECT_NAME AS LOCK_OBJECT " +
                                    "FROM V$LOCK WHERE LOCK_MODE = 'CONVERTING'")) {
                while (rs.next()) {
                    LOG.debug(
                            "holding session: {}, waiting session: {}, lock type: {}, lock object: {}",
                            rs.getString(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getString(4));
                }
            } catch (SQLException e) {
                LOG.warn("Failed to query blocked transactions: {}", e.getMessage());
                // If the query fails, you can try a more basic query
                try (ResultSet rs = st.executeQuery("SELECT * FROM V$LOCK LIMIT 10")) {
                    LOG.debug("V$LOCK columns available:");
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        LOG.debug("Column {}: {}", i, rs.getMetaData().getColumnName(i));
                    }
                } catch (SQLException ex) {
                    LOG.warn("Failed to get V$LOCK metadata: {}", ex.getMessage());
                }
            }
        }
    }
}
