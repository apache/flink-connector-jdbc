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

package org.apache.flink.connector.jdbc.testutils.databases.mysql;

import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.flink.util.Preconditions.checkArgument;

/** {@link MySQLContainer} with Xa and LockDB. */
public class MySqlContainer extends MySQLContainer<MySqlContainer> {
    private long lockWaitTimeout = 0;
    private boolean xaActive = false;
    private volatile InnoDbStatusLogger innoDbStatusLogger;

    public MySqlContainer(String dockerImageName) {
        super(DockerImageName.parse(dockerImageName));
    }

    public MySqlContainer withXa() {
        this.xaActive = true;
        return this.self();
    }

    public MySqlContainer withLockWaitTimeout(long lockWaitTimeout) {
        checkArgument(lockWaitTimeout >= 0, "lockWaitTimeout should be greater than 0");
        this.lockWaitTimeout = lockWaitTimeout;
        return this.self();
    }

    @Override
    public void start() {
        super.start();
        // prevent XAER_RMERR: Fatal error occurred in the transaction  branch - check your
        // data for consistency works for mysql v8+
        prepareDb();

        if (lockWaitTimeout > 0) {
            this.innoDbStatusLogger =
                    new InnoDbStatusLogger(
                            getJdbcUrl(), "root", getPassword(), lockWaitTimeout / 2);
            innoDbStatusLogger.start();
        }
    }

    @Override
    public void stop() {
        try {
            if (innoDbStatusLogger != null) {
                innoDbStatusLogger.stop();
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        } finally {
            super.stop();
        }
    }

    private void prepareDb() {
        try (Connection connection =
                        DriverManager.getConnection(getJdbcUrl(), "root", getPassword());
                Statement st = connection.createStatement()) {
            st.execute(
                    String.format(
                            "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION",
                            getUsername()));
            st.execute("FLUSH PRIVILEGES");

            if (xaActive) {
                st.execute(
                        String.format("GRANT XA_RECOVER_ADMIN ON *.* TO '%s'@'%%'", getUsername()));
                st.execute("FLUSH PRIVILEGES");
            }
            // if the reason of task cancellation failure is waiting for a lock
            // then failing transactions with a relevant message would ease debugging
            if (lockWaitTimeout > 0) {
                st.execute("SET GLOBAL innodb_lock_wait_timeout = " + lockWaitTimeout);
                // st.execute("SET GLOBAL innodb_status_output = ON");
                // st.execute("SET GLOBAL innodb_status_output_locks = ON");
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /** InnoDB status logger. */
    public static class InnoDbStatusLogger {
        private static final Logger LOG = LoggerFactory.getLogger(InnoDbStatusLogger.class);
        private final Thread thread;
        private volatile boolean running;

        private InnoDbStatusLogger(String url, String user, String password, long intervalMs) {
            running = true;
            thread =
                    new Thread(
                            () -> {
                                LOG.info("Logging InnoDB status every {}ms", intervalMs);
                                try (Connection connection =
                                        DriverManager.getConnection(url, user, password)) {
                                    while (running) {
                                        Thread.sleep(intervalMs);
                                        queryAndLog(connection);
                                    }
                                } catch (Exception e) {
                                    LOG.warn("failed", e);
                                } finally {
                                    LOG.info("Logging InnoDB status stopped");
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
                showAllTrx(st);
                showEngineStatus(st);
                showRecoveredTrx(st);
                // additional query: show full processlist \G; -- only shows live
            }
        }

        private void showRecoveredTrx(Statement st) throws SQLException {
            try (ResultSet rs = st.executeQuery("xa recover convert xid ")) {
                while (rs.next()) {
                    LOG.debug(
                            "recovered trx: {} {} {} {}",
                            rs.getString(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getString(4));
                }
            }
        }

        private void showEngineStatus(Statement st) throws SQLException {
            LOG.debug("Engine status");
            try (ResultSet rs = st.executeQuery("show engine innodb status")) {
                while (rs.next()) {
                    LOG.debug(rs.getString(3));
                }
            }
        }

        private void showAllTrx(Statement st) throws SQLException {
            LOG.debug("All TRX");
            try (ResultSet rs = st.executeQuery("select * from information_schema.innodb_trx")) {
                while (rs.next()) {
                    LOG.debug(
                            "trx_id: {}, trx_state: {}, trx_started: {}, trx_requested_lock_id: {}, trx_wait_started: {}, trx_mysql_thread_id: {},",
                            rs.getString("trx_id"),
                            rs.getString("trx_state"),
                            rs.getString("trx_started"),
                            rs.getString("trx_requested_lock_id"),
                            rs.getString("trx_wait_started"),
                            rs.getString("trx_mysql_thread_id") /* 0 for recovered*/);
                }
            }
        }

        private void showBlockedTrx(Statement st) throws SQLException {
            LOG.debug("Blocked TRX");
            try (ResultSet rs =
                    st.executeQuery(
                            " SELECT waiting_trx_id, waiting_pid, waiting_query, blocking_trx_id, blocking_pid, blocking_query "
                                    + "FROM sys.innodb_lock_waits; ")) {
                while (rs.next()) {
                    LOG.debug(
                            "waiting_trx_id: {}, waiting_pid: {}, waiting_query: {}, blocking_trx_id: {}, blocking_pid: {}, blocking_query: {}",
                            rs.getString(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getString(4),
                            rs.getString(5),
                            rs.getString(6));
                }
            }
        }
    }
}
