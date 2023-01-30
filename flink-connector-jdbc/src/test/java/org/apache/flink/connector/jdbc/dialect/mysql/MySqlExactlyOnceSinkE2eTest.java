package org.apache.flink.connector.jdbc.dialect.mysql;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.mysql.MySqlMetadata;
import org.apache.flink.connector.jdbc.test.DockerImageVersions;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.SerializableSupplier;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.XADataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. Check for issues with errors on
 * closing connections.
 */
@Testcontainers
public class MySqlExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest {

    @Container
    private static final MySqlXaContainer CONTAINER =
            new MySqlXaContainer(DockerImageVersions.MYSQL)
                    .withLockWaitTimeout(
                            (CHECKPOINT_TIMEOUT_MS + TASK_CANCELLATION_TIMEOUT_MS) * 2);

    @Override
    public DatabaseMetadata getMetadata() {
        return new MySqlMetadata(CONTAINER);
    }

    @Override
    public SerializableSupplier<XADataSource> getDataSourceSupplier() {
        return () -> {
            MysqlXADataSource xaDataSource = new MysqlXADataSource();
            xaDataSource.setUrl(CONTAINER.getJdbcUrl());
            xaDataSource.setUser(CONTAINER.getUsername());
            xaDataSource.setPassword(CONTAINER.getPassword());
            return xaDataSource;
        };
    }

    /** {@link MySQLContainer} with XA enabled. */
    static class MySqlXaContainer extends MySQLContainer<MySqlXaContainer> {
        private long lockWaitTimeout = 0;
        private volatile InnoDbStatusLogger innoDbStatusLogger;

        public MySqlXaContainer(String dockerImageName) {
            super(DockerImageName.parse(dockerImageName));
        }

        public MySqlXaContainer withLockWaitTimeout(long lockWaitTimeout) {
            checkArgument(lockWaitTimeout >= 0, "lockWaitTimeout should be greater than 0");
            this.lockWaitTimeout = lockWaitTimeout;
            return this.self();
        }

        @Override
        public void start() {
            super.start();
            // prevent XAER_RMERR: Fatal error occurred in the transaction  branch - check your
            // data for consistency works for mysql v8+
            try (Connection connection =
                    DriverManager.getConnection(getJdbcUrl(), "root", getPassword())) {
                prepareDb(connection, lockWaitTimeout);
            } catch (SQLException e) {
                ExceptionUtils.rethrow(e);
            }

            this.innoDbStatusLogger =
                    new InnoDbStatusLogger(
                            getJdbcUrl(), "root", getPassword(), lockWaitTimeout / 2);
            innoDbStatusLogger.start();
        }

        @Override
        public void stop() {
            try {
                innoDbStatusLogger.stop();
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            } finally {
                super.stop();
            }
        }

        private void prepareDb(Connection connection, long lockWaitTimeout) throws SQLException {
            try (Statement st = connection.createStatement()) {
                st.execute("GRANT XA_RECOVER_ADMIN ON *.* TO '" + getUsername() + "'@'%'");
                st.execute("FLUSH PRIVILEGES");
                // if the reason of task cancellation failure is waiting for a lock
                // then failing transactions with a relevant message would ease debugging
                st.execute("SET GLOBAL innodb_lock_wait_timeout = " + lockWaitTimeout);
                // st.execute("SET GLOBAL innodb_status_output = ON");
                // st.execute("SET GLOBAL innodb_status_output_locks = ON");
            }
        }
    }

    /** InnoDB status logger. */
    static class InnoDbStatusLogger {
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
