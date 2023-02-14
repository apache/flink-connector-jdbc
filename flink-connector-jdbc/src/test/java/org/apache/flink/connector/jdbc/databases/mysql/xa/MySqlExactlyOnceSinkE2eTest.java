package org.apache.flink.connector.jdbc.databases.mysql.xa;

import org.apache.flink.connector.jdbc.databases.mysql.MySqlTestBase;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;

/**
 * A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. Check for issues with errors on
 * closing connections.
 */
public class MySqlExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest
        implements MySqlTestBase {}
