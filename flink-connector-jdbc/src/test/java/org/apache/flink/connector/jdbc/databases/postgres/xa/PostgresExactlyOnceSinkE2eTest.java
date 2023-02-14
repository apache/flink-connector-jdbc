package org.apache.flink.connector.jdbc.databases.postgres.xa;

import org.apache.flink.connector.jdbc.databases.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;

/**
 * A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. Check for issues with suspending
 * connections (requires pooling) and honoring limits (properly closing connections).
 */
public class PostgresExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest
        implements PostgresTestBase {}
