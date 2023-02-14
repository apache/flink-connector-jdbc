package org.apache.flink.connector.jdbc.databases.oracle.xa;

import org.apache.flink.connector.jdbc.databases.oracle.OracleTestBase;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;

/** A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. */
public class OracleExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest
        implements OracleTestBase {}
