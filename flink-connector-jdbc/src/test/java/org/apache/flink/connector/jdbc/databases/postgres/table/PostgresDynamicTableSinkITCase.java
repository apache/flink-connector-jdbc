package org.apache.flink.connector.jdbc.databases.postgres.table;

import org.apache.flink.connector.jdbc.databases.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSinkITCase;

/** The Table Sink ITCase for Postgres. */
public class PostgresDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements PostgresTestBase {}
