package org.apache.flink.connector.jdbc.databases.postgres.xa;

import org.apache.flink.connector.jdbc.databases.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.databases.postgres.PostgresDatabase;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;

/**
 * A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. Check for issues with suspending
 * connections (requires pooling) and honoring limits (properly closing connections).
 */
public class PostgresExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest
        implements PostgresTestBase {

    @Override
    public DatabaseMetadata getMetadata() {
        return PostgresDatabase.getMetadata();
    }

    @Override
    public SerializableSupplier<XADataSource> getDataSourceSupplier() {
        return () -> PostgresDatabase.getMetadata().buildXaDataSource();
    }
}
