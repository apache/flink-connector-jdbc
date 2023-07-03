package org.apache.flink.connector.jdbc.dialect.postgres;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.postgres.PostgresDatabase;
import org.apache.flink.connector.jdbc.databases.postgres.PostgresMetadata;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;
import org.apache.flink.util.function.SerializableSupplier;

import org.postgresql.xa.PGXADataSource;

import javax.sql.XADataSource;

/**
 * A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. Check for issues with suspending
 * connections (requires pooling) and honoring limits (properly closing connections).
 */
public class PostgresExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest
        implements PostgresDatabase {

    @Override
    public DatabaseMetadata getMetadata() {
        return new PostgresMetadata(CONTAINER, true);
    }

    @Override
    public SerializableSupplier<XADataSource> getDataSourceSupplier() {
        return () -> {
            PGXADataSource xaDataSource = new PGXADataSource();
            xaDataSource.setUrl(CONTAINER.getJdbcUrl());
            xaDataSource.setUser(CONTAINER.getUsername());
            xaDataSource.setPassword(CONTAINER.getPassword());
            return xaDataSource;
        };
    }
}
