package org.apache.flink.connector.jdbc.dialect.postgres;

import org.apache.flink.connector.jdbc.DbMetadata;
import org.apache.flink.connector.jdbc.test.DockerImageVersions;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;
import org.apache.flink.util.function.SerializableSupplier;

import org.postgresql.xa.PGXADataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.XADataSource;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. Check for issues with suspending
 * connections (requires pooling) and honoring limits (properly closing connections).
 */
@Testcontainers
public class PostgresExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest {

    @Container
    private static final PostgreSQLContainer<?> CONTAINER =
            new PostgresXaContainer(DockerImageVersions.POSTGRES)
                    .withMaxConnections(PARALLELISM * 2)
                    .withMaxTransactions(50);

    @Override
    protected String getDockerVersion() {
        return CONTAINER.getDockerImageName();
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return new PostgresMetadata(CONTAINER);
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

    /** {@link PostgreSQLContainer} with XA enabled (by setting max_prepared_transactions). */
    public static class PostgresXaContainer extends PostgreSQLContainer<PostgresXaContainer> {
        private static final int SUPERUSER_RESERVED_CONNECTIONS = 1;
        private int maxConnections = SUPERUSER_RESERVED_CONNECTIONS + 1;
        private int maxTransactions = 1;

        public PostgresXaContainer(String dockerImageName) {
            super(DockerImageName.parse(dockerImageName));
        }

        public PostgresXaContainer withMaxConnections(int maxConnections) {
            checkArgument(
                    maxConnections > SUPERUSER_RESERVED_CONNECTIONS,
                    "maxConnections should be greater than superuser_reserved_connections");
            this.maxConnections = maxConnections;
            return this.self();
        }

        public PostgresXaContainer withMaxTransactions(int maxTransactions) {
            checkArgument(maxTransactions > 1, "maxTransactions should be greater 1");
            this.maxTransactions = maxTransactions;
            return this.self();
        }

        @Override
        public void start() {
            setCommand(
                    "postgres",
                    "-c",
                    "superuser_reserved_connections=" + SUPERUSER_RESERVED_CONNECTIONS,
                    "-c",
                    "max_connections=" + maxConnections,
                    "-c",
                    "max_prepared_transactions=" + maxTransactions,
                    "-c",
                    "fsync=off");
            super.start();
        }
    }
}
