package org.apache.flink.connector.jdbc.databases.postgres;

import org.apache.flink.connector.jdbc.databases.DatabaseExtension;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.DockerImageVersions;

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/** Postgres database for testing. * */
public class PostgresDatabase extends DatabaseExtension {

    private static final PostgreSQLContainer<?> container =
            new PostgreSQLContainer<>(DockerImageVersions.POSTGRES)
                    .withLogConsumer(
                            new Slf4jLogConsumer(LoggerFactory.getLogger(PostgresDatabase.class)));

    private static DatabaseMetadata metadata;

    public static DatabaseMetadata getMetadata() {
        return metadata;
    }

    @Override
    protected Lifecycle getLifecycle() {
        return Lifecycle.PER_CLASS;
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        container.start();
        metadata = new PostgresMetadata(container);
        return metadata;
    }

    @Override
    protected void stopDatabase() throws Exception {
        container.stop();
    }
}
