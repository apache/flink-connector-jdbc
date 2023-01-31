package org.apache.flink.connector.jdbc.dialect.oracle;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.oracle.OracleDatabase;
import org.apache.flink.connector.jdbc.databases.oracle.OracleMetadata;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;
import org.apache.flink.util.function.SerializableSupplier;

import oracle.jdbc.xa.client.OracleXADataSource;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import javax.sql.XADataSource;

import java.sql.SQLException;

/** A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. */
@DisabledOnOs(OS.MAC)
public class OracleExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest
        implements OracleDatabase {

    @Override
    public DatabaseMetadata getMetadata() {
        return new OracleMetadata(CONTAINER, true);
    }

    @Override
    public SerializableSupplier<XADataSource> getDataSourceSupplier() {
        return () -> {
            try {
                OracleXADataSource xaDataSource = new OracleXADataSource();
                xaDataSource.setURL(CONTAINER.getJdbcUrl());
                xaDataSource.setUser(CONTAINER.getUsername());
                xaDataSource.setPassword(CONTAINER.getPassword());
                return xaDataSource;
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        };
    }
}
