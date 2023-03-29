package org.apache.flink.connector.jdbc.dialect.mysql;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.mysql.MySqlDatabase;
import org.apache.flink.connector.jdbc.databases.mysql.MySqlMetadata;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;
import org.apache.flink.util.function.SerializableSupplier;

import com.mysql.cj.jdbc.MysqlXADataSource;

import javax.sql.XADataSource;

/**
 * A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. Check for issues with errors on
 * closing connections.
 */
public class MySqlExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest
        implements MySqlDatabase {

    @Override
    public DatabaseMetadata getMetadata() {
        return new MySqlMetadata(CONTAINER, true);
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
}
