package org.apache.flink.connector.jdbc.databases.mysql.xa;

import org.apache.flink.connector.jdbc.databases.mysql.MySqlTestBase;
import org.apache.flink.connector.jdbc.testutils.databases.mysql.MySqlDatabase;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;

/**
 * A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. Check for issues with errors on
 * closing connections.
 */
public class MySqlExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest
        implements MySqlTestBase {

    @Override
    public SerializableSupplier<XADataSource> getDataSourceSupplier() {
        return () -> MySqlDatabase.getMetadata().buildXaDataSource();
    }
}
