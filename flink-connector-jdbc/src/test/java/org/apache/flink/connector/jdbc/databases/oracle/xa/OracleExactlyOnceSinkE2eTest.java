package org.apache.flink.connector.jdbc.databases.oracle.xa;

import org.apache.flink.connector.jdbc.databases.oracle.OracleXaTestBase;
import org.apache.flink.connector.jdbc.testutils.databases.oracle.OracleXaDatabase;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;

/** A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. */
public class OracleExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest
        implements OracleXaTestBase {

    @Override
    public SerializableSupplier<XADataSource> getDataSourceSupplier() {
        return () -> OracleXaDatabase.getMetadata().buildXaDataSource();
    }
}
