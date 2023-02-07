package org.apache.flink.connector.jdbc.databases.derby;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.DatabaseTest;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.OutputStream;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Derby database for testing. */
public interface DerbyDatabase extends DatabaseTest {

    @SuppressWarnings("unused") // used in string constant in prepareDatabase
    OutputStream DEV_NULL =
            new OutputStream() {
                @Override
                public void write(int b) {}
            };

    DatabaseMetadata METADATA = startDatabase();

    @Override
    default DatabaseMetadata getMetadata() {
        return METADATA;
    }

    static DatabaseMetadata startDatabase() {
        DatabaseMetadata metadata = new DerbyMetadata("test");
        try {
            System.setProperty(
                    "derby.stream.error.field",
                    DerbyDatabase.class.getCanonicalName() + ".DEV_NULL");
            Class.forName(metadata.getDriverClass());
            DriverManager.getConnection(String.format("%s;create=true", metadata.getJdbcUrl()))
                    .close();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        return metadata;
    }

    default void stopDatabase() throws Exception {
        DatabaseMetadata metadata = getMetadata();
        try {
            DriverManager.getConnection(String.format("%s;shutdown=true", metadata.getJdbcUrl()))
                    .close();
        } catch (SQLException ignored) {
        }
    }
}
