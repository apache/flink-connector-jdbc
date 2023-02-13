package org.apache.flink.connector.jdbc.testutils.databases.derby;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.OutputStream;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Derby database for testing. */
public class DerbyDatabase extends DatabaseExtension {

    @SuppressWarnings("unused") // used in string constant in prepareDatabase
    public static final OutputStream DEV_NULL =
            new OutputStream() {
                @Override
                public void write(int b) {}
            };

    private static DerbyMetadata metadata;

    public static DerbyMetadata getMetadata() {
        if (metadata == null) {
            metadata = new DerbyMetadata("test");
        }
        return metadata;
    }

    @Override
    public DatabaseMetadata startDatabase() throws Exception {
        DatabaseMetadata metadata = getMetadata();
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

    @Override
    protected void stopDatabase() throws Exception {
        try {
            DriverManager.getConnection(String.format("%s;shutdown=true", metadata.getJdbcUrl()))
                    .close();
        } catch (SQLException ignored) {
        } finally {
            metadata = null;
        }
    }
}
