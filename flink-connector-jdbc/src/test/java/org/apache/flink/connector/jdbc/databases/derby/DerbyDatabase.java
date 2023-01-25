package org.apache.flink.connector.jdbc.databases.derby;

import org.apache.flink.connector.jdbc.databases.DatabaseExtension;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import java.io.OutputStream;
import java.sql.DriverManager;

/** Derby database for testing. * */
public class DerbyDatabase extends DatabaseExtension {

    protected Lifecycle getLifecycle() {
        return Lifecycle.PER_CLASS;
    }

    @SuppressWarnings("unused") // used in string constant in prepareDatabase
    protected static final OutputStream DEV_NULL =
            new OutputStream() {
                @Override
                public void write(int b) {}
            };

    private static final DerbyMetadata metadata = new DerbyMetadata("test");

    public static DerbyMetadata getMetadata() {
        return metadata;
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        System.setProperty(
                "derby.stream.error.field", DerbyDatabase.class.getCanonicalName() + ".DEV_NULL");
        Class.forName(metadata.getDriverClass());

        DriverManager.getConnection(
                metadata.getInitUrl(), metadata.getUser(), metadata.getPassword());

        return metadata;
    }

    @Override
    protected void stopDatabase() throws Exception {}
}
