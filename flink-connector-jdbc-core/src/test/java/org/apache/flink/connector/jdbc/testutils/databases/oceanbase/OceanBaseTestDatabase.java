package org.apache.flink.connector.jdbc.testutils.databases.oceanbase;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;

/** OceanBase database for locally testing. */
public class OceanBaseTestDatabase extends DatabaseExtension {

    public static OceanBaseMetadata getMetadata() {
        return new OceanBaseMetadata(
                System.getenv("test.oceanbase.username"),
                System.getenv("test.oceanbase.password"),
                System.getenv("test.oceanbase.url"),
                "com.oceanbase.jdbc.Driver",
                "test");
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        return getMetadata();
    }

    @Override
    protected void stopDatabase() throws Exception {}
}
