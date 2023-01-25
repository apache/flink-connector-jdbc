package org.apache.flink.connector.jdbc.databases;

import org.apache.flink.connector.jdbc.templates.TableManaged;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.SQLException;

/** Database extension for testing. * */
public abstract class DatabaseExtension
        implements BeforeAllCallback,
                AfterAllCallback,
                BeforeEachCallback,
                AfterEachCallback,
                ExtensionContext.Store.CloseableResource {

    /** Database Lifecycle for testing. * */
    public enum Lifecycle {
        PER_EXECUTION,
        PER_CLASS
        //        PER_METHOD
    }

    protected abstract DatabaseMetadata startDatabase() throws Exception;

    protected abstract void stopDatabase() throws Exception;

    private final String uniqueKey = this.getClass().getName();

    protected Lifecycle getLifecycle() {
        return Lifecycle.PER_CLASS;
    }

    private ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getRoot().getStore(Namespace.GLOBAL);
    }

    private DatabaseBaseTest getDatabaseBaseTest(Class<?> clazz) throws Exception {
        DatabaseBaseTest dbClazz = null;
        for (Constructor<?> c : clazz.getDeclaredConstructors()) {
            c.setAccessible(true);
            dbClazz = (DatabaseBaseTest) c.newInstance();
        }
        return dbClazz;
    }

    private void getManagedTables(
            ExtensionContext context, ConsumerWithException<TableManaged, SQLException> execute) {
        context.getTestClass()
                .filter(DatabaseBaseTest.class::isAssignableFrom)
                .map(
                        clazz -> {
                            try {
                                for (TableManaged table :
                                        getDatabaseBaseTest(clazz).getManagedTables()) {
                                    execute.apply(table);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            return clazz;
                        });
    }

    @Override
    public final void beforeAll(ExtensionContext context) throws Exception {

        if (Lifecycle.PER_EXECUTION == getLifecycle() && getStore(context).get(uniqueKey) != null) {
            return;
        }

        DatabaseMetadata metadata = startDatabase();
        try (Connection conn = metadata.getConnection()) {
            getManagedTables(context, (table) -> table.createTable(conn));
        }
        getStore(context).put(uniqueKey, metadata);
    }

    @Override
    public final void beforeEach(ExtensionContext context) throws Exception {}

    @Override
    public final void afterEach(ExtensionContext context) throws Exception {
        DatabaseMetadata metadata = getStore(context).get(uniqueKey, DatabaseMetadata.class);
        try (Connection conn = metadata.getConnection()) {
            getManagedTables(context, (table) -> table.deleteTable(conn));
        }
    }

    @Override
    public final void afterAll(ExtensionContext context) throws Exception {

        DatabaseMetadata metadata = getStore(context).get(uniqueKey, DatabaseMetadata.class);

        try (Connection conn = metadata.getConnection()) {
            if (Lifecycle.PER_CLASS == getLifecycle()) {
                getManagedTables(context, (table) -> table.dropTable(conn));
                stopDatabase();
            }
        }
    }

    @Override
    public final void close() throws Throwable {
        if (Lifecycle.PER_EXECUTION == getLifecycle()) {
            stopDatabase();
        }
    }

    @FunctionalInterface
    private interface ConsumerWithException<T, E extends Throwable> {
        void apply(T var1) throws E;
    }
}
