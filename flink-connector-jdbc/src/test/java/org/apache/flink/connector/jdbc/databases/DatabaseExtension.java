package org.apache.flink.connector.jdbc.databases;

import org.apache.flink.connector.jdbc.templates.TableManaged;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.junit.platform.commons.support.AnnotationSupport.findRepeatableAnnotations;

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

    private final String uniqueKey = this.getClass().getSimpleName();

    protected Lifecycle getLifecycle() {
        return Lifecycle.PER_EXECUTION;
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
            ExtensionContext context,
            BiConsumerWithException<TableManaged, Connection, SQLException> execute) {
        context.getTestClass()
                .filter(DatabaseBaseTest.class::isAssignableFrom)
                .ifPresent(
                        clazz -> {
                            DatabaseMetadata metadata =
                                    getStore(context).get(uniqueKey, DatabaseMetadata.class);
                            try (Connection conn = metadata.getConnection()) {
                                for (TableManaged table :
                                        getDatabaseBaseTest(clazz).getManagedTables()) {
                                    execute.accept(table, conn);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    private boolean ignoreTestDatabase(ExtensionContext context) {
        Set<String> dbExtensions = retrieveDatabaseExtensions(context);

        if (dbExtensions.size() > 1) {
            return uniqueKey.equals("DerbyDatabase") && dbExtensions.contains(uniqueKey);
        }
        return false;
    }

    @Override
    public final void beforeAll(ExtensionContext context) throws Exception {
        if (ignoreTestDatabase(context)) {
            return;
        }

        if (getStore(context).get(uniqueKey) == null) {
            getStore(context).put(uniqueKey, startDatabase());
        }

        getManagedTables(context, TableManaged::createTable);
    }

    @Override
    public final void beforeEach(ExtensionContext context) throws Exception {}

    @Override
    public final void afterEach(ExtensionContext context) throws Exception {
        if (ignoreTestDatabase(context)) {
            return;
        }
        getManagedTables(context, TableManaged::deleteTable);
    }

    @Override
    public final void afterAll(ExtensionContext context) throws Exception {
        if (ignoreTestDatabase(context)) {
            return;
        }
        getManagedTables(context, TableManaged::dropTable);
        if (Lifecycle.PER_CLASS == getLifecycle()) {
            stopDatabase();
            getStore(context).remove(uniqueKey, DatabaseMetadata.class);
        }
    }

    @Override
    public final void close() throws Throwable {
        if (Lifecycle.PER_EXECUTION == getLifecycle()) {
            stopDatabase();
        }
    }

    private Set<String> retrieveDatabaseExtensions(final ExtensionContext context) {

        BiFunction<ExtensionContext, Set<String>, Set<String>> retrieveExtensions =
                new BiFunction<ExtensionContext, Set<String>, Set<String>>() {

                    @Override
                    public Set<String> apply(ExtensionContext context, Set<String> acc) {
                        Set<String> current = new HashSet<>(acc);
                        current.addAll(
                                findRepeatableAnnotations(context.getElement(), ExtendWith.class)
                                        .stream()
                                        .flatMap(extendWith -> Arrays.stream(extendWith.value()))
                                        .filter(DatabaseExtension.class::isAssignableFrom)
                                        .map(Class::getSimpleName)
                                        .collect(Collectors.toSet()));

                        return context.getParent()
                                .map(extensionContext -> apply(extensionContext, current))
                                .orElse(current);
                    }
                };

        return retrieveExtensions.apply(context, new HashSet<>());
    }
}
