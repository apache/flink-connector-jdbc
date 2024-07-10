/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.testutils;

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
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.platform.commons.support.AnnotationSupport.findRepeatableAnnotations;

/** Database extension for testing. */
public abstract class DatabaseExtension
        implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    protected abstract DatabaseMetadata getMetadataDB();

    protected abstract DatabaseResource getResource();

    private final String uniqueKey = this.getClass().getSimpleName();
    private final String uniqueResource = String.format("%sResource", uniqueKey);

    private DatabaseTest getDatabaseBaseTest(Class<?> clazz) throws Exception {
        DatabaseTest dbClazz = null;
        for (Constructor<?> c : clazz.getDeclaredConstructors()) {
            c.setAccessible(true);
            dbClazz = (DatabaseTest) c.newInstance();
        }
        return dbClazz;
    }

    private void getManagedTables(
            ExtensionContext context,
            BiConsumerWithException<TableManaged, Connection, SQLException> execute) {
        getManagedTables(context, execute, 0);
    }

    private void getManagedTables(
            ExtensionContext context,
            BiConsumerWithException<TableManaged, Connection, SQLException> execute,
            int attempt) {
        context.getTestClass()
                .filter(DatabaseTest.class::isAssignableFrom)
                .ifPresent(
                        clazz -> {
                            DatabaseMetadata metadata = getMetadataDB();
                            if (metadata != null) {
                                try (Connection conn = metadata.getConnection()) {
                                    for (TableManaged table :
                                            getDatabaseBaseTest(clazz).getManagedTables()) {
                                        execute.accept(table, conn);
                                    }
                                } catch (Exception e) {
                                    if (attempt <= 1) {
                                        waitToDB();
                                        getManagedTables(context, execute, attempt + 1);
                                    } else {
                                        throw new RuntimeException(e);
                                    }
                                }
                            }
                        });
    }

    private void waitToDB() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private boolean ignoreTestDatabase(ExtensionContext context) {
        Set<String> dbExtensions = retrieveDatabaseExtensions(context);

        if (dbExtensions.size() > 1) {
            return uniqueKey.equals("DerbyDatabase") && dbExtensions.contains(uniqueKey);
        }
        return false;
    }

    private DatabaseResource getResource(ExtensionContext context) {
        return context.getRoot()
                .getStore(Namespace.GLOBAL)
                .getOrComputeIfAbsent(uniqueResource, startResource(), DatabaseResource.class);
    }

    @Override
    public final void beforeAll(ExtensionContext context) throws Exception {
        if (ignoreTestDatabase(context)) {
            return;
        }

        getResource(context);

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
        getResource(context);
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

    private Function<String, DatabaseResource> startResource() {
        return s -> {
            DatabaseResource resource = getResource();
            resource.start();
            return resource;
        };
    }
}
