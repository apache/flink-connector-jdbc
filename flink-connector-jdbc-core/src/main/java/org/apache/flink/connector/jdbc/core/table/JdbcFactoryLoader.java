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

package org.apache.flink.connector.jdbc.core.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.table.dialect.JdbcDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Utility for working with {@link JdbcDialect}. */
@Internal
public final class JdbcFactoryLoader {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcFactoryLoader.class);

    private JdbcFactoryLoader() {}

    public static JdbcDialect loadDialect(String url, ClassLoader classLoader) {
        return loadDialect(url, classLoader, null);
    }

    /**
     * Loads the unique JDBC Dialect that can handle the given database url.
     *
     * @param url A database URL.
     * @param classLoader the classloader used to load the factory.
     * @param compatibleMode the compatible mode of database.
     * @throws IllegalStateException if the loader cannot find exactly one dialect that can
     *     unambiguously process the given database URL.
     * @return The loaded dialect.
     */
    public static JdbcDialect loadDialect(
            String url, ClassLoader classLoader, String compatibleMode) {
        return load(url, classLoader).createDialect(compatibleMode);
    }

    //    public static JdbcCatalog loadCatalog(
    //            ClassLoader classLoader,
    //            String catalogName,
    //            String defaultDatabase,
    //            String username,
    //            String pwd,
    //            String baseUrl) {
    //        return load(baseUrl, classLoader)
    //                .createCatalog(
    //                        classLoader, catalogName, defaultDatabase, username, pwd, baseUrl,
    // null);
    //    }
    //
    //    public static JdbcCatalog loadCatalog(
    //            ClassLoader classLoader,
    //            String catalogName,
    //            String defaultDatabase,
    //            String username,
    //            String pwd,
    //            String baseUrl,
    //            String compatibleMode) {
    //        return load(baseUrl, classLoader)
    //                .createCatalog(
    //                        classLoader,
    //                        catalogName,
    //                        defaultDatabase,
    //                        username,
    //                        pwd,
    //                        baseUrl,
    //                        compatibleMode);
    //    }

    private static JdbcFactory load(String url, ClassLoader classLoader) {
        List<JdbcFactory> foundFactories = discoverFactories(classLoader);

        if (foundFactories.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any jdbc factories that implement '%s' in the classpath.",
                            JdbcFactory.class.getName()));
        }

        final List<JdbcFactory> matchingFactories =
                foundFactories.stream().filter(f -> f.acceptsURL(url)).collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any jdbc factory that can handle url '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factories are:\n\n"
                                    + "%s",
                            url,
                            JdbcFactory.class.getName(),
                            foundFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingFactories.size() > 1) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple jdbc factories can handle url '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            url,
                            JdbcFactory.class.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingFactories.get(0);
    }

    private static List<JdbcFactory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<JdbcFactory> result = new LinkedList<>();
            ServiceLoader.load(JdbcFactory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for jdbc factory.", e);
            throw new RuntimeException("Could not load service provider for jdbc factory.", e);
        }
    }
}
