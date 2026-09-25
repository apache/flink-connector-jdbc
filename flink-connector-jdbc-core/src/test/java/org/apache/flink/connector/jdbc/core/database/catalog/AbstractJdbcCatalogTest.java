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

package org.apache.flink.connector.jdbc.core.database.catalog;

import org.apache.flink.table.catalog.ObjectPath;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AbstractJdbcCatalog}. */
class AbstractJdbcCatalogTest {

    @Test
    void testValidJdbcUrl() {
        AbstractJdbcCatalog.validateJdbcUrl("jdbc:dialect://localhost:1234/db", "db");
        AbstractJdbcCatalog.validateJdbcUrl("jdbc:dialect://localhost:1234/db", null);
        AbstractJdbcCatalog.validateJdbcUrl("jdbc:dialect://localhost:1234/", "db");
        AbstractJdbcCatalog.validateJdbcUrl("jdbc:dialect://localhost:1234", "db");
    }

    @Test
    void testInvalidJdbcUrl() {
        assertThatThrownBy(
                        () ->
                                AbstractJdbcCatalog.validateJdbcUrl(
                                        "jdbc:dialect://localhost:1234", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AbstractJdbcCatalog.NO_DATABASES_HINT);
        assertThatThrownBy(
                        () ->
                                AbstractJdbcCatalog.validateJdbcUrl(
                                        "jdbc:dialect://localhost:1234/", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AbstractJdbcCatalog.NO_DATABASES_HINT);
        assertThatThrownBy(
                        () ->
                                AbstractJdbcCatalog.validateJdbcUrl(
                                        "jdbc:dialect://localhost:1234/db", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AbstractJdbcCatalog.DATABASE_NOT_UNIQUE_HINT);
        assertThatThrownBy(
                        () ->
                                AbstractJdbcCatalog.validateJdbcUrl(
                                        "jdbc:dialect://localhost:1234/db", "not_db"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AbstractJdbcCatalog.DATABASE_NOT_UNIQUE_HINT);
    }

    @Test
    void testDefaultDatabaseResolver() {
        String baseUrl = "jdbc:dialect://localhost:1234/custom/path/";
        AbstractJdbcCatalog catalog =
                new TestJdbcCatalog(
                        "db",
                        baseUrl,
                        (url, database) -> {
                            assertThat(url).isEqualTo(baseUrl);
                            assertThat(database).isEqualTo("db");
                            return "resolved_db";
                        });

        assertThat(catalog.getDefaultDatabase()).isEqualTo("resolved_db");
    }

    @Test
    void testDefaultResolverValidatesJdbcUrl() {
        assertThatThrownBy(() -> new TestJdbcCatalog("not_db", "jdbc:dialect://localhost:1234/db"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(AbstractJdbcCatalog.DATABASE_NOT_UNIQUE_HINT);
    }

    /** Minimal {@link AbstractJdbcCatalog} used to test the constructors. */
    private static class TestJdbcCatalog extends AbstractJdbcCatalog {

        TestJdbcCatalog(String defaultDatabase, String baseUrl) {
            super(
                    Thread.currentThread().getContextClassLoader(),
                    "catalog",
                    defaultDatabase,
                    baseUrl,
                    authProperties());
        }

        TestJdbcCatalog(
                String defaultDatabase,
                String baseUrl,
                BiFunction<String, String, String> defaultDatabaseResolver) {
            super(
                    Thread.currentThread().getContextClassLoader(),
                    "catalog",
                    defaultDatabase,
                    baseUrl,
                    authProperties(),
                    defaultDatabaseResolver);
        }

        private static Properties authProperties() {
            Properties properties = new Properties();
            properties.setProperty("user", "user");
            properties.setProperty("password", "password");
            return properties;
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String databaseName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(ObjectPath tablePath) {
            throw new UnsupportedOperationException();
        }
    }
}
