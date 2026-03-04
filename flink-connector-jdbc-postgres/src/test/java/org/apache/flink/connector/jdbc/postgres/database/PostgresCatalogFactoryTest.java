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

package org.apache.flink.connector.jdbc.postgres.database;

import org.apache.flink.connector.jdbc.core.database.JdbcFactoryLoader;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactoryOptions;
import org.apache.flink.connector.jdbc.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.postgres.database.catalog.PostgresCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for OceanBase database catalog factory.
 *
 * <p>This test follows the same pattern as other database factory tests, verifying that {@link
 * PostgresCatalog} is correctly instantiated and configured.
 */
class PostgresCatalogFactoryTest implements PostgresTestBase {

    private static final String TEST_CATALOG_NAME = "mypg";
    private static final String DEFAULT_DATABASE = "test";

    @Test
    void testFactoryWithBaseUrlAndDefaultDatabase() {
        // Test case 1: base-url without database name + default-database = "postgres"
        // jdbc:postgresql://localhost:50807/postgres -> jdbc:postgresql://localhost:50807/
        String jdbcUrl = getMetadata().getJdbcUrl();
        String baseUrl = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"));

        JdbcCatalog expectedCatalog =
                JdbcFactoryLoader.loadCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        PostgresCatalog.DEFAULT_DATABASE,
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        baseUrl,
                        null);

        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), JdbcCatalogFactoryOptions.IDENTIFIER);
        options.put(
                JdbcCatalogFactoryOptions.DEFAULT_DATABASE.key(), PostgresCatalog.DEFAULT_DATABASE);
        options.put(JdbcCatalogFactoryOptions.USERNAME.key(), getMetadata().getUsername());
        options.put(JdbcCatalogFactoryOptions.PASSWORD.key(), getMetadata().getPassword());
        options.put(JdbcCatalogFactoryOptions.BASE_URL.key(), baseUrl);

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        TEST_CATALOG_NAME,
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader());

        assertThat(actualCatalog).isEqualTo(expectedCatalog).isInstanceOf(PostgresCatalog.class);
    }

    @Test
    void testFactoryWithFullUrlAndNullDefaultDatabase() {
        // Test case 2: base-url with database name + default-database = null
        // jdbc:postgresql://localhost:53036/test?loggerLevel=OFF (full URL)
        String baseUrl = getMetadata().getJdbcUrl();

        JdbcCatalog expectedCatalog =
                JdbcFactoryLoader.loadCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        null,
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        baseUrl);

        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), JdbcCatalogFactoryOptions.IDENTIFIER);
        options.put(JdbcCatalogFactoryOptions.USERNAME.key(), getMetadata().getUsername());
        options.put(JdbcCatalogFactoryOptions.PASSWORD.key(), getMetadata().getPassword());
        options.put(JdbcCatalogFactoryOptions.BASE_URL.key(), baseUrl);

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        TEST_CATALOG_NAME,
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader());

        assertThat(actualCatalog).isEqualTo(expectedCatalog).isInstanceOf(PostgresCatalog.class);
    }

    @Test
    void testFactoryWithMatchingDefaultDatabase() {
        // Test case 3: base-url with database name + default-database matches the database in URL
        // jdbc:postgresql://localhost:53036/test?loggerLevel=OFF + default-database = "test"
        String baseUrl = getMetadata().getJdbcUrl();

        JdbcCatalog expectedCatalog =
                JdbcFactoryLoader.loadCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        DEFAULT_DATABASE,
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        baseUrl);

        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), JdbcCatalogFactoryOptions.IDENTIFIER);
        options.put(JdbcCatalogFactoryOptions.DEFAULT_DATABASE.key(), DEFAULT_DATABASE);
        options.put(JdbcCatalogFactoryOptions.USERNAME.key(), getMetadata().getUsername());
        options.put(JdbcCatalogFactoryOptions.PASSWORD.key(), getMetadata().getPassword());
        options.put(JdbcCatalogFactoryOptions.BASE_URL.key(), baseUrl);

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        TEST_CATALOG_NAME,
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader());

        assertThat(actualCatalog).isEqualTo(expectedCatalog).isInstanceOf(PostgresCatalog.class);
    }
}
