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

package org.apache.flink.connector.jdbc.core.database.catalog.factory;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.jdbc.core.database.JdbcFactoryLoader;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactoryOptions.BASE_URL;
import static org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactoryOptions.COMPATIBLE_MODE;
import static org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactoryOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactoryOptions.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

/** Factory for {@link JdbcCatalog}. */
public class JdbcCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return JdbcCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(BASE_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPERTY_VERSION);
        options.add(COMPATIBLE_MODE);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        return JdbcFactoryLoader.loadCatalog(
                context.getClassLoader(),
                context.getName(),
                helper.getOptions().get(JdbcCatalogFactoryOptions.DEFAULT_DATABASE),
                helper.getOptions().get(JdbcCatalogFactoryOptions.USERNAME),
                helper.getOptions().get(JdbcCatalogFactoryOptions.PASSWORD),
                helper.getOptions().get(JdbcCatalogFactoryOptions.BASE_URL),
                helper.getOptions().get(JdbcCatalogFactoryOptions.COMPATIBLE_MODE));
    }
}
