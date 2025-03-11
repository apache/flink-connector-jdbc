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

package org.apache.flink.connector.jdbc.dameng.database;

import org.apache.flink.connector.jdbc.core.database.JdbcFactory;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dameng.database.catalog.DaMengCatalog;
import org.apache.flink.connector.jdbc.dameng.database.dialect.DaMengDialect;

/** Factory for {@link DaMengDialect}. */
public class DaMengFactory implements JdbcFactory
{
    @Override
    public boolean acceptsURL(String url)
    {
        return url.startsWith("jdbc:dm:");
    }

    @Override
    public JdbcDialect createDialect()
    {
        return new DaMengDialect();
    }

    @Override
    public JdbcCatalog createCatalog(
            ClassLoader classLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl)
    {
        return new DaMengCatalog(classLoader, catalogName, defaultDatabase, username, pwd, baseUrl);
    }
}
