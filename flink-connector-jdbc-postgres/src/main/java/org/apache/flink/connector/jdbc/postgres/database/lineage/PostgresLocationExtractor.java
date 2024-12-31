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

package org.apache.flink.connector.jdbc.postgres.database.lineage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.lineage.JdbcLocation;
import org.apache.flink.connector.jdbc.lineage.JdbcLocationExtractor;
import org.apache.flink.connector.jdbc.lineage.OverrideJdbcLocationExtractor;

import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;

/**
 * Implementation of {@link JdbcLocationExtractor} for Postgres.
 *
 * @see <a href="https://jdbc.postgresql.org/documentation/use/#connecting-to-the-database">Postgres
 *     URL Format</a>
 */
@Internal
public class PostgresLocationExtractor implements JdbcLocationExtractor {
    private static final String SCHEME = "postgres";
    private static final String DEFAULT_PORT = "5432";
    private static final String DEFAULT_AUTHORITY = "localhost:5432";

    private JdbcLocationExtractor delegate() {
        return new OverrideJdbcLocationExtractor(SCHEME, DEFAULT_PORT);
    }

    @Override
    public boolean isDefinedAt(String jdbcUri) {
        return delegate().isDefinedAt(jdbcUri);
    }

    @Override
    public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
        if (!rawUri.contains("//")) {
            String[] parts = rawUri.split(":");
            if (parts.length != 2) {
                throw new URISyntaxException(rawUri, "Invalid Postgres JDBC url");
            }
            return new JdbcLocation.Builder()
                    .withScheme(SCHEME)
                    .withAuthority(Optional.of(DEFAULT_AUTHORITY))
                    .withDatabase(Optional.of(parts[1]))
                    .build();
        }
        return delegate().extract(rawUri, properties);
    }
}
