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

package org.apache.flink.connector.jdbc.trino.database.lineage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.lineage.JdbcLocation;
import org.apache.flink.connector.jdbc.lineage.JdbcLocationExtractor;
import org.apache.flink.connector.jdbc.lineage.OverrideJdbcLocationExtractor;

import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Implementation of {@link JdbcLocationExtractor} for Trino.
 *
 * @see <a href="https://trino.io/docs/current/client/jdbc.html">Trino URL Format</a>
 */
@Internal
public class TrinoLocationExtractor implements JdbcLocationExtractor {

    private JdbcLocationExtractor delegate() {
        return new OverrideJdbcLocationExtractor("trino", "443");
    }

    @Override
    public boolean isDefinedAt(String jdbcUri) {
        return delegate().isDefinedAt(jdbcUri);
    }

    @Override
    public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
        return delegate().extract(rawUri, properties);
    }
}
