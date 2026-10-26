/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.spanner.database.lineage;

import org.apache.flink.annotation.Internal;

import io.openlineage.client.utils.jdbc.JdbcExtractor;
import io.openlineage.client.utils.jdbc.JdbcLocation;

import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link JdbcExtractor} for Spanner JDBC URLs.
 *
 * <p>The namespace follows the OpenLineage naming convention {@code
 * spanner://{projectId}:{instanceId}} and is suffixed with the database, e.g. {@code
 * spanner://my-project:my-instance/my-database}, as the connector only uses the table name as the
 * dataset name.
 */
@Internal
public class SpannerJdbcExtractor implements JdbcExtractor {

    private static final String SCHEME = "spanner";
    private static final String URL_PREFIX = "cloudspanner:";
    private static final Pattern PATH_PATTERN =
            Pattern.compile("/projects/([^/;?]+)/instances/([^/;?]+)(?:/databases/([^/;?]+))?");

    @Override
    public boolean isDefinedAt(String jdbcUri) {
        return jdbcUri.regionMatches(true, 0, URL_PREFIX, 0, URL_PREFIX.length());
    }

    @Override
    public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
        Matcher matcher = PATH_PATTERN.matcher(rawUri);
        if (!matcher.find()) {
            throw new URISyntaxException(rawUri, "Missing Spanner project and instance");
        }
        Optional<String> database = Optional.ofNullable(matcher.group(3));
        return new JdbcLocation(
                SCHEME, Optional.of(matcher.group(1) + ":" + matcher.group(2)), database, database);
    }
}
