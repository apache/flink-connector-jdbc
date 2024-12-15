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

package org.apache.flink.connector.jdbc.sqlserver.database.lineage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.lineage.JdbcLocation;
import org.apache.flink.connector.jdbc.lineage.JdbcLocationExtractor;

import org.apache.commons.lang3.StringUtils;

import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of {@link JdbcLocationExtractor} for SqlServer.
 *
 * @see <a
 *     href="https://learn.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16">SqlServer
 *     URL Format</a>
 */
@Internal
public class SqlServerLocationExtractor implements JdbcLocationExtractor {
    private static final String SCHEME = "sqlserver";
    private static final String SERVICE_PROPERTY = "servername";
    private static final String PORT_PROPERTY = "portnumber";
    private static final String INSTANCE_PROPERTY = "instancename";
    private static final String DATABASE_NAME_PROPERTY = "databasename";
    private static final String DATABASE_PROPERTY = "database";
    private static final String DEFAULT_PORT = "1433";

    private static final Pattern URL =
            Pattern.compile(
                    "(?:\\w+)://(?<host>[\\w\\d\\.-]+)?(?:\\\\)?(?<instance>[\\w]+)?(?::)?(?<port>\\d+)?(?<params>.*)");

    @Override
    public boolean isDefinedAt(String jdbcUri) {
        return jdbcUri.toLowerCase(Locale.ROOT).startsWith(SCHEME);
    }

    @Override
    @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
    public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
        Matcher matcher = URL.matcher(rawUri);
        if (!matcher.matches()) {
            throw new URISyntaxException(rawUri, "Failed to parse jdbc url");
        }

        // Priority: url components > url params > properties
        Properties finalProperties = new Properties();
        if (matcher.group("host") != null) {
            finalProperties.setProperty(SERVICE_PROPERTY, matcher.group("host"));
        }

        if (matcher.group("port") != null) {
            finalProperties.setProperty(PORT_PROPERTY, matcher.group("port"));
        }

        if (matcher.group("instance") != null) {
            finalProperties.setProperty(INSTANCE_PROPERTY, matcher.group("instance"));
        }

        String[] urlParams =
                StringUtils.defaultString(matcher.group("params")).replaceFirst(";", "").split(";");

        for (String urlParam : urlParams) {
            String[] parts = urlParam.split("=");
            if (parts.length == 2) {
                // property names are case-insensitive
                String key = parts[0].toLowerCase(Locale.ROOT);
                String value = parts[1];
                finalProperties.setProperty(key, value);
            }
        }

        for (String key : properties.stringPropertyNames()) {
            // properties have higher priority than in-url params.
            // property names are case-insensitive
            // https://learn.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties?view=sql-server-ver16#remarks
            String normalizedKey = key.toLowerCase(Locale.ROOT);
            if (finalProperties.getProperty(normalizedKey) == null) {
                finalProperties.setProperty(normalizedKey, properties.getProperty(key));
            }
        }

        String host = finalProperties.getProperty(SERVICE_PROPERTY);
        if (host == null) {
            throw new URISyntaxException(rawUri, "Missing host");
        }
        if (host.contains(":") && !host.startsWith("[")) {
            // IPv6 address
            host = "[" + host + "]";
        }

        String port = finalProperties.getProperty(PORT_PROPERTY);
        String authority;
        if (port != null) {
            authority = host + ":" + port;
        } else {
            authority = host + ":" + DEFAULT_PORT;
        }

        Optional<String> instance =
                Optional.ofNullable(finalProperties.getProperty(INSTANCE_PROPERTY));
        Optional<String> database =
                Optional.ofNullable(finalProperties.getProperty(DATABASE_NAME_PROPERTY))
                        .map(Optional::of)
                        .orElseGet(
                                () ->
                                        Optional.ofNullable(
                                                finalProperties.getProperty(DATABASE_PROPERTY)));

        return JdbcLocation.builder()
                .withScheme(SCHEME)
                .withAuthority(Optional.of(authority))
                .withInstance(instance)
                .withDatabase(database)
                .build();
    }
}
