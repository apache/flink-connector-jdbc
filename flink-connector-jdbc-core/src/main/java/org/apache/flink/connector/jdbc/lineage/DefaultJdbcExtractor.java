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

package org.apache.flink.connector.jdbc.lineage;

import org.apache.flink.annotation.PublicEvolving;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Default implementation of {@link JdbcLocationExtractor}. */
@PublicEvolving
public class DefaultJdbcExtractor implements JdbcLocationExtractor {

    private static final Pattern URL_FORMAT =
            Pattern.compile(
                    "^(?<scheme>\\w+)://(?<authority>[\\w\\d\\.\\[\\]:,-]+)/?(?<database>[\\w\\d.]+)?(?:\\?.*)?");

    @Override
    public boolean isDefinedAt(String jdbcUri) {
        return true;
    }

    @Override
    public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
        if (!rawUri.contains(",")) {
            return extractOneHost(rawUri);
        }
        return extractMultipleHosts(rawUri);
    }

    private JdbcLocation extractOneHost(String rawUri) throws URISyntaxException {
        URI uri = new URI(rawUri);

        if (uri.getHost() == null) {
            throw new URISyntaxException(rawUri, "Missing host");
        }

        String scheme = uri.getScheme();
        String host = uri.getHost();
        String authority;
        if (uri.getPort() > 0) {
            authority = String.format("%s:%d", host, uri.getPort());
        } else {
            authority = host;
        }

        Optional<String> database =
                Optional.ofNullable(uri.getPath())
                        .map(db -> db.replaceFirst("/", ""))
                        .filter(db -> !db.isEmpty());
        return JdbcLocation.builder()
                .withScheme(scheme)
                .withAuthority(Optional.of(authority))
                .withDatabase(database)
                .build();
    }

    private JdbcLocation extractMultipleHosts(String rawUri) throws URISyntaxException {
        // new URI() parses 'scheme://host1,host2' syntax as scheme-specific part instead of
        // authority
        // Using regex to extract URI components

        Matcher matcher = URL_FORMAT.matcher(rawUri);
        if (!matcher.matches()) {
            throw new URISyntaxException(rawUri, "Failed to parse jdbc url");
        }

        String scheme = matcher.group("scheme");
        String authority = matcher.group("authority");
        String database = matcher.group("database");

        return JdbcLocation.builder()
                .withScheme(scheme)
                .withAuthority(Optional.of(authority))
                .withDatabase(Optional.ofNullable(database))
                .build();
    }
}
