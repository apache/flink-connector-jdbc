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

import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Implementation of {@link JdbcLocationExtractor} that can override scheme. */
@PublicEvolving
public class OverrideJdbcLocationExtractor extends DefaultJdbcExtractor {

    private final String overrideScheme;
    private final String defaultPort;
    private static final Pattern HOST_PORT_FORMAT =
            Pattern.compile("^(?<host>[\\[\\]\\w\\d.-]+):(?<port>\\d+)?");

    public OverrideJdbcLocationExtractor(String overrideScheme) {
        this(overrideScheme, null);
    }

    public OverrideJdbcLocationExtractor(String overrideScheme, String defaultPort) {
        this.overrideScheme = overrideScheme;
        this.defaultPort = defaultPort;
    }

    @Override
    public boolean isDefinedAt(String jdbcUri) {
        return jdbcUri.toLowerCase(Locale.ROOT).startsWith(overrideScheme);
    }

    @Override
    public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
        JdbcLocation result = super.extract(rawUri, properties);

        Optional<String> authority = result.getAuthority();
        if (authority.isPresent() && defaultPort != null) {
            authority = authority.map(this::appendDefaultPort);
        }

        return JdbcLocation.builder()
                .withScheme(overrideScheme)
                .withAuthority(authority)
                .withInstance(result.getInstance())
                .withDatabase(result.getDatabase())
                .build();
    }

    private String appendDefaultPort(String authority) {
        String[] hosts = authority.split(",");
        for (int i = 0; i < hosts.length; i++) {
            String host = hosts[i];
            Matcher hostPortMatcher = HOST_PORT_FORMAT.matcher(host);
            if (!hostPortMatcher.matches()) {
                hosts[i] = host + ":" + defaultPort;
            }
        }
        return String.join(",", hosts);
    }
}
