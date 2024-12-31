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

package org.apache.flink.connector.jdbc.oracle.database.lineage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.lineage.JdbcLocation;
import org.apache.flink.connector.jdbc.lineage.JdbcLocationExtractor;
import org.apache.flink.connector.jdbc.lineage.OverrideJdbcLocationExtractor;

import org.apache.commons.lang3.StringUtils;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Implementation of {@link JdbcLocationExtractor} for Oracle.
 *
 * @see <a
 *     href="https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/data-sources-and-URLs.html#GUID-EF07727C-50AB-4DCE-8EDC-57F0927FF61A">Oracle
 *     URL Format</a>
 */
@Internal
public class OracleLocationExtractor implements JdbcLocationExtractor {

    private static final String SCHEME = "oracle";
    private static final String DEFAULT_PORT = "1521";
    private static final String URI_START = "^.*@(//)?";
    private static final String URI_END = "\\?.*$";
    private static final String PROTOCOL_PART = "^\\w+://$";

    @Override
    public boolean isDefinedAt(String jdbcUri) {
        return jdbcUri.toLowerCase(Locale.ROOT).startsWith(SCHEME);
    }

    @Override
    public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
        // oracle:thin:@//host:1521:sid?... -> host:1521:sid
        String uri = rawUri.replaceFirst(URI_START, "").replaceAll(URI_END, "");

        if (uri.contains("(")) {
            throw new URISyntaxException(uri, "TNS format is unsupported for now");
        }
        return extractUri(uri, properties);
    }

    private JdbcLocation extractUri(String uri, Properties properties) throws URISyntaxException {
        // convert 'tcp://'' protocol to 'oracle://'', convert ':sid' format to '/sid'
        String normalizedUri = uri.replaceFirst(PROTOCOL_PART, "");
        normalizedUri = SCHEME + "://" + normalizedUri;

        return new OverrideJdbcLocationExtractor(SCHEME).extract(normalizedUri, properties);
    }

    private String fixSidFormat(String uri) {
        if (!uri.contains(":")) {
            return uri;
        }
        List<String> components = Arrays.stream(uri.split(":")).collect(Collectors.toList());
        String last = components.remove(components.size() - 1);
        if (last.contains("]") || last.matches("^\\d+$")) {
            // '[ip:v:6]' or 'host:1521'
            return uri;
        }
        // 'host:1521:sid' -> 'host:1521/sid'
        return StringUtils.join(components, ":") + "/" + last;
    }
}
