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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

/** Utils for JDBC url preprocess and namespace extraction. */
public class JdbcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);
    private static final String SLASH_DELIMITER_USER_PASSWORD_REGEX =
            "[A-Za-z0-9_%]+//?[A-Za-z0-9_%]*@";
    private static final String COLON_DELIMITER_USER_PASSWORD_REGEX =
            "([/|,])[A-Za-z0-9_%]+:?[A-Za-z0-9_%]*@";
    private static final String PARAMS_USER_PASSWORD_REGEX =
            "(?i)[,;&:]?(?:user|username|password)=[^,;&:()]+[,;&:]?";
    private static final String DUPLICATED_DELIMITERS = "(\\(\\)){2,}|[,;&:]{2,}";
    private static final String QUERY_PARAMS_REGEX = "\\?.*$";

    private static final List<JdbcLocationExtractor> extractors = new ArrayList<>();

    static {
        for (JdbcLocationExtractorFactory factory :
                ServiceLoader.load(JdbcLocationExtractorFactory.class)) {
            extractors.add(factory.createExtractor());
        }
    }

    public static String getJdbcNamespace(String jdbcUrl, Properties properties) {
        String uri = jdbcUrl.replaceAll("^(?i)jdbc:", "");
        try {
            JdbcLocationExtractor extractor = getExtractor(uri);
            return extractor.extract(uri, properties).toNamespace();
        } catch (URISyntaxException e) {
            LOG.debug("Failed to parse jdbc url", e);
            return dropSensitiveData(uri);
        }
    }

    private static JdbcLocationExtractor getExtractor(String jdbcUrl) throws URISyntaxException {
        for (JdbcLocationExtractor extractor : extractors) {
            if (extractor.isDefinedAt(jdbcUrl)) {
                return extractor;
            }
        }

        throw new URISyntaxException(jdbcUrl, "Unsupported JDBC URL");
    }

    /**
     * JdbcUrl can contain username and password this method clean-up credentials from jdbcUrl. Also
     * drop query params as they include a lot of useless options, like timeout
     *
     * @param jdbcUrl url to database
     * @return String
     */
    private static String dropSensitiveData(String jdbcUrl) {
        return jdbcUrl.replaceAll(SLASH_DELIMITER_USER_PASSWORD_REGEX, "@")
                .replaceAll(COLON_DELIMITER_USER_PASSWORD_REGEX, "$1")
                .replaceAll(PARAMS_USER_PASSWORD_REGEX, "")
                .replaceAll(DUPLICATED_DELIMITERS, "")
                .replaceAll(QUERY_PARAMS_REGEX, "");
    }
}
