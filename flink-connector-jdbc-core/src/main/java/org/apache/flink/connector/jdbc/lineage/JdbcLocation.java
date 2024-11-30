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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

/** JDBC connection URL location. */
@Internal
public class JdbcLocation {
    private final String scheme;
    private final Optional<String> authority;
    private final Optional<String> instance;
    private final Optional<String> database;

    private JdbcLocation(
            String scheme,
            Optional<String> authority,
            Optional<String> instance,
            Optional<String> database) {
        this.scheme = scheme;
        this.authority = authority;
        this.instance = instance;
        this.database = database;
    }

    public String toNamespace() {
        String result = scheme.toLowerCase(Locale.ROOT) + ":";
        if (authority.isPresent()) {
            result = String.format("%s//%s", result, authority.get().toLowerCase(Locale.ROOT));
        }
        if (instance.isPresent()) {
            result = String.format("%s/%s", result, StringUtils.stripStart(instance.get(), "/"));
        }
        return result;
    }

    public String toName(List<String> parts) {
        if (database.isPresent()) {
            parts.add(0, database.get());
        }
        return String.join(".", parts);
    }

    public String getScheme() {
        return this.scheme;
    }

    public Optional<String> getAuthority() {
        return this.authority;
    }

    public Optional<String> getInstance() {
        return this.instance;
    }

    public Optional<String> getDatabase() {
        return this.database;
    }

    public static JdbcLocation.Builder builder() {
        return new JdbcLocation.Builder();
    }

    /** Builder for {@link JdbcLocation}. */
    @PublicEvolving
    public static final class Builder {
        private String scheme = "";
        private Optional<String> authority = Optional.empty();
        private Optional<String> instance = Optional.empty();
        private Optional<String> database = Optional.empty();

        public Builder withScheme(String scheme) {
            this.scheme = scheme;
            return this;
        }

        public Builder withAuthority(Optional<String> authority) {
            this.authority = authority;
            return this;
        }

        public Builder withInstance(Optional<String> instance) {
            this.instance = instance;
            return this;
        }

        public Builder withDatabase(Optional<String> database) {
            this.database = database;
            return this;
        }

        public JdbcLocation build() {
            return new JdbcLocation(scheme, authority, instance, database);
        }
    }
}
