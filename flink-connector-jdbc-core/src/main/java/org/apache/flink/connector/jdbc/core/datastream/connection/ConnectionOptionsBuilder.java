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

package org.apache.flink.connector.jdbc.core.datastream.connection;

import org.apache.flink.annotation.PublicEvolving;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Builder for creating instances of {@link ConnectionOptions}. */
@PublicEvolving
public class ConnectionOptionsBuilder {

    private String url;
    private String driverName;
    private Duration connectionCheckTimeout = Duration.ofSeconds(60);
    private Duration connectionQueryTimeout = Duration.ofSeconds(60);
    private final Properties properties = new Properties();

    public ConnectionOptionsBuilder withUrl(String url) {
        this.url = url;
        return this;
    }

    public ConnectionOptionsBuilder withDriverName(String driverName) {
        this.driverName = driverName;
        return this;
    }

    public ConnectionOptionsBuilder withProperty(String propKey, String propVal) {
        checkNotNull(propKey, "Connection property key mustn't be null");
        checkNotNull(propVal, "Connection property value mustn't be null");
        this.properties.put(propKey, propVal);
        return this;
    }

    public ConnectionOptionsBuilder withUsername(String username) {
        if (Objects.nonNull(username)) {
            this.properties.put("user", username);
        }

        return this;
    }

    public ConnectionOptionsBuilder withPassword(String password) {
        if (Objects.nonNull(password)) {
            this.properties.put("password", password);
        }

        return this;
    }

    public ConnectionOptionsBuilder withConnectionCheckTimeout(Duration connectionCheckTimeout) {
        checkNotNull(connectionCheckTimeout, "Connection check timeout mustn't be null");
        this.connectionCheckTimeout = connectionCheckTimeout;
        return this;
    }

    public ConnectionOptionsBuilder withConnectionQueryTimeout(Duration connectionQueryTimeout) {
        checkNotNull(connectionQueryTimeout, "Connection query timeout mustn't be null");
        this.connectionQueryTimeout = connectionQueryTimeout;
        return this;
    }

    public ConnectionOptions build() {
        return new ConnectionOptions(
                this.url,
                this.driverName,
                this.connectionCheckTimeout,
                this.connectionQueryTimeout,
                this.properties);
    }
}
