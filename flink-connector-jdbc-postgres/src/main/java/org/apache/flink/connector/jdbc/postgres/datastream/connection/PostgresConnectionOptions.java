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

package org.apache.flink.connector.jdbc.postgres.datastream.connection;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionOptions;
import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionOptionsBuilder;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Builder for creating Postgres-flavored {@link ConnectionOptions}. */
@PublicEvolving
public class PostgresConnectionOptions {

    private String host;
    private int port;
    private String database;
    private final ConnectionOptionsBuilder connectionOptions;

    public PostgresConnectionOptions() {
        this.connectionOptions = ConnectionOptions.builder();
        this.connectionOptions.withDriverName("org.postgresql.Driver");
    }

    public static PostgresConnectionOptions create() {
        return new PostgresConnectionOptions();
    }

    public PostgresConnectionOptions withHost(String host) {
        this.host = host;
        return this;
    }

    public PostgresConnectionOptions withPort(int port) {
        this.port = port;
        return this;
    }

    public PostgresConnectionOptions withDatabase(String database) {
        this.database = database;
        return this;
    }

    public PostgresConnectionOptions withDriverName(String driverName) {
        this.connectionOptions.withDriverName(driverName);
        return this;
    }

    public PostgresConnectionOptions withProperty(String propKey, String propVal) {
        this.connectionOptions.withProperty(propKey, propVal);
        return this;
    }

    public PostgresConnectionOptions withUsername(String username) {
        this.connectionOptions.withUsername(username);
        return this;
    }

    public PostgresConnectionOptions withPassword(String password) {
        this.connectionOptions.withPassword(password);
        return this;
    }

    public PostgresConnectionOptions withConnectionCheckTimeout(Duration connectionCheckTimeout) {
        this.connectionOptions.withConnectionCheckTimeout(connectionCheckTimeout);
        return this;
    }

    public PostgresConnectionOptions withConnectionQueryTimeout(Duration connectionQueryTimeout) {
        this.connectionOptions.withConnectionQueryTimeout(connectionQueryTimeout);
        return this;
    }

    public ConnectionOptions build() {
        checkNotNull(host, "Connection host mustn't be null");
        checkNotNull(port, "Connection port mustn't be null");
        checkNotNull(database, "Connection database mustn't be null");
        return connectionOptions
                .withUrl(String.format("jdbc:postgresql://%s:%s/%s", host, port, database))
                .build();
    }
}
