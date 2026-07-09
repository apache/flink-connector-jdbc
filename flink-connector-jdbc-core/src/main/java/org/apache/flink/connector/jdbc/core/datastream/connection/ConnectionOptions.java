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
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Properties;

/** Connection options for a {@link ConnectionProvider}, extending the base JDBC options. */
@PublicEvolving
public class ConnectionOptions extends JdbcConnectionOptions {

    private final int connectionQueryTimeoutSeconds;

    protected ConnectionOptions(
            @Nonnull String url,
            @Nullable String driverName,
            @Nonnull Duration connectionCheckTimeout,
            @Nonnull Duration connectionQueryTimeout,
            @Nonnull Properties properties) {
        super(url, driverName, Math.toIntExact(connectionCheckTimeout.getSeconds()), properties);
        this.connectionQueryTimeoutSeconds = Math.toIntExact(connectionQueryTimeout.getSeconds());
    }

    public int getConnectionQueryTimeoutSeconds() {
        return connectionQueryTimeoutSeconds;
    }

    public static ConnectionOptionsBuilder builder() {
        return new ConnectionOptionsBuilder();
    }
}
