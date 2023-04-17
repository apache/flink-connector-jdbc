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

package org.apache.flink.connector.jdbc.internal.options;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.connector.jdbc.split.JdbcMultiTableProvider;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Options for the JDBC connector. */
@Internal
public class JdbcConnectorOptions extends JdbcConnectionOptions {

    private static final long serialVersionUID = 1L;

    private final String tableName;
    //Oracle和Postgres的schemaName为dbName
    private final String dbName;
    private final JdbcDialect dialect;
    private final @Nullable Integer parallelism;
    private final List<JdbcMultiTableProvider.TableItem> tables;

    private JdbcConnectorOptions(
            String dbURL,
            String tableName,
            String dbName,
            @Nullable String driverName,
            @Nullable String username,
            @Nullable String password,
            JdbcDialect dialect,
            @Nullable Integer parallelism,
            int connectionCheckTimeoutSeconds,
            List<JdbcMultiTableProvider.TableItem> tables) {
        super(dbURL, driverName, username, password, connectionCheckTimeoutSeconds);
        this.tableName = tableName;
        this.dbName = dbName;
        this.dialect = dialect;
        this.parallelism = parallelism;
        this.tables = tables;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public JdbcDialect getDialect() {
        return dialect;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public List<JdbcMultiTableProvider.TableItem> getTables() {
        return tables;
    }

    public static Builder builder() {
        return new Builder();
    }


    @Override
    public boolean equals(Object o) {
        if (o instanceof JdbcConnectorOptions) {
            JdbcConnectorOptions options = (JdbcConnectorOptions) o;
            return Objects.equals(url, options.url)
                    && Objects.equals(tableName, options.tableName)
                    && Objects.equals(driverName, options.driverName)
                    && Objects.equals(username, options.username)
                    && Objects.equals(password, options.password)
                    && Objects.equals(
                            dialect.getClass().getName(), options.dialect.getClass().getName())
                    && Objects.equals(parallelism, options.parallelism)
                    && Objects.equals(
                            connectionCheckTimeoutSeconds, options.connectionCheckTimeoutSeconds);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                url,
                tableName,
                driverName,
                username,
                password,
                dialect.getClass().getName(),
                parallelism,
                connectionCheckTimeoutSeconds);
    }

    /** Builder of {@link JdbcConnectorOptions}. */
    public static class Builder {
        private ClassLoader classLoader;
        private String dbURL;
        private String tableName;
        private String dbName;
        private String driverName;
        private String username;
        private String password;
        private JdbcDialect dialect;
        private Integer parallelism;
        private int connectionCheckTimeoutSeconds = 60;
        private List<JdbcMultiTableProvider.TableItem> tables;

        /**
         * optional, specifies the classloader to use in the planner for load the class in user jar.
         *
         * <p>By default, this is configured using {@code
         * Thread.currentThread().getContextClassLoader()}.
         *
         * <p>Modify the {@link ClassLoader} only if you know what you're doing.
         */
        public Builder setClassLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        /** required, table name. */
        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setDbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        /** optional, user name. */
        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /** optional, password. */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /** optional, connectionCheckTimeoutSeconds. */
        public Builder setConnectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        /**
         * optional, driver name, dialect has a default driver name, See {@link
         * JdbcDialect#defaultDriverName}.
         */
        public Builder setDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        /** required, JDBC DB url. */
        public Builder setDBUrl(String dbURL) {
            this.dbURL = dbURL;
            return this;
        }

        /**
         * optional, Handle the SQL dialect of jdbc driver. If not set, it will be infer by {@link
         * JdbcDialectLoader#load} from DB url.
         */
        public Builder setDialect(JdbcDialect dialect) {
            this.dialect = dialect;
            return this;
        }

        public Builder setParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }
        public Builder setTables(List<JdbcMultiTableProvider.TableItem> tables) {
            this.tables = tables;
            return this;
        }

        public JdbcConnectorOptions build() {
            checkNotNull(dbURL, "No dbURL supplied.");
            checkNotNull(tableName, "No tableName supplied.");
            if (this.dialect == null) {
                if (classLoader == null) {
                    classLoader = Thread.currentThread().getContextClassLoader();
                }
                this.dialect = JdbcDialectLoader.load(dbURL, classLoader);
            }
            if (this.driverName == null) {
                Optional<String> optional = dialect.defaultDriverName();
                this.driverName =
                        optional.orElseThrow(
                                () -> new NullPointerException("No driverName supplied."));
            }

            return new JdbcConnectorOptions(
                    dialect.appendDefaultUrlProperties(dbURL),
                    tableName,
                    dbName,
                    driverName,
                    username,
                    password,
                    dialect,
                    parallelism,
                    connectionCheckTimeoutSeconds,
                    tables);
        }
    }
}
