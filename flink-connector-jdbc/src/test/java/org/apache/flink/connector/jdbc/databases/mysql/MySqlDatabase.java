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

package org.apache.flink.connector.jdbc.databases.mysql;

import org.apache.flink.connector.jdbc.databases.DatabaseExtension;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.DockerImageVersions;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/** A MySql database. * */
public class MySqlDatabase extends DatabaseExtension {

    private final MySQLContainer<?> container;

    private static MySqlMetadata metadata;

    public MySqlDatabase() {
        this(DockerImageVersions.MYSQL_8_0);
    }

    public MySqlDatabase(String dockerImage) {
        container =
                new MySqlContainer(dockerImage)
                        .withUsername("mysql")
                        .withPassword("mysql")
                        .withInitScript("mysql-scripts/catalog-init-for-test.sql")
                        .withEnv("MYSQL_ROOT_HOST", "%")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        LoggerFactory.getLogger(MySqlDatabaseAll.class)));
    }

    public static MySqlMetadata getMetadata() {
        return metadata;
    }

    public MySqlMetadata getMetadataDb() {
        return metadata;
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        container.start();
        metadata = new MySqlMetadata(container);
        return metadata;
    }

    @Override
    protected void stopDatabase() throws Exception {
        container.stop();
    }

    /** An implementation of {@link MySqlDatabaseAll.MySqlAllContainer}. * */
    static class MySqlContainer extends MySQLContainer<MySqlContainer> {
        public MySqlContainer(String dockerImageName) {
            super(DockerImageName.parse(dockerImageName));
        }

        @Override
        protected void runInitScriptIfRequired() {
            try (Connection connection =
                            DriverManager.getConnection(getJdbcUrl(), "root", getPassword());
                    Statement st = connection.createStatement()) {
                st.execute(
                        String.format(
                                "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION",
                                getUsername()));
                st.execute("FLUSH PRIVILEGES");
            } catch (SQLException e) {
                ExceptionUtils.rethrow(e);
            } finally {
                super.runInitScriptIfRequired();
            }
        }
    }
}
