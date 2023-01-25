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

import org.apache.flink.connector.jdbc.databases.DockerImageVersions;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/** A set of distinct MySql databases. * */
@Testcontainers
public interface MySqlDatabaseAll {

    String TEST_USERNAME = "mysql";
    String TEST_PASSWORD = "mysql";

    @Container
    MySQLContainer<?> MYSQL_CONTAINER_8_0 = createContainer(DockerImageVersions.MYSQL_8_0);

    @Container
    MySQLContainer<?> MYSQL_CONTAINER_5_7 = createContainer(DockerImageVersions.MYSQL_5_7);

    @Container
    MySQLContainer<?> MYSQL_CONTAINER_5_6 = createContainer(DockerImageVersions.MYSQL_5_6);

    List<MySQLContainer<?>> MYSQL_CONTAINERS =
            Arrays.asList(MYSQL_CONTAINER_8_0, MYSQL_CONTAINER_5_7, MYSQL_CONTAINER_5_6);

    static MySQLContainer<?> createContainer(String dockerImage) {
        return new MySqlAllContainer(dockerImage)
                .withUsername(TEST_USERNAME)
                .withPassword(TEST_PASSWORD)
                .withInitScript("mysql-scripts/catalog-init-for-test.sql")
                .withEnv("MYSQL_ROOT_HOST", "%")
                .withLogConsumer(
                        new Slf4jLogConsumer(LoggerFactory.getLogger(MySqlDatabaseAll.class)));
    }

    /** An implementation of {@link MySqlAllContainer}. * */
    class MySqlAllContainer extends MySQLContainer<MySqlAllContainer> {
        public MySqlAllContainer(String dockerImageName) {
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
