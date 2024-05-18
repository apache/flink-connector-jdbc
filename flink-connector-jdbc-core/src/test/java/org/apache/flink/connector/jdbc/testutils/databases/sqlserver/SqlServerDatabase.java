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

package org.apache.flink.connector.jdbc.testutils.databases.sqlserver;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/** A SqlServer database for testing. */
public class SqlServerDatabase extends DatabaseExtension implements SqlServerImages {

    private static final MSSQLServerContainer<?> CONTAINER =
            new SqlServerContainer(MSSQL_AZURE_SQL_EDGE).acceptLicense().withXa();

    private static SqlServerMetadata metadata;

    public static SqlServerMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new SqlServerMetadata(CONTAINER, true);
        }
        return metadata;
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        CONTAINER.start();
        return getMetadata();
    }

    @Override
    protected void stopDatabase() throws Exception {
        CONTAINER.stop();
        metadata = null;
    }

    /** {@link MSSQLServerContainer} with Xa. */
    public static class SqlServerContainer extends MSSQLServerContainer<SqlServerContainer> {
        private boolean xaActive = false;

        public SqlServerContainer(String dockerImageName) {
            super(DockerImageName.parse(dockerImageName));
        }

        public SqlServerContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        public SqlServerContainer withXa() {
            this.xaActive = true;
            return this.self();
        }

        @Override
        public void start() {
            super.start();
            prepareDb();
        }

        private void prepareDb() {
            try (Connection connection =
                            DriverManager.getConnection(
                                    getJdbcUrl(), getUsername(), getPassword());
                    Statement st = connection.createStatement()) {

                if (xaActive) {
                    st.execute("EXEC sp_sqljdbc_xa_install");
                }
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            }
        }
    }
}
