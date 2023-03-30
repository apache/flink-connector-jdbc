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

package org.apache.flink.connector.jdbc.databases.vertica;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.DatabaseTest;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** A Vertica database for testing. */
@Testcontainers
public interface VerticaDatabase extends DatabaseTest {

    DockerImageName VERTICA_CE = DockerImageName.parse("vertica/vertica-ce");

    @Container
    GenericContainer<?> CONTAINER = new GenericContainer<>(VERTICA_CE).withExposedPorts(5433);

    @Override
    default DatabaseMetadata getMetadata() {
        return new VerticaMetadata(CONTAINER);
    }

    static Connection getConnection() throws SQLException, ClassNotFoundException {
        VerticaMetadata verticaMetadata = new VerticaMetadata(CONTAINER);
        Class.forName(verticaMetadata.getDriverClass());

        return DriverManager.getConnection(
                verticaMetadata.getJdbcUrl(),
                verticaMetadata.getUsername(),
                verticaMetadata.getPassword());
    }
}
