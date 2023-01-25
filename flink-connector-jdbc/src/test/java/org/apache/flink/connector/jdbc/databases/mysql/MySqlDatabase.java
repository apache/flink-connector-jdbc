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

import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/** A MySql database. * */
public class MySqlDatabase extends DatabaseExtension {

    private static final MySQLContainer<?> container =
            new MySQLContainer<>(DockerImageVersions.MYSQL)
                    .withLogConsumer(
                            new Slf4jLogConsumer(LoggerFactory.getLogger(MySqlDatabase.class)));

    private static MySqlMetadata metadata;

    public static MySqlMetadata getMetadata() {
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
}
