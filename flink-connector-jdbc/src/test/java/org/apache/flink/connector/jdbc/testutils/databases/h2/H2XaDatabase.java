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

package org.apache.flink.connector.jdbc.testutils.databases.h2;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.util.FlinkRuntimeException;

import java.sql.DriverManager;

/** H2 database for testing. */
public class H2XaDatabase extends DatabaseExtension {

    private static H2Metadata metadata;

    public static H2Metadata getMetadata() {
        if (metadata == null) {
            metadata = new H2Metadata("test");
        }
        return metadata;
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        DatabaseMetadata metadata = getMetadata();
        try {
            Class.forName(metadata.getDriverClass());
            DriverManager.getConnection(
                            String.format(
                                    "%s;DB_CLOSE_DELAY=-1;INIT=CREATE SCHEMA IF NOT EXISTS %s\\;SET SCHEMA %s",
                                    metadata.getJdbcUrl(), "test", "test"))
                    .close();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        return metadata;
    }

    @Override
    protected void stopDatabase() throws Exception {}
}
