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

package org.apache.flink.connector.jdbc.databases.h2;

import org.apache.flink.connector.jdbc.databases.DatabaseExtension;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import java.sql.DriverManager;

/** H2 database for testing. * */
public class H2XaDatabase extends DatabaseExtension {

    private static final H2Metadata metadata = new H2Metadata("test");

    public static H2Metadata getMetadata() {
        return metadata;
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        DriverManager.getConnection(
                        String.format(
                                "%s;DB_CLOSE_DELAY=-1;INIT=CREATE SCHEMA IF NOT EXISTS %s\\;SET SCHEMA %s",
                                metadata.getUrl(), "test", "test"))
                .close();
        return metadata;
    }

    @Override
    protected void stopDatabase() throws Exception {}
}
