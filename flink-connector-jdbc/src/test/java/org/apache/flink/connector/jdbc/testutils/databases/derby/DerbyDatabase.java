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

package org.apache.flink.connector.jdbc.testutils.databases.derby;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.OutputStream;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Derby database for testing. */
public class DerbyDatabase extends DatabaseExtension {

    @SuppressWarnings("unused") // used in string constant in prepareDatabase
    public static final OutputStream DEV_NULL =
            new OutputStream() {
                @Override
                public void write(int b) {}
            };

    private static DerbyMetadata metadata;

    public static DerbyMetadata getMetadata() {
        if (metadata == null) {
            metadata = new DerbyMetadata("test");
        }
        return metadata;
    }

    @Override
    public DatabaseMetadata startDatabase() throws Exception {
        DatabaseMetadata metadata = getMetadata();
        try {
            System.setProperty(
                    "derby.stream.error.field",
                    DerbyDatabase.class.getCanonicalName() + ".DEV_NULL");
            Class.forName(metadata.getDriverClass());
            DriverManager.getConnection(String.format("%s;create=true", metadata.getJdbcUrl()))
                    .close();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        return metadata;
    }

    @Override
    protected void stopDatabase() throws Exception {
        try {
            DriverManager.getConnection(String.format("%s;shutdown=true", metadata.getJdbcUrl()))
                    .close();
        } catch (SQLException ignored) {
        } finally {
            metadata = null;
        }
    }
}
