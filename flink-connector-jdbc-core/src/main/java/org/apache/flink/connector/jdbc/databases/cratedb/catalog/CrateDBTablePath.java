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

package org.apache.flink.connector.jdbc.databases.cratedb.catalog;

import org.apache.flink.connector.jdbc.databases.postgres.catalog.PostgresTablePath;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Table path of CrateDB in Flink. Can be of formats "table_name" or "schema_name.table_name". When
 * it's "table_name", the schema name defaults to "doc".
 */
public class CrateDBTablePath extends PostgresTablePath {

    private static final String DEFAULT_CRATE_SCHEMA_NAME = "doc";

    public CrateDBTablePath(String pgSchemaName, String pgTableName) {
        super(pgSchemaName, pgTableName);
    }

    public static CrateDBTablePath fromFlinkTableName(String flinkTableName) {
        if (flinkTableName.contains(".")) {
            String[] path = flinkTableName.split("\\.");

            checkArgument(
                    path.length == 2,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            flinkTableName, path.length));

            return new CrateDBTablePath(path[0], path[1]);
        } else {
            return new CrateDBTablePath(getDefaultSchemaName(), flinkTableName);
        }
    }

    public static String toFlinkTableName(String schema, String table) {
        return new PostgresTablePath(schema, table).getFullPath();
    }

    protected static String getDefaultSchemaName() {
        return DEFAULT_CRATE_SCHEMA_NAME;
    }
}
