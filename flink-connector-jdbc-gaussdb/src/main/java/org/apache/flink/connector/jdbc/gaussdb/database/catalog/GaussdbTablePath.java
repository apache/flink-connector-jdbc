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

package org.apache.flink.connector.jdbc.gaussdb.database.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.StringUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Table path of GaussDB in Flink. Can be of formats "table_name" or "schema_name.table_name". When
 * it's "table_name", the schema name defaults to "public".
 */
@Internal
public class GaussdbTablePath {

    private static final String DEFAULT_POSTGRES_SCHEMA_NAME = "public";

    private final String pgSchemaName;
    private final String pgTableName;

    public GaussdbTablePath(String pgSchemaName, String pgTableName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(pgSchemaName),
                "Schema name is not valid. Null or empty is not allowed");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(pgTableName),
                "Table name is not valid. Null or empty is not allowed");

        this.pgSchemaName = pgSchemaName;
        this.pgTableName = pgTableName;
    }

    public static GaussdbTablePath fromFlinkTableName(String flinkTableName) {
        if (flinkTableName.contains(".")) {
            String[] path = flinkTableName.split("\\.");

            checkArgument(
                    path != null && path.length == 2,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            flinkTableName, path.length));

            return new GaussdbTablePath(path[0], path[1]);
        } else {
            return new GaussdbTablePath(getDefaultSchemaName(), flinkTableName);
        }
    }

    public static String toFlinkTableName(String schema, String table) {
        return new GaussdbTablePath(schema, table).getFullPath();
    }

    public String getFullPath() {
        return String.format("%s.%s", pgSchemaName, pgTableName);
    }

    public String getPgTableName() {
        return pgTableName;
    }

    public String getPgSchemaName() {
        return pgSchemaName;
    }

    protected static String getDefaultSchemaName() {
        return DEFAULT_POSTGRES_SCHEMA_NAME;
    }

    @Override
    public String toString() {
        return getFullPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GaussdbTablePath that = (GaussdbTablePath) o;
        return Objects.equals(pgSchemaName, that.pgSchemaName)
                && Objects.equals(pgTableName, that.pgTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pgSchemaName, pgTableName);
    }
}
