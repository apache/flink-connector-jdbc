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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.util.StringUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Table path of CrateDB in Flink. Can be of formats "table_name" or "schema_name.table_name". When
 * it's "table_name", the schema name defaults to "doc".
 */
public class CrateDBTablePath {

    private static final String DEFAULT_CRATE_SCHEMA_NAME = "doc";

    private final String crateDBSchemaName;
    private final String crateDBTableName;

    public CrateDBTablePath(String crateDBSchemaName, String crateDBTableName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(crateDBSchemaName));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(crateDBTableName));

        this.crateDBSchemaName = crateDBSchemaName;
        this.crateDBTableName = crateDBTableName;
    }

    public static CrateDBTablePath fromFlinkTableName(String flinkTableName) {
        if (flinkTableName.contains(".")) {
            String[] path = flinkTableName.split("\\.");

            checkArgument(
                    path != null && path.length == 2,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            flinkTableName, path.length));

            return new CrateDBTablePath(path[0], path[1]);
        } else {
            return new CrateDBTablePath(DEFAULT_CRATE_SCHEMA_NAME, flinkTableName);
        }
    }

    public static String toFlinkTableName(String schema, String table) {
        return new CrateDBTablePath(schema, table).getFullPath();
    }

    public String getFullPath() {
        return String.format("%s.%s", crateDBSchemaName, crateDBTableName);
    }

    public String getCrateDBTableName() {
        return crateDBTableName;
    }

    public String getCrateDBSchemaName() {
        return crateDBSchemaName;
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

        CrateDBTablePath that = (CrateDBTablePath) o;
        return Objects.equals(crateDBSchemaName, that.crateDBSchemaName)
                && Objects.equals(crateDBTableName, that.crateDBTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(crateDBSchemaName, crateDBTableName);
    }
}
