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

package org.apache.flink.connector.jdbc.spanner.database.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.StringUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Table path of Spanner in Flink. Can be of formats "table_name" or "schema_name.table_name".
 * Spanner requires the use of fully qualified names to reference a database object in non-default
 * named schema. The schema name is not required for the default schema.
 * https://cloud.google.com/spanner/docs/named-schemas
 */
@Internal
public class SpannerTablePath {

    private static final String DEFAULT_SCHEMA_NAME = "";

    private final String schemaName;
    private final String tableName;

    public SpannerTablePath(String schemaName, String tableName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "Table name is not valid. Null or empty is not allowed");
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public SpannerTablePath(String tableName) {
        this(DEFAULT_SCHEMA_NAME, tableName);
    }

    public static SpannerTablePath fromFlinkTableName(String flinkTableName) {
        if (flinkTableName.contains(".")) {
            String[] path = flinkTableName.split("\\.");
            checkArgument(
                    path.length == 2,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            flinkTableName, path.length));
            return new SpannerTablePath(path[0], path[1]);
        } else {
            return new SpannerTablePath(flinkTableName);
        }
    }

    public static String toFlinkTableName(String schema, String table) {
        return new SpannerTablePath(schema, table).getFullPath();
    }

    public String getFullPath() {
        if (StringUtils.isNullOrWhitespaceOnly(schemaName)) {
            return tableName;
        } else {
            return String.format("%s.%s", schemaName, tableName);
        }
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
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

        SpannerTablePath that = (SpannerTablePath) o;
        return Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName);
    }
}
