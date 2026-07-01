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
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/** SpannerTypeMapper util class. */
@Internal
public class SpannerTypeMapper implements JdbcCatalogTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerTypeMapper.class);

    private static final String SPANNER_BOOL = "BOOL";
    private static final String SPANNER_INT64 = "INT64";
    private static final String SPANNER_NUMERIC = "NUMERIC";
    private static final String SPANNER_FLOAT64 = "FLOAT64";
    private static final String SPANNER_FLOAT32 = "FLOAT32";
    private static final String SPANNER_STRING = "STRING";
    private static final String SPANNER_JSON = "JSON";
    private static final String SPANNER_PROTO = "PROTO";
    private static final String SPANNER_ENUM = "ENUM";
    private static final String SPANNER_BYTES = "BYTES";
    private static final String SPANNER_TIMESTAMP = "TIMESTAMP";
    private static final String SPANNER_DATE = "DATE";
    private static final String SPANNER_ARRAY = "ARRAY";
    private static final String SPANNER_STRUCT = "STRUCT";

    private static final String SPANNER_ARRAY_BOOL =
            "[Ljava.lang.Boolean;"; // Boolean[].class.getName()
    private static final String SPANNER_ARRAY_BYTES = "[[B"; // byte[][].class.getName()
    // private static final String SPANNER_ARRAY_PROTO = "[[B";
    private static final String SPANNER_ARRAY_DATE = "[Ljava.sql.Date;"; // Date[].class.getName()
    private static final String SPANNER_ARRAY_FLOAT32 =
            "[Ljava.lang.Float;"; // Float[].class.getName()
    private static final String SPANNER_ARRAY_FLOAT64 =
            "[Ljava.lang.Double;"; // Double[].class.getName()
    private static final String SPANNER_ARRAY_INT64 = "[Ljava.lang.Long;"; // Long[].class.getName()
    // private static final String SPANNER_ARRAY_ENUM = "[Ljava.lang.Long;";
    private static final String SPANNER_ARRAY_NUMERIC =
            "[Ljava.math.BigDecimal;"; // BigDecimal[].class.getName()
    private static final String SPANNER_ARRAY_STRING =
            "[Ljava.lang.String;"; // String[].class.getName()
    // private static final String SPANNER_ARRAY_JSON = "[Ljava.lang.String;";
    private static final String SPANNER_ARRAY_TIMESTAMP =
            "[Ljava.sql.Timestamp;"; // Timestamp[].class.getName()

    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        final String spannerType = metadata.getColumnTypeName(colIndex);
        final String spannerClassName = metadata.getColumnClassName(colIndex);
        final int precision = metadata.getPrecision(colIndex);
        final int scale = metadata.getScale(colIndex);
        return getMapping(spannerType, spannerClassName, precision, scale);
    }

    private DataType getMapping(
            String spannerType, String spannerClassName, int precision, int scale) {
        switch (spannerType) {
            case SPANNER_BOOL:
                return DataTypes.BOOLEAN();
            case SPANNER_BYTES:
            case SPANNER_PROTO:
                // The default column display size is returned for precision,
                // so it is handled as a BYTES type, not a VARBINARY type.
                // https://github.com/googleapis/java-spanner-jdbc/blob/v2.26.1/src/main/java/com/google/cloud/spanner/jdbc/JdbcResultSetMetaData.java#L157
                return DataTypes.BYTES();
            case SPANNER_DATE:
                return DataTypes.DATE();
            case SPANNER_FLOAT32:
                return DataTypes.FLOAT();
            case SPANNER_FLOAT64:
                return DataTypes.DOUBLE();
            case SPANNER_INT64:
            case SPANNER_ENUM:
                return DataTypes.BIGINT();
            case SPANNER_NUMERIC:
                // The precision for the numeric type is 14 and the scale is 15, both of which are
                // fixed values.
                // https://github.com/googleapis/java-spanner-jdbc/blob/v2.26.1/src/main/java/com/google/cloud/spanner/jdbc/JdbcResultSetMetaData.java#L149
                // https://github.com/googleapis/java-spanner-jdbc/blob/v2.26.1/src/main/java/com/google/cloud/spanner/jdbc/JdbcResultSetMetaData.java#L168
                // But the document describes it as follows.
                // The GoogleSQL NUMERIC is an exact numeric data type capable of representing an
                // exact numeric value
                // with a precision of 38 and scale of 9.
                // https://cloud.google.com/spanner/docs/working-with-numerics
                return DataTypes.DECIMAL(38, 9);
            case SPANNER_STRING:
                // The default column display size is returned for precision,
                // so it is handled as a STRING type, not a VARCHAR type.
                // https://github.com/googleapis/java-spanner-jdbc/blob/v2.26.1/src/main/java/com/google/cloud/spanner/jdbc/JdbcResultSetMetaData.java#L157
            case SPANNER_JSON:
            case SPANNER_STRUCT:
                return DataTypes.STRING();
            case SPANNER_TIMESTAMP:
                return DataTypes.TIMESTAMP();
            case SPANNER_ARRAY:
                return getArrayMapping(spannerClassName, precision, scale);
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported spanner type: %s", spannerType));
        }
    }

    private DataType getArrayMapping(String spannerClassName, int precision, int scale) {
        switch (spannerClassName) {
            case SPANNER_ARRAY_BOOL:
                return DataTypes.ARRAY(DataTypes.BOOLEAN());
            case SPANNER_ARRAY_BYTES:
                // The default column display size is returned for precision,
                // so it is handled as a BYTES type, not a VARBINARY type.
                return DataTypes.ARRAY(DataTypes.BYTES());
            case SPANNER_ARRAY_DATE:
                return DataTypes.ARRAY(DataTypes.DATE());
            case SPANNER_ARRAY_FLOAT32:
                return DataTypes.ARRAY(DataTypes.FLOAT());
            case SPANNER_ARRAY_FLOAT64:
                return DataTypes.ARRAY(DataTypes.DOUBLE());
            case SPANNER_ARRAY_INT64:
                return DataTypes.ARRAY(DataTypes.BIGINT());
            case SPANNER_ARRAY_NUMERIC:
                return DataTypes.ARRAY(DataTypes.DECIMAL(38, 9));
            case SPANNER_ARRAY_STRING:
                // The default column display size is returned for precision,
                // so it is handled as a STRING type, not a VARCHAR type.
                return DataTypes.ARRAY(DataTypes.STRING());
            case SPANNER_ARRAY_TIMESTAMP:
                return DataTypes.ARRAY(DataTypes.TIMESTAMP());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported spanner array type: " + spannerClassName);
        }
    }
}
