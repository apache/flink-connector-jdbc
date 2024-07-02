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

package org.apache.flink.connector.jdbc.databases.oceanbase.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/** OceanBaseTypeMapper util class. */
@Internal
public class OceanBaseTypeMapper implements JdbcDialectTypeMapper {

    private static final int RAW_TIME_LENGTH = 10;
    private static final int RAW_TIMESTAMP_LENGTH = 19;

    private static final int TYPE_BINARY_FLOAT = 100;
    private static final int TYPE_BINARY_DOUBLE = 101;

    private final String compatibleMode;

    public OceanBaseTypeMapper(String compatibleMode) {
        this.compatibleMode = compatibleMode;
    }

    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String typeName = metadata.getColumnTypeName(colIndex).toUpperCase();
        int jdbcType = metadata.getColumnType(colIndex);
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (jdbcType) {
            case Types.BIT:
                return DataTypes.BOOLEAN();
            case Types.TINYINT:
                if (isUnsignedType(typeName) || precision > 4) {
                    return DataTypes.SMALLINT();
                }
                return DataTypes.TINYINT();
            case Types.SMALLINT:
                if (isUnsignedType(typeName)) {
                    return DataTypes.INT();
                }
                return DataTypes.SMALLINT();
            case Types.INTEGER:
                if (!typeName.toUpperCase().startsWith("MEDIUMINT") && isUnsignedType(typeName)) {
                    return DataTypes.BIGINT();
                }
                return DataTypes.INT();
            case Types.BIGINT:
                if (isUnsignedType(typeName)) {
                    return DataTypes.DECIMAL(20, 0);
                }
                return DataTypes.BIGINT();
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.DECIMAL:
                if ("mysql".equalsIgnoreCase(compatibleMode)) {
                    if (isUnsignedType(typeName)) {
                        return getDecimalType(precision + 1, scale);
                    }
                    return getDecimalType(precision, scale);
                }
                return getNumericType(precision, scale);
            case Types.REAL:
            case TYPE_BINARY_FLOAT:
                return DataTypes.FLOAT();
            case Types.DOUBLE:
            case TYPE_BINARY_DOUBLE:
                return DataTypes.DOUBLE();
            case Types.DATE:
                if ("YEAR".equals(typeName)) {
                    return DataTypes.INT();
                }
                return DataTypes.DATE();
            case Types.TIME:
                return isExplicitPrecision(precision, RAW_TIME_LENGTH)
                        ? DataTypes.TIME(precision - RAW_TIME_LENGTH - 1)
                        : DataTypes.TIME(0);
            case Types.TIMESTAMP:
                if (typeName.equalsIgnoreCase("DATE")) {
                    return DataTypes.DATE();
                }
                return isExplicitPrecision(precision, RAW_TIMESTAMP_LENGTH)
                        ? DataTypes.TIMESTAMP(precision - RAW_TIMESTAMP_LENGTH - 1)
                        : DataTypes.TIMESTAMP(0);
            case Types.CHAR:
            case Types.NCHAR:
                return DataTypes.CHAR(precision);
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
                return precision > 0 ? DataTypes.VARCHAR(precision) : DataTypes.STRING();
            case Types.CLOB:
                return DataTypes.STRING();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return DataTypes.BYTES();
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support type '%s' on column '%s'.", typeName, columnName));
        }
    }

    private DataType getNumericType(int precision, int scale) {
        if (precision == 0) {
            return DataTypes.STRING();
        }
        if (scale <= 0) {
            int width = precision - scale;
            if (width < 3) {
                return DataTypes.TINYINT();
            } else if (width < 5) {
                return DataTypes.SMALLINT();
            } else if (width < 10) {
                return DataTypes.INT();
            } else if (width < 19) {
                return DataTypes.BIGINT();
            }
        }
        return getDecimalType(precision, scale);
    }

    private DataType getDecimalType(int precision, int scale) {
        if (precision >= DecimalType.MAX_PRECISION || precision == 0) {
            return DataTypes.STRING();
        }
        return DataTypes.DECIMAL(precision, scale);
    }

    private boolean isUnsignedType(String typeName) {
        return typeName.toUpperCase().contains("UNSIGNED");
    }

    private boolean isExplicitPrecision(int precision, int defaultPrecision) {
        return precision > defaultPrecision
                && (precision - defaultPrecision - 1
                        <= ("mysql".equalsIgnoreCase(compatibleMode) ? 6 : 9));
    }
}
