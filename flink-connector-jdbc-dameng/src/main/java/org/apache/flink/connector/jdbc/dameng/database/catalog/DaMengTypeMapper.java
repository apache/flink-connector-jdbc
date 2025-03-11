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

package org.apache.flink.connector.jdbc.dameng.database.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/** DaMengTypeMapper util class. */
@Internal
public class DaMengTypeMapper
        implements JdbcCatalogTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(DaMengTypeMapper.class);

    // ============================data types=====================

    private static final String DM_UNKNOWN = "UNKNOWN";
    private static final String DM_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String DM_TINYINT = "TINYINT";
    private static final String DM_SMALLINT = "SMALLINT";
    private static final String DM_INT = "INT";
    private static final String DM_INTEGER = "INTEGER";
    private static final String DM_BIGINT = "BIGINT";
    private static final String DM_DECIMAL = "DECIMAL";
    private static final String DM_NUMERIC = "NUMERIC";
    private static final String DM_NUMBER = "NUMBER";
    private static final String DM_FLOAT = "FLOAT";
    private static final String DM_DOUBLE = "DOUBLE";
    private static final String DM_REAL = "REAL";
    private static final String DM_DOUBLE_PRECISION = "DOUBLE PRECISION";

    // -------------------------string----------------------------
    private static final String DM_CHAR = "CHAR";
    private static final String DM_CHARACTER = "CHARACTER";
    private static final String DM_VARCHAR = "VARCHAR";
    private static final String DM_VARCHAR2 = "VARCHAR2";
    private static final String DM_TEXT = "TEXT";
    private static final String DM_LONGVARCHAR = "LONGVARCHAR";
    private static final String DM_CLOB = "CLOB";

    // ------------------------------time-------------------------
    private static final String DM_DATE = "DATE";
    private static final String DM_TIME = "TIME";
    private static final String DM_TIMESTAMP = "TIMESTAMP";
    private static final String DM_DATETIME = "DATETIME";

    // ------------------------------blob-------------------------
    private static final String DM_BINARY = "BINARY";
    private static final String DM_VARBINARY = "VARBINARY";
    private static final String DM_BLOB = "BLOB";
    private static final String DM_IMAGE = "IMAGE";
    private static final String DM_BFILE = "BFILE";

    // -------------------------boolean---------------------------
    private static final String DM_BOOLEAN = "BOOLEAN";

    // -------------------------other-----------------------------
    private static final String DM_INTERVAL = "INTERVAL";

    private static final int RAW_TIME_LENGTH = 8;
    private static final int RAW_TIMESTAMP_LENGTH = 19;

    private final String databaseVersion;
    private final String driverVersion;

    public DaMengTypeMapper(String databaseVersion, String driverVersion) {
        this.databaseVersion = databaseVersion;
        this.driverVersion = driverVersion;
    }

    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String dmType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        switch (dmType) {
            case DM_BIT:
            case DM_BOOLEAN:
                return DataTypes.BOOLEAN();
            case DM_BINARY:
            case DM_VARBINARY:
            case DM_BLOB:
            case DM_IMAGE:
            case DM_BFILE:
                return DataTypes.BYTES();
            case DM_TINYINT:
                return DataTypes.TINYINT();
            case DM_SMALLINT:
                return DataTypes.SMALLINT();
            case DM_INT:
            case DM_INTEGER:
                return DataTypes.INT();
            case DM_BIGINT:
                return DataTypes.BIGINT();
            case DM_DECIMAL:
            case DM_NUMERIC:
            case DM_NUMBER:
                return DataTypes.DECIMAL(precision, scale);
            case DM_FLOAT:
            case DM_REAL:
                return DataTypes.FLOAT();
            case DM_DOUBLE:
            case DM_DOUBLE_PRECISION:
                return DataTypes.DOUBLE();
            case DM_CHAR:
            case DM_CHARACTER:
                return DataTypes.CHAR(precision);
            case DM_VARCHAR:
            case DM_VARCHAR2:
                return DataTypes.VARCHAR(precision);
            case DM_TEXT:
            case DM_LONGVARCHAR:
            case DM_CLOB:
                return DataTypes.STRING();
            case DM_DATE:
                return DataTypes.DATE();
            case DM_TIME:
                return isExplicitPrecision(precision, RAW_TIME_LENGTH)
                        ? DataTypes.TIME(precision - RAW_TIME_LENGTH - 1)
                        : DataTypes.TIME(0);
            case DM_TIMESTAMP:
            case DM_DATETIME:
                boolean explicitPrecision = isExplicitPrecision(precision, RAW_TIMESTAMP_LENGTH);
                if (explicitPrecision) {
                    int p = precision - RAW_TIMESTAMP_LENGTH - 1;
                    if (p <= 6 && p >= 0) {
                        return DataTypes.TIMESTAMP(p);
                    }
                    return p > 6 ? DataTypes.TIMESTAMP(6) : DataTypes.TIMESTAMP(0);
                }
                return DataTypes.TIMESTAMP(0);
            case DM_INTERVAL:
                // Mapping INTERVAL to STRING since Flink doesn't have a direct equivalent
                return DataTypes.STRING();
            case DM_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support DaMeng type '%s' on column '%s' in DaMeng version %s, driver version %s yet.",
                                dmType, jdbcColumnName, databaseVersion, driverVersion));
        }
    }

    private boolean isExplicitPrecision(int precision, int defaultPrecision) {
        return precision > defaultPrecision && precision - defaultPrecision - 1 <= 9;
    }

    private void checkMaxPrecision(ObjectPath tablePath, String columnName, int precision) {
        if (precision >= DecimalType.MAX_PRECISION) {
            throw new CatalogException(
                    String.format(
                            "Precision %s of table %s column name %s in type %s exceeds "
                                    + "DecimalType.MAX_PRECISION %s.",
                            precision,
                            tablePath.getFullName(),
                            columnName,
                            DM_DECIMAL,
                            DecimalType.MAX_PRECISION));
        }
    }
}
