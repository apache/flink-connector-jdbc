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

package org.apache.flink.connector.jdbc.core.database.dialect;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Optional;

/**
 * Capability interface for dialects that support database-specific bulk insert optimizations (e.g.,
 * PostgreSQL's {@code UNNEST}). A dialect that does not implement this interface cannot be used
 * with bulk insert mode; the sink will fail fast at build time rather than silently falling back.
 */
@PublicEvolving
public interface JdbcBulkInsertDialect extends JdbcDialect {

    /**
     * Generate a batch insert statement using the database's bulk insert optimization.
     *
     * @param tableName the target table name
     * @param fieldNames array of field names
     * @param fieldTypes array of database type names for each field
     * @return Optional containing the optimized batch insert SQL, or empty if not applicable for
     *     the given inputs (e.g., no fields)
     */
    Optional<String> getBatchInsertStatement(
            String tableName, String[] fieldNames, String[] fieldTypes);

    /**
     * Generate a batch upsert statement using the database's bulk insert optimization with conflict
     * handling.
     *
     * @param tableName the target table name
     * @param fieldNames array of all field names
     * @param fieldTypes array of database type names for each field
     * @param uniqueKeyFields array of unique key field names for conflict detection
     * @return Optional containing the optimized batch upsert SQL, or empty if not applicable
     */
    Optional<String> getBatchUpsertStatement(
            String tableName, String[] fieldNames, String[] fieldTypes, String[] uniqueKeyFields);

    /**
     * Database-specific type name used for array creation (e.g., {@code
     * Connection.createArrayOf(typeName, values)} and SQL type casting).
     *
     * @param logicalType the Flink logical type
     * @return the database-specific type name for array operations
     * @throws UnsupportedOperationException if the type is not supported for array operations
     */
    String getArrayTypeName(LogicalType logicalType);

    /**
     * Extract a value from {@link RowData} and convert it to a JDBC-compatible Java object suitable
     * for {@link java.sql.Connection#createArrayOf(String, Object[])}. Dialects may override this
     * to handle database-specific types (e.g., PostgreSQL {@code JSONB}, {@code UUID}).
     */
    default Object toJdbcValue(RowData row, int pos, LogicalType type) {
        if (row.isNullAt(pos)) {
            return null;
        }

        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(pos);
            case TINYINT:
                return (short) row.getByte(pos);
            case SMALLINT:
                return row.getShort(pos);
            case INTEGER:
                return row.getInt(pos);
            case BIGINT:
                return row.getLong(pos);
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale())
                        .toBigDecimal();
            case CHAR:
            case VARCHAR:
                return row.getString(pos).toString();
            case DATE:
                return Date.valueOf(LocalDate.ofEpochDay(row.getInt(pos)));
            case TIME_WITHOUT_TIME_ZONE:
                return Time.valueOf(LocalTime.ofNanoOfDay(row.getInt(pos) * 1_000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType tsType = (TimestampType) type;
                return row.getTimestamp(pos, tsType.getPrecision()).toTimestamp();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lztsType = (LocalZonedTimestampType) type;
                return row.getTimestamp(pos, lztsType.getPrecision()).toTimestamp();
            case VARBINARY:
                return row.getBinary(pos);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Type %s is not supported for bulk insert. "
                                        + "Please disable it by setting 'sink.bulk-insert.enabled' = 'false'.",
                                type));
        }
    }
}
