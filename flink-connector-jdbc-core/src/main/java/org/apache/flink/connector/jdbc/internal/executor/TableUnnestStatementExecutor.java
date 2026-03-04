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

package org.apache.flink.connector.jdbc.internal.executor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Table API specific UNNEST batch statement executor for PostgreSQL.
 *
 * <p>This executor is optimized for Flink's Table/SQL API and uses PostgreSQL's UNNEST() function
 * for bulk inserts/upserts. It provides significant performance improvements (5-10x) by executing a
 * single INSERT statement with arrays instead of multiple individual INSERTs.
 *
 * <p>Compatible with both INSERT and UPSERT modes when using PostgreSQL.
 */
@Internal
public class TableUnnestStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(TableUnnestStatementExecutor.class);

    private final String sql;
    private final RowType rowType;
    private final JdbcDialect dialect;

    private final List<Object[]> batch;
    private transient PreparedStatement statement;

    /**
     * Create a new table UNNEST batch statement executor.
     *
     * @param sql the UNNEST SQL statement
     * @param rowType the logical row type
     * @param dialect the JDBC dialect
     */
    public TableUnnestStatementExecutor(String sql, RowType rowType, JdbcDialect dialect) {
        this.sql = checkNotNull(sql);
        this.rowType = checkNotNull(rowType);
        this.dialect = checkNotNull(dialect);
        this.batch = new ArrayList<>();
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.statement = connection.prepareStatement(sql);
    }

    @Override
    public void addToBatch(RowData record) {
        List<LogicalType> fieldTypes = rowType.getChildren();
        Object[] values = new Object[fieldTypes.size()];

        for (int i = 0; i < fieldTypes.size(); i++) {
            values[i] = extractValue(record, i, fieldTypes.get(i));
        }

        batch.add(values);
    }

    @Override
    public void executeBatch() throws SQLException {
        if (batch.isEmpty()) {
            return;
        }

        try {
            bindArrays();
            int updateCount = statement.executeUpdate();

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "UNNEST batch operation affected {} rows for {} records",
                        updateCount,
                        batch.size());
            }
        } finally {
            batch.clear();
        }
    }

    /**
     * Bind column arrays to the prepared statement.
     *
     * <p>Collects values column-wise from the batch and creates PostgreSQL arrays for binding.
     */
    private void bindArrays() throws SQLException {
        Connection conn = statement.getConnection();
        List<LogicalType> fieldTypes = rowType.getChildren();
        int fieldCount = fieldTypes.size();

        // Bind column-wise arrays
        for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            List<Object> columnValues = new ArrayList<>(batch.size());

            // Collect this column's values from all rows
            for (Object[] row : batch) {
                columnValues.add(row[fieldIndex]);
            }

            // Get array type name and create array
            LogicalType fieldType = fieldTypes.get(fieldIndex);
            String arrayTypeName = dialect.getArrayTypeName(fieldType);

            Array sqlArray = conn.createArrayOf(arrayTypeName, columnValues.toArray());
            statement.setArray(fieldIndex + 1, sqlArray);

            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Bound array for field {} (type {}) with {} values",
                        fieldIndex,
                        arrayTypeName,
                        columnValues.size());
            }
        }
    }

    /**
     * Extract value from RowData and convert to JDBC-compatible type.
     *
     * @param row the row data
     * @param pos the field position
     * @param type the logical type
     * @return the extracted value in JDBC-compatible format
     */
    private Object extractValue(RowData row, int pos, LogicalType type) {
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
                                "Type %s is not supported for UNNEST optimization. "
                                        + "Please disable UNNEST by setting 'sink.postgres.unnest.enabled' = 'false'.",
                                type));
        }
    }

    @Override
    public String insertSql() {
        return sql;
    }

    @Override
    public void closeStatements() throws SQLException {
        if (statement != null) {
            statement.close();
            statement = null;
        }
    }
}
