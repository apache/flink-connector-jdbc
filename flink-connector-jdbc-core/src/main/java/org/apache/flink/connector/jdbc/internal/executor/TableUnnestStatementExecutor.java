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
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcBulkInsertDialect;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Table API specific bulk-insert batch statement executor.
 *
 * <p>Uses a {@link JdbcBulkInsertDialect} to generate the SQL and to convert column values into
 * JDBC-compatible objects, which are then bound as SQL arrays (e.g., PostgreSQL's {@code UNNEST}).
 * Compatible with both INSERT and UPSERT modes.
 */
@Internal
public class TableUnnestStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(TableUnnestStatementExecutor.class);

    private final String sql;
    private final RowType rowType;
    private final JdbcBulkInsertDialect dialect;

    private final List<Object[]> batch;
    private transient PreparedStatement statement;

    public TableUnnestStatementExecutor(
            String sql, RowType rowType, JdbcBulkInsertDialect dialect) {
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
            values[i] = dialect.toJdbcValue(record, i, fieldTypes.get(i));
        }

        batch.add(values);
    }

    @Override
    public void executeBatch() throws SQLException {
        if (batch.isEmpty()) {
            return;
        }

        try {
            List<Array> arrays = bindArrays();
            try {
                int updateCount = statement.executeUpdate();

                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Bulk insert batch affected {} rows for {} records",
                            updateCount,
                            batch.size());
                }
            } finally {
                for (Array array : arrays) {
                    array.free();
                }
            }
        } finally {
            batch.clear();
        }
    }

    private List<Array> bindArrays() throws SQLException {
        Connection conn = statement.getConnection();
        List<LogicalType> fieldTypes = rowType.getChildren();
        int fieldCount = fieldTypes.size();
        List<Array> arrays = new ArrayList<>(fieldCount);

        for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            Object[] columnValues = new Object[batch.size()];
            for (int rowIndex = 0; rowIndex < batch.size(); rowIndex++) {
                columnValues[rowIndex] = batch.get(rowIndex)[fieldIndex];
            }

            LogicalType fieldType = fieldTypes.get(fieldIndex);
            String arrayTypeName = dialect.getArrayTypeName(fieldType);

            Array sqlArray = conn.createArrayOf(arrayTypeName, columnValues);
            arrays.add(sqlArray);
            statement.setArray(fieldIndex + 1, sqlArray);

            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Bound array for field {} (type {}) with {} values",
                        fieldIndex,
                        arrayTypeName,
                        columnValues.length);
            }
        }

        return arrays;
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
