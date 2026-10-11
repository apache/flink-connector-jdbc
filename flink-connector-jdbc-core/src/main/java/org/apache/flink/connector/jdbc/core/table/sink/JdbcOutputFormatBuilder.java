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

package org.apache.flink.connector.jdbc.core.table.sink;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcBulkInsertDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferReducedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableInsertOrUpdateStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableSimpleStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableUnnestStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Builder for {@link JdbcOutputFormat} for Table/SQL. */
public class JdbcOutputFormatBuilder implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormatBuilder.class);

    private InternalJdbcConnectionOptions jdbcOptions;
    private JdbcExecutionOptions executionOptions;
    private JdbcDmlOptions dmlOptions;
    private DataType[] fieldDataTypes;

    public JdbcOutputFormatBuilder() {}

    public JdbcOutputFormatBuilder setJdbcOptions(InternalJdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
        return this;
    }

    public JdbcOutputFormatBuilder setJdbcExecutionOptions(JdbcExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }

    public JdbcOutputFormatBuilder setJdbcDmlOptions(JdbcDmlOptions dmlOptions) {
        this.dmlOptions = dmlOptions;
        return this;
    }

    public JdbcOutputFormatBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
        return this;
    }

    public JdbcOutputFormat<RowData, ?, ?> build() {
        checkNotNull(jdbcOptions, "jdbc options can not be null");
        checkNotNull(dmlOptions, "jdbc dml options can not be null");
        checkNotNull(executionOptions, "jdbc execution options can not be null");

        final LogicalType[] logicalTypes =
                Arrays.stream(fieldDataTypes)
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new);

        boolean useBulkInsert = shouldUseBulkInsert(executionOptions, dmlOptions.getDialect());

        if (dmlOptions.getKeyFields().isPresent() && dmlOptions.getKeyFields().get().length > 0) {
            return new JdbcOutputFormat<>(
                    new SimpleJdbcConnectionProvider(jdbcOptions),
                    executionOptions,
                    () -> createBufferReduceExecutor(dmlOptions, logicalTypes, useBulkInsert));
        } else {
            return new JdbcOutputFormat<>(
                    new SimpleJdbcConnectionProvider(jdbcOptions),
                    executionOptions,
                    () ->
                            createSimpleBufferedExecutor(
                                    dmlOptions.getDialect(),
                                    dmlOptions.getFieldNames(),
                                    logicalTypes,
                                    dmlOptions.getTableName(),
                                    useBulkInsert));
        }
    }

    private static boolean shouldUseBulkInsert(
            JdbcExecutionOptions executionOptions, JdbcDialect dialect) {
        if (!executionOptions.isBulkInsertEnabled() || executionOptions.getBatchSize() <= 1) {
            return false;
        }
        if (!(dialect instanceof JdbcBulkInsertDialect)) {
            throw new IllegalStateException(
                    String.format(
                            "Bulk insert is enabled but dialect '%s' does not implement "
                                    + "JdbcBulkInsertDialect. Either switch to a dialect that supports it "
                                    + "or set 'sink.bulk-insert.enabled' = 'false'.",
                            dialect.dialectName()));
        }
        return true;
    }

    private static JdbcBatchStatementExecutor<RowData> createBufferReduceExecutor(
            JdbcDmlOptions opt, LogicalType[] fieldTypes, boolean useBulkInsert) {
        checkArgument(opt.getKeyFields().isPresent());
        JdbcDialect dialect = opt.getDialect();
        String tableName = opt.getTableName();
        String[] pkNames = opt.getKeyFields().get();
        int[] pkFields =
                Arrays.stream(pkNames)
                        .mapToInt(Arrays.asList(opt.getFieldNames())::indexOf)
                        .toArray();
        LogicalType[] pkTypes =
                Arrays.stream(pkFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);

        return new TableBufferReducedStatementExecutor(
                createUpsertRowExecutor(
                        dialect,
                        tableName,
                        opt.getFieldNames(),
                        fieldTypes,
                        pkFields,
                        pkNames,
                        pkTypes,
                        useBulkInsert),
                createDeleteExecutor(dialect, tableName, pkNames, pkTypes),
                createRowKeyExtractor(fieldTypes, pkFields));
    }

    private static JdbcBatchStatementExecutor<RowData> createSimpleBufferedExecutor(
            JdbcDialect dialect,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            String tableName,
            boolean useBulkInsert) {

        return new TableBufferedStatementExecutor(
                createSimpleRowExecutor(dialect, fieldNames, fieldTypes, tableName, useBulkInsert));
    }

    private static JdbcBatchStatementExecutor<RowData> createUpsertRowExecutor(
            JdbcDialect dialect,
            String tableName,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            int[] pkFields,
            String[] pkNames,
            LogicalType[] pkTypes,
            boolean useBulkInsert) {

        if (useBulkInsert) {
            JdbcBulkInsertDialect bulkDialect = (JdbcBulkInsertDialect) dialect;
            String[] fieldTypeNames = getFieldTypeNames(fieldTypes, bulkDialect);
            Optional<String> bulkSql =
                    bulkDialect.getBatchUpsertStatement(
                            tableName, fieldNames, fieldTypeNames, pkNames);

            if (bulkSql.isPresent()) {
                return new TableUnnestStatementExecutor(
                        bulkSql.get(), RowType.of(fieldTypes), bulkDialect);
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Bulk insert is enabled but dialect '%s' returned no upsert statement "
                                        + "for the given inputs.",
                                dialect.dialectName()));
            }
        }

        return dialect.getUpsertStatement(tableName, fieldNames, pkNames)
                .map(sql -> createSimpleRowExecutor(dialect, fieldNames, fieldTypes, sql))
                .orElseGet(
                        () ->
                                createInsertOrUpdateExecutor(
                                        dialect,
                                        tableName,
                                        fieldNames,
                                        fieldTypes,
                                        pkFields,
                                        pkNames,
                                        pkTypes));
    }

    private static JdbcBatchStatementExecutor<RowData> createSimpleRowExecutor(
            JdbcDialect dialect,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            String tableName,
            boolean useBulkInsert) {

        if (useBulkInsert) {
            JdbcBulkInsertDialect bulkDialect = (JdbcBulkInsertDialect) dialect;
            String[] fieldTypeNames = getFieldTypeNames(fieldTypes, bulkDialect);
            Optional<String> bulkSql =
                    bulkDialect.getBatchInsertStatement(tableName, fieldNames, fieldTypeNames);

            if (bulkSql.isPresent()) {
                return new TableUnnestStatementExecutor(
                        bulkSql.get(), RowType.of(fieldTypes), bulkDialect);
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Bulk insert is enabled but dialect '%s' returned no insert statement "
                                        + "for the given inputs.",
                                dialect.dialectName()));
            }
        }

        final String sql = dialect.getInsertIntoStatement(tableName, fieldNames);
        return createSimpleRowExecutor(dialect, fieldNames, fieldTypes, sql);
    }

    private static JdbcBatchStatementExecutor<RowData> createDeleteExecutor(
            JdbcDialect dialect, String tableName, String[] pkNames, LogicalType[] pkTypes) {
        String deleteSql = dialect.getDeleteStatement(tableName, pkNames);
        return createSimpleRowExecutor(dialect, pkNames, pkTypes, deleteSql);
    }

    private static JdbcBatchStatementExecutor<RowData> createSimpleRowExecutor(
            JdbcDialect dialect, String[] fieldNames, LogicalType[] fieldTypes, final String sql) {
        final JdbcDialectConverter rowConverter = dialect.getRowConverter(RowType.of(fieldTypes));
        return new TableSimpleStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames),
                rowConverter);
    }

    private static String[] getFieldTypeNames(
            LogicalType[] fieldTypes, JdbcBulkInsertDialect dialect) {
        String[] typeNames = new String[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            typeNames[i] = dialect.getArrayTypeName(fieldTypes[i]);
        }
        return typeNames;
    }

    private static JdbcBatchStatementExecutor<RowData> createInsertOrUpdateExecutor(
            JdbcDialect dialect,
            String tableName,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            int[] pkFields,
            String[] pkNames,
            LogicalType[] pkTypes) {
        final String existStmt = dialect.getRowExistsStatement(tableName, pkNames);
        final String insertStmt = dialect.getInsertIntoStatement(tableName, fieldNames);
        final String updateStmt = dialect.getUpdateStatement(tableName, fieldNames, pkNames);
        return new TableInsertOrUpdateStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, existStmt, pkNames),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, insertStmt, fieldNames),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, updateStmt, fieldNames),
                dialect.getRowConverter(RowType.of(pkTypes)),
                dialect.getRowConverter(RowType.of(fieldTypes)),
                dialect.getRowConverter(RowType.of(fieldTypes)),
                createRowKeyExtractor(fieldTypes, pkFields));
    }

    private static Function<RowData, RowData> createRowKeyExtractor(
            LogicalType[] logicalTypes, int[] pkFields) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[pkFields.length];
        for (int i = 0; i < pkFields.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
        }
        return row -> getPrimaryKey(row, fieldGetters);
    }

    private static RowData getPrimaryKey(RowData row, RowData.FieldGetter[] fieldGetters) {
        GenericRowData pkRow = new GenericRowData(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return pkRow;
    }
}
