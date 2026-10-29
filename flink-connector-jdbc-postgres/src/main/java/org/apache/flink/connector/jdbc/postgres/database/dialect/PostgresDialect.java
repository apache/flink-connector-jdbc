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

package org.apache.flink.connector.jdbc.postgres.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcBulkInsertDialect;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** JDBC dialect for PostgreSQL. */
@Internal
public class PostgresDialect extends AbstractDialect implements JdbcBulkInsertDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to PostgreSQL docs:
    // https://www.postgresql.org/docs/12/datatype-datetime.html
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 0;

    // Define MAX/MIN precision of DECIMAL type according to PostgreSQL docs:
    // https://www.postgresql.org/docs/12/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
    private static final int MAX_DECIMAL_PRECISION = 1000;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public PostgresDialectConverter getRowConverter(RowType rowType) {
        return new PostgresDialectConverter(rowType);
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.postgresql.Driver");
    }

    @Override
    public String dialectName() {
        return "PostgreSQL";
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    /** Postgres upsert query. It use ON CONFLICT ... DO UPDATE SET.. to replace into Postgres. */
    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        final Set<String> uniqueKeyFieldsSet = new HashSet<>(Arrays.asList(uniqueKeyFields));
        String updateClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !uniqueKeyFieldsSet.contains(f))
                        .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        String conflictAction =
                updateClause.isEmpty()
                        ? " DO NOTHING"
                        : String.format(" DO UPDATE SET %s", updateClause);
        return Optional.of(
                getInsertIntoStatement(tableName, fieldNames)
                        + " ON CONFLICT ("
                        + uniqueColumns
                        + ")"
                        + conflictAction);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(MIN_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION));
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        // The data types used in PostgreSQL are list at:
        // https://www.postgresql.org/docs/12/datatype.html

        // TODO: We can't convert BINARY data type to
        //  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
        // LegacyTypeInfoDataTypeConverter.

        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                LogicalTypeRoot.ARRAY);
    }

    /**
     * Generate UNNEST-based batch insert statement for PostgreSQL.
     *
     * <p>Example: {@code INSERT INTO users (id, name) SELECT * FROM UNNEST(?::INTEGER[],
     * ?::VARCHAR[]) AS t(id, name)}.
     */
    @Override
    public Optional<String> getBatchInsertStatement(
            String tableName, String[] fieldNames, String[] fieldTypes) {

        if (fieldNames.length == 0) {
            return Optional.empty();
        }

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(quoteIdentifier(tableName));

        sql.append(" (");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(fieldNames[i]));
        }
        sql.append(")");

        sql.append(" SELECT * FROM UNNEST(");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?::").append(extractBaseType(fieldTypes[i])).append("[]");
        }

        sql.append(") AS t(");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(fieldNames[i]));
        }
        sql.append(")");

        return Optional.of(sql.toString());
    }

    /**
     * Generate UNNEST-based batch upsert statement for PostgreSQL, extending the batch insert with
     * {@code ON CONFLICT ... DO UPDATE SET} (or {@code DO NOTHING} when all fields are keys).
     */
    @Override
    public Optional<String> getBatchUpsertStatement(
            String tableName, String[] fieldNames, String[] fieldTypes, String[] uniqueKeyFields) {

        Optional<String> batchInsert = getBatchInsertStatement(tableName, fieldNames, fieldTypes);

        if (!batchInsert.isPresent()) {
            return Optional.empty();
        }

        StringBuilder sql = new StringBuilder(batchInsert.get());

        sql.append(" ON CONFLICT (");
        for (int i = 0; i < uniqueKeyFields.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(uniqueKeyFields[i]));
        }

        Set<String> keySet = new HashSet<>(Arrays.asList(uniqueKeyFields));
        List<String> nonKeyFields =
                Arrays.stream(fieldNames)
                        .filter(f -> !keySet.contains(f))
                        .collect(Collectors.toList());

        if (nonKeyFields.isEmpty()) {
            sql.append(") DO NOTHING");
        } else {
            sql.append(") DO UPDATE SET ");
            for (int i = 0; i < nonKeyFields.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                String field = quoteIdentifier(nonKeyFields.get(i));
                sql.append(field).append("=EXCLUDED.").append(field);
            }
        }

        return Optional.of(sql.toString());
    }

    /**
     * Strip array brackets and precision/scale modifiers from a SQL type name (e.g. {@code
     * VARCHAR(255)[] -> VARCHAR}). Used for {@code createArrayOf()} and SQL casts.
     */
    private String extractBaseType(String typeName) {
        typeName = typeName.replaceAll("\\[\\]", "").trim();

        int parenIndex = typeName.indexOf('(');
        if (parenIndex > 0) {
            typeName = typeName.substring(0, parenIndex).trim();
        }

        return typeName;
    }

    /**
     * PostgreSQL type name used by {@link java.sql.Connection#createArrayOf} and {@code
     * ?::typename[]} casts.
     */
    @Override
    public String getArrayTypeName(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                return "NUMERIC";
            case CHAR:
            case VARCHAR:
                return "VARCHAR";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return "TIME";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "TIMESTAMP";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "TIMESTAMPTZ";
            case VARBINARY:
                return "BYTEA";
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Type %s is not supported for bulk insert. "
                                        + "Supported types are: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, "
                                        + "FLOAT, DOUBLE, DECIMAL, CHAR, VARCHAR, DATE, TIME, TIMESTAMP, "
                                        + "TIMESTAMP_LTZ, VARBINARY. "
                                        + "Please disable it by setting 'sink.bulk-insert.enabled' = 'false'.",
                                logicalType));
        }
    }
}
