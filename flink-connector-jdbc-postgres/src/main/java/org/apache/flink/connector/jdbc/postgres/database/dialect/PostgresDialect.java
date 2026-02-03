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
public class PostgresDialect extends AbstractDialect {

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
     * <p>This method generates SQL that uses PostgreSQL's UNNEST() function for bulk inserts.
     * Instead of executing multiple INSERT statements, it creates a single INSERT with arrays,
     * providing significant performance improvements (5-10x) and ensuring a single query plan
     * regardless of batch size.
     *
     * <p>Example output for a table with columns (id, name, age):
     * <pre>
     * INSERT INTO users (id, name, age)
     * SELECT * FROM UNNEST(?::INTEGER[], ?::VARCHAR[], ?::INTEGER[]) AS t(id, name, age)
     * </pre>
     *
     * @param tableName the target table name
     * @param fieldNames array of field names in the order they appear in the table
     * @param fieldTypes array of SQL type names corresponding to each field
     * @return Optional containing the UNNEST SQL statement
     */
    @Override
    public Optional<String> getBatchInsertStatement(
            String tableName,
            String[] fieldNames,
            String[] fieldTypes) {

        if (fieldNames.length == 0) {
            return Optional.empty();
        }

        StringBuilder sql = new StringBuilder();

        // INSERT INTO table_name
        sql.append("INSERT INTO ").append(quoteIdentifier(tableName));

        // (col1, col2, col3)
        sql.append(" (");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(fieldNames[i]));
        }
        sql.append(")");

        // SELECT * FROM UNNEST
        sql.append(" SELECT * FROM UNNEST(");

        // ?::type1[], ?::type2[], ?::type3[]
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String baseType = extractBaseType(fieldTypes[i]);
            sql.append("?::").append(baseType).append("[]");
        }

        // AS t(col1, col2, col3)
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
     * Generate UNNEST-based batch upsert statement for PostgreSQL.
     *
     * <p>This extends the batch insert statement with PostgreSQL's ON CONFLICT clause to handle
     * upsert semantics. The statement will insert new rows and update existing ones on conflict.
     *
     * <p>Example output:
     * <pre>
     * INSERT INTO users (id, name, age)
     * SELECT * FROM UNNEST(?::INTEGER[], ?::VARCHAR[], ?::INTEGER[]) AS t(id, name, age)
     * ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, age=EXCLUDED.age
     * </pre>
     *
     * @param tableName the target table name
     * @param fieldNames array of all field names
     * @param fieldTypes array of SQL type names
     * @param uniqueKeyFields array of unique key field names for conflict detection
     * @return Optional containing the UNNEST upsert SQL statement
     */
    @Override
    public Optional<String> getBatchUpsertStatement(
            String tableName,
            String[] fieldNames,
            String[] fieldTypes,
            String[] uniqueKeyFields) {

        Optional<String> batchInsert =
                getBatchInsertStatement(tableName, fieldNames, fieldTypes);

        if (!batchInsert.isPresent()) {
            return Optional.empty();
        }

        StringBuilder sql = new StringBuilder(batchInsert.get());

        // ON CONFLICT (key1, key2)
        sql.append(" ON CONFLICT (");
        for (int i = 0; i < uniqueKeyFields.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(uniqueKeyFields[i]));
        }

        // Determine non-key fields for UPDATE clause
        Set<String> keySet = new HashSet<>(Arrays.asList(uniqueKeyFields));
        List<String> nonKeyFields =
                Arrays.stream(fieldNames)
                        .filter(f -> !keySet.contains(f))
                        .collect(Collectors.toList());

        if (nonKeyFields.isEmpty()) {
            sql.append(") DO NOTHING");
        } else {
            // DO UPDATE SET col1=EXCLUDED.col1, col2=EXCLUDED.col2
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
     * Extract base type name for PostgreSQL array creation.
     *
     * <p>PostgreSQL's createArrayOf() requires base type names without:
     * <ul>
     *   <li>Array brackets: text[] → text</li>
     *   <li>Length modifiers: varchar(255) → varchar</li>
     *   <li>Precision/scale: numeric(10,2) → numeric</li>
     * </ul>
     *
     * @param typeName the full SQL type name
     * @return the base type name suitable for createArrayOf()
     */
    private String extractBaseType(String typeName) {
        // Remove array brackets: text[][] -> text
        typeName = typeName.replaceAll("\\[\\]", "").trim();

        // Remove precision/scale modifiers: varchar(255) -> varchar, numeric(10,2) -> numeric
        int parenIndex = typeName.indexOf('(');
        if (parenIndex > 0) {
            typeName = typeName.substring(0, parenIndex).trim();
        }

        return typeName.toUpperCase();
    }

    /**
     * Get PostgreSQL type name for array operations.
     *
     * <p>Returns the PostgreSQL type name used for:
     * <ul>
     *   <li>{@code Connection.createArrayOf(typeName, values)}</li>
     *   <li>SQL type casting: {@code ?::typename[]}</li>
     * </ul>
     *
     * <p>PostgreSQL is case-insensitive for type names, but we follow PostgreSQL's convention of
     * using lowercase type names as shown in {@code \dT} command output.
     *
     * @param logicalType the Flink logical type
     * @return PostgreSQL type name (lowercase, following PostgreSQL convention)
     * @throws UnsupportedOperationException if the type is not supported for UNNEST
     */
    @Override
    public String getArrayTypeName(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return "boolean";
            case TINYINT:
            case SMALLINT:
                return "smallint";
            case INTEGER:
                return "integer";
            case BIGINT:
                return "bigint";
            case FLOAT:
                return "real";
            case DOUBLE:
                return "double precision";
            case DECIMAL:
                return "numeric";
            case CHAR:
            case VARCHAR:
                return "varchar";
            case DATE:
                return "date";
            case TIME_WITHOUT_TIME_ZONE:
                return "time";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "timestamp";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "timestamptz";
            case VARBINARY:
                return "bytea";
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Type %s is not supported for UNNEST optimization. "
                                        + "Please disable UNNEST by setting 'sink.postgres.unnest.enabled' = 'false'.",
                                logicalType));
        }
    }
}
