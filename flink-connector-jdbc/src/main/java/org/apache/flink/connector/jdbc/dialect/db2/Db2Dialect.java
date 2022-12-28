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

package org.apache.flink.connector.jdbc.dialect.db2;

import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.internal.converter.Db2RowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class Db2Dialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    private static final int MAX_TIMESTAMP_PRECISION = 9;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    private static final int MAX_DECIMAL_PRECISION = 31;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new Db2RowConverter(rowType);
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.ibm.db2.jcc.DB2Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public String dialectName() {
        return "Db2";
    }

    @Override
    public String getLimitClause(long limit) {
        return String.format("FETCH FIRST %d ROWS ONLY", limit);
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(f -> !Arrays.asList(uniqueKeyFields).contains(f))
                        .collect(Collectors.toList());
        String fieldsProjection =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        String valuesBinding =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));

        String columnBinding =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        String onConditions =
                Arrays.stream(uniqueKeyFields)
                        .map(f -> "TARGET." + quoteIdentifier(f) + "= SOURCE." + quoteIdentifier(f))
                        .collect(Collectors.joining(" AND "));
        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(f -> "TARGET." + quoteIdentifier(f) + "= SOURCE." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String insertValues =
                Arrays.stream(fieldNames)
                        .map(f -> "SOURCE." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        Optional<String> format =
                Optional.of(
                        String.format(
                                "MERGE INTO %s AS TARGET"
                                        + " USING TABLE (VALUES ( %s )) AS SOURCE ( %s )"
                                        + " ON (%s)"
                                        + " WHEN MATCHED THEN"
                                        + " UPDATE SET %s"
                                        + " WHEN NOT MATCHED THEN"
                                        + " INSERT (%s) VALUES (%s);",
                                quoteIdentifier(tableName),
                                valuesBinding,
                                columnBinding,
                                onConditions,
                                updateSetClause,
                                fieldsProjection,
                                insertValues));

        return format;
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
        // The data types used in Db2 are list at
        // https://www.ibm.com/docs/en/db2-for-zos/12?topic=columns-data-types

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
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
    }
}
