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

package org.apache.flink.connector.jdbc.mysql.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** JDBC dialect for DaMeng. */
@Internal
public class DaMengDialect
        extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to DaMeng docs
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 0;

    // Define MAX/MIN precision of DECIMAL type according to DaMeng docs
    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public JdbcDialectConverter getRowConverter(RowType rowType) {
        return new DaMengDialectConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "FETCH FIRST " + limit + " ROWS ONLY";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("dm.jdbc.driver.DmDriver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    /**
     * DaMeng upsert query using MERGE INTO.
     *
     * <p>NOTE: It requires DaMeng's primary key to be consistent with pkFields.
     */
    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String fieldsNameList = Arrays.stream(fieldNames)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String valuesClause = Arrays.stream(fieldNames)
                .map(field -> ":" + field)
                .collect(Collectors.joining(", "));

        String onClause = Arrays.stream(uniqueKeyFields)
                .map(field -> "t." + quoteIdentifier(field) + " = s." + quoteIdentifier(field))
                .collect(Collectors.joining(" AND "));

        String updateClause = Arrays.stream(fieldNames)
                .map(field -> "t." + quoteIdentifier(field) + " = s." + quoteIdentifier(field))
                .collect(Collectors.joining(", "));

        String mergeInto = "MERGE INTO " + tableName + " t " +
                "USING (SELECT " +
                valuesClause +
                " FROM DUAL) s(" +
                fieldsNameList +
                ") ON (" +
                onClause +
                ") " +
                "WHEN MATCHED THEN UPDATE SET " +
                updateClause +
                " WHEN NOT MATCHED THEN " +
                "INSERT (" +
                fieldsNameList +
                ") VALUES (" +
                Arrays.stream(fieldNames)
                        .map(field -> "s." + quoteIdentifier(field))
                        .collect(Collectors.joining(", ")) +
                ")";

        return Optional.of(mergeInto);
    }

    @Override
    public String dialectName() {
        return "DaMeng";
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
