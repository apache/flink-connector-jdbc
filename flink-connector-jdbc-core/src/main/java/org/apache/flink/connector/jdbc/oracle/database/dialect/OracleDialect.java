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

package org.apache.flink.connector.jdbc.oracle.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * JDBC dialect for Oracle.
 */
@Internal
public class OracleDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to Oracle docs:
    // https://www.techonthenet.com/oracle/datatypes.php
    private static final int MAX_TIMESTAMP_PRECISION = 9;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    // Define MAX/MIN precision of DECIMAL type according to Oracle docs:
    // https://www.techonthenet.com/oracle/datatypes.php
    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final int MIN_DECIMAL_PRECISION = 1;
    private static final String CONFIG_FILE = "/flink/conf/flink-conf.yaml";
    private static final String DEL_OP_COL_CONFIG_KEY = "del.op.col";
    private static String delOpCol;
    private static final String DEL_OP_VAL_CONFIG_KEY = "del.op.val";
    private static String delOpVal;

    private static final Logger logger = LoggerFactory.getLogger(OracleDialect.class);

    @Override
    public JdbcDialectConverter getRowConverter(RowType rowType) {
        return new OracleDialectConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "FETCH FIRST " + limit + " ROWS ONLY";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("oracle.jdbc.OracleDriver");
    }

    @Override
    public String dialectName() {
        return "Oracle";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    private static void readConfigFile() throws FileNotFoundException {
        logger.info("Read config file: {}", CONFIG_FILE);
        File configFile = new File(CONFIG_FILE);
        Scanner scanner = new Scanner(configFile);
        while (scanner.hasNextLine()) {
            String[] configEntry = scanner.nextLine().split(":");
            switch (configEntry[0].trim()) {
                case DEL_OP_COL_CONFIG_KEY:
                    delOpCol = configEntry[1].trim();
                    logger.info("DEL op column: {}", delOpCol);
                    break;
                case DEL_OP_VAL_CONFIG_KEY:
                    delOpVal = configEntry[1].trim();
                    break;
            }
        }
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        boolean processDelOp;
        try {
            readConfigFile();
            processDelOp = delOpCol != null && delOpVal != null;
        } catch (FileNotFoundException fnfe) {
            processDelOp = false;
        }
        boolean finalProcessDelOp = processDelOp;
        String sourceFields =
                Arrays.stream(fieldNames)
                        .map(f -> ":" + f + " " + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        String onClause =
                Arrays.stream(uniqueKeyFields)
                        .map(f -> "t." + quoteIdentifier(f) + "=s." + quoteIdentifier(f))
                        .collect(Collectors.joining(" and "));

        final Set<String> uniqueKeyFieldsSet =
                Arrays.stream(uniqueKeyFields).collect(Collectors.toSet());

        String updateClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !uniqueKeyFieldsSet.contains(f))
                        .map(
                                f -> {
                                    if (finalProcessDelOp) {
                                        StringBuilder builder = new StringBuilder();
                                        builder
                                                .append("t.")
                                                .append(quoteIdentifier(f))
                                                .append("=")
                                                .append(" CASE WHEN (")
                                                .append("s.")
                                                .append(quoteIdentifier(delOpCol))
                                                .append("=")
                                                .append("'")
                                                .append(delOpVal)
                                                .append("')")
                                                .append(" THEN ");
                                        if (f.equalsIgnoreCase(delOpCol)) {
                                            builder.append("s.");
                                        } else {
                                            builder.append("t.");
                                        }
                                        builder.append(quoteIdentifier(f))
                                                .append(" ELSE ")
                                                .append("s.")
                                                .append(quoteIdentifier(f))
                                                .append(" END");
                                        return builder.toString();
                                    } else {
                                        return "t."
                                                + quoteIdentifier(f)
                                                + "="
                                                + "s."
                                                + quoteIdentifier(f);
                                    }
                                })
                        .collect(Collectors.joining(", "));

        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

        String valuesClause =
                Arrays.stream(fieldNames)
                        .map(f -> "s." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        // if we can't divide schema and table-name is risky to call quoteIdentifier(tableName)
        // for example [tbo].[sometable] is ok but [tbo.sometable] is not
        String mergeQuery =
                " MERGE INTO "
                        + tableName
                        + " t "
                        + " USING (SELECT "
                        + sourceFields
                        + " FROM DUAL) s "
                        + " ON ("
                        + onClause
                        + ") "
                        + " WHEN MATCHED THEN UPDATE SET "
                        + updateClause
                        + " WHEN NOT MATCHED THEN INSERT ("
                        + insertFields
                        + ")"
                        + " VALUES ("
                        + valuesClause
                        + ")";
        logger.info("Sink statement: {}", mergeQuery);
        return Optional.of(mergeQuery);
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
        // The data types used in Oracle are list at:
        // https://www.techonthenet.com/oracle/datatypes.php

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
}
