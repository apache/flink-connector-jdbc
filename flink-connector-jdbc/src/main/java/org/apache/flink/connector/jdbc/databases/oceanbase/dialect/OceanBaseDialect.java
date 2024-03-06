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

package org.apache.flink.connector.jdbc.databases.oceanbase.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.databases.mysql.dialect.MySqlDialect;
import org.apache.flink.connector.jdbc.databases.oracle.dialect.OracleDialect;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nonnull;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/** JDBC dialect for OceanBase. */
@Internal
public class OceanBaseDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    private final AbstractDialect dialect;

    public OceanBaseDialect(@Nonnull String compatibleMode) {
        switch (compatibleMode.toLowerCase()) {
            case "mysql":
                this.dialect = new MySqlDialect();
                break;
            case "oracle":
                this.dialect = new OracleDialect();
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported compatible mode: " + compatibleMode);
        }
    }

    @Override
    public String dialectName() {
        return "OceanBase";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.oceanbase.jdbc.Driver");
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

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new OceanBaseRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return dialect.getLimitClause(limit);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return dialect.quoteIdentifier(identifier);
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] conditionFields) {
        return dialect.getUpsertStatement(tableName, fieldNames, conditionFields);
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return dialect.timestampPrecisionRange();
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return dialect.decimalPrecisionRange();
    }

    @Override
    public String appendDefaultUrlProperties(String url) {
        return dialect.appendDefaultUrlProperties(url);
    }
}
