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

package org.apache.flink.connector.jdbc.dialect.elastic;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.internal.converter.ElasticRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/** JDBC dialect for Elastic. */
@Internal
public class ElasticDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to Elastic docs:
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-data-types.html
    private static final int MIN_TIMESTAMP_PRECISION = 0;
    private static final int MAX_TIMESTAMP_PRECISION = 9;

    @Override
    public String dialectName() {
        return "Elasticsearch";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.elasticsearch.xpack.sql.jdbc.EsDriver");
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        // The list of types supported by Elastic SQL.
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-data-types.html
        return EnumSet.of(
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.VARCHAR);
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new ElasticRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return '"' + identifier + '"';
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        throw new UnsupportedOperationException("Upsert is not supported.");
    }

    @Override
    public String getInsertIntoStatement(String tableName, String[] fieldNames) {
        throw new UnsupportedOperationException("Insert into is not supported.");
    }

    @Override
    public String getUpdateStatement(
            String tableName, String[] fieldNames, String[] conditionFields) {
        throw new UnsupportedOperationException("Update is not supported.");
    }

    @Override
    public String getDeleteStatement(String tableName, String[] conditionFields) {
        throw new UnsupportedOperationException("Delete is not supported.");
    }
}
