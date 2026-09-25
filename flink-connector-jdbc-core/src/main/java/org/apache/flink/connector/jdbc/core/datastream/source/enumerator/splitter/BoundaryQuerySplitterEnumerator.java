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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.core.datastream.source.split.CheckpointedOffset;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link AbstractDynamicSplitterEnumerator} that discovers split boundaries by executing a
 * user-provided {@code scan.partition.boundary-query} against the source database, instead of
 * requiring a statically-known numeric {@code [lower, upper]} range.
 *
 * <p>The boundary query is expected to return a single, comparable column; each returned value
 * (after being sorted ascending) becomes a split point. {@code N} returned values produce {@code N
 * + 1} partitions:
 *
 * <ul>
 *   <li>first: {@code col < v(0) OR col IS NULL}
 *   <li>middle {@code i} (0 &lt; i &lt; N): {@code col >= v(i-1) AND col < v(i)}
 *   <li>last: {@code col >= v(N-1)}
 * </ul>
 *
 * <p>Unlike every other splitter in this connector, the SQL predicate text itself differs between
 * splits rather than only the bound parameter values, since a single {@code BETWEEN ? AND ?}
 * template cannot express the "first"/"last" open-ended ranges above.
 */
@Internal
public class BoundaryQuerySplitterEnumerator extends AbstractDynamicSplitterEnumerator {

    private static final Set<Integer> NUMERIC_SQL_TYPES =
            new HashSet<>(
                    Arrays.asList(
                            Types.TINYINT,
                            Types.SMALLINT,
                            Types.INTEGER,
                            Types.BIGINT,
                            Types.DECIMAL,
                            Types.NUMERIC,
                            Types.FLOAT,
                            Types.REAL,
                            Types.DOUBLE));

    private static final Set<Integer> STRING_SQL_TYPES =
            new HashSet<>(
                    Arrays.asList(
                            Types.CHAR,
                            Types.VARCHAR,
                            Types.LONGVARCHAR,
                            Types.NCHAR,
                            Types.NVARCHAR,
                            Types.LONGNVARCHAR));

    private static final Set<Integer> DATETIME_SQL_TYPES =
            new HashSet<>(
                    Arrays.asList(
                            Types.DATE,
                            Types.TIME,
                            Types.TIMESTAMP,
                            Types.TIME_WITH_TIMEZONE,
                            Types.TIMESTAMP_WITH_TIMEZONE));

    private final String baseSqlTemplate;
    private final String boundaryQuery;
    private final String partitionColumn;
    private final LogicalType partitionColumnType;
    private final JdbcDialect dialect;
    private final int numPartitions;

    public BoundaryQuerySplitterEnumerator(
            String baseSqlTemplate,
            String boundaryQuery,
            String partitionColumn,
            LogicalType partitionColumnType,
            JdbcDialect dialect,
            int numPartitions) {
        this.baseSqlTemplate = Preconditions.checkNotNull(baseSqlTemplate);
        this.boundaryQuery = Preconditions.checkNotNull(boundaryQuery);
        this.partitionColumn = Preconditions.checkNotNull(partitionColumn);
        this.partitionColumnType = Preconditions.checkNotNull(partitionColumnType);
        this.dialect = Preconditions.checkNotNull(dialect);
        Preconditions.checkArgument(numPartitions > 0, "numPartitions must be positive");
        this.numPartitions = numPartitions;
    }

    @Override
    protected List<JdbcSourceSplit> discoverSplits(Connection connection) throws SQLException {
        List<Serializable> boundaryValues = fetchBoundaryValues(connection);
        return buildSplits(boundaryValues);
    }

    @Override
    protected String describeSource() {
        // Used only for error messages and AbstractDynamicSplitterEnumerator's discovery
        // description - deliberately not the same query lineageQueries() reports below.
        return boundaryQuery;
    }

    @Override
    public List<String> lineageQueries() {
        // Report the actual data query for lineage purposes, not the boundary-query used only to
        // discover split points - the latter often targets a different table (e.g. a stats table).
        return Collections.singletonList(baseSqlTemplate);
    }

    private List<Serializable> fetchBoundaryValues(Connection connection) throws SQLException {
        // Wrap with ORDER BY 1 so split points are always ascending, even if the user's query
        // doesn't specify an order itself.
        String wrappedQuery =
                "SELECT * FROM (" + boundaryQuery + ") AS boundary_query_result ORDER BY 1";

        // A misconfigured boundary-query (e.g. missing an aggregation/sampling step) could
        // otherwise return an unbounded number of rows and exhaust JobManager memory before the
        // count is ever checked, so the max-allowed check happens per-row instead of after
        // buffering the full result.
        int maxAllowed = numPartitions - 1;
        List<Serializable> values = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(wrappedQuery);
                ResultSet resultSet = statement.executeQuery()) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            validateSingleColumn(metaData);
            validateColumnTypeComparable(metaData.getColumnType(1));

            while (resultSet.next()) {
                if (values.size() >= maxAllowed) {
                    throw new IllegalStateException(
                            "The boundary-query ["
                                    + boundaryQuery
                                    + "] returned more than "
                                    + maxAllowed
                                    + " values, exceeding the limit allowed for scan.partition.num="
                                    + numPartitions
                                    + ".");
                }
                Object value = resultSet.getObject(1);
                if (value == null) {
                    throw new IllegalStateException(
                            "The boundary-query ["
                                    + boundaryQuery
                                    + "] returned a NULL boundary value, which is not allowed.");
                }
                values.add((Serializable) value);
            }
        }

        return values;
    }

    // Validation happens here, against a live connection at split-discovery time, rather than
    // statically when the job is submitted - unlike every other scan.partition.* option, the
    // legality of a boundary-query can only be known by actually running it.
    private void validateSingleColumn(ResultSetMetaData metaData) throws SQLException {
        if (metaData.getColumnCount() != 1) {
            throw new IllegalStateException(
                    "The boundary-query ["
                            + boundaryQuery
                            + "] must return exactly one column, but returned "
                            + metaData.getColumnCount()
                            + ".");
        }
    }

    private void validateColumnTypeComparable(int sqlType) {
        if (!isTypeComparable(sqlType, partitionColumnType)) {
            throw new IllegalStateException(
                    "The boundary-query's returned column type (java.sql.Types code "
                            + sqlType
                            + ") is not comparable with the type of '"
                            + partitionColumn
                            + "' ("
                            + partitionColumnType
                            + ").");
        }
    }

    private static boolean isTypeComparable(int sqlType, LogicalType partitionColumnType) {
        LogicalTypeRoot root = partitionColumnType.getTypeRoot();
        if (NUMERIC_SQL_TYPES.contains(sqlType)) {
            return root.getFamilies().contains(LogicalTypeFamily.NUMERIC);
        }
        if (STRING_SQL_TYPES.contains(sqlType)) {
            return root.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING);
        }
        if (DATETIME_SQL_TYPES.contains(sqlType)) {
            return root.getFamilies().contains(LogicalTypeFamily.DATETIME);
        }
        return false;
    }

    private List<JdbcSourceSplit> buildSplits(List<Serializable> boundaryValues) {
        int partitionCount = boundaryValues.size() + 1;
        List<JdbcSourceSplit> splits = new ArrayList<>(partitionCount);

        if (partitionCount == 1) {
            splits.add(newSplit(0, baseSqlTemplate, null));
            return splits;
        }

        String quotedColumn = dialect.quoteIdentifier(partitionColumn);
        for (int i = 0; i < partitionCount; i++) {
            String predicate;
            Serializable[] params;
            if (i == 0) {
                predicate = quotedColumn + " < ? OR " + quotedColumn + " IS NULL";
                params = new Serializable[] {boundaryValues.get(0)};
            } else if (i == partitionCount - 1) {
                predicate = quotedColumn + " >= ?";
                params = new Serializable[] {boundaryValues.get(i - 1)};
            } else {
                predicate = quotedColumn + " >= ? AND " + quotedColumn + " < ?";
                params = new Serializable[] {boundaryValues.get(i - 1), boundaryValues.get(i)};
            }
            String sql = baseSqlTemplate + " WHERE (" + predicate + ")";
            splits.add(newSplit(i, sql, params));
        }
        return splits;
    }

    private static JdbcSourceSplit newSplit(
            int index, String sqlTemplate, Serializable[] parameters) {
        return new JdbcSourceSplit(
                String.valueOf(index), sqlTemplate, parameters, new CheckpointedOffset());
    }
}
