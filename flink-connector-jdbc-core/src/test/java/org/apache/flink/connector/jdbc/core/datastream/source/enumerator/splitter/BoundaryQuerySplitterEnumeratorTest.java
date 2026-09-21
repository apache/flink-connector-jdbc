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

import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.derby.database.dialect.DerbyDialect;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Tests {@link BoundaryQuerySplitterEnumerator} against a real, in-memory H2 database - no
 * Testcontainers/Docker required since the class under test only relies on plain JDBC.
 */
class BoundaryQuerySplitterEnumeratorTest {

    private Connection connection;
    private JdbcDialect dialect;

    @BeforeEach
    void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        connection =
                DriverManager.getConnection(
                        "jdbc:h2:mem:boundary_query_test_"
                                + UUID.randomUUID()
                                + ";DB_CLOSE_DELAY=-1");
        // Derby and H2 share ANSI-standard identifier/predicate syntax, and quoteIdentifier() is
        // the only dialect behavior this class relies on.
        dialect = new DerbyDialect();
    }

    @AfterEach
    void tearDown() throws Exception {
        connection.close();
    }

    @Test
    void testHappyPathProducesExpectedPartitions() throws Exception {
        BoundaryQuerySplitterEnumerator enumerator =
                newEnumerator("SELECT v FROM (VALUES (100),(200),(300)) AS t(v)", 4);

        List<JdbcSourceSplit> splits = enumerator.enumerateSplits();

        assertThat(splits).hasSize(4);

        assertThat(splits.get(0).getSqlTemplate()).contains("id < ? OR id IS NULL");
        assertThat(splits.get(0).getParameters()).containsExactly(100);

        assertThat(splits.get(1).getSqlTemplate()).contains("id >= ? AND id < ?");
        assertThat(splits.get(1).getParameters()).containsExactly(100, 200);

        assertThat(splits.get(2).getParameters()).containsExactly(200, 300);

        assertThat(splits.get(3).getSqlTemplate()).contains("id >= ?");
        assertThat(splits.get(3).getParameters()).containsExactly(300);
    }

    @Test
    void testUnsortedBoundaryQueryIsOrderedAutomatically() throws Exception {
        BoundaryQuerySplitterEnumerator enumerator =
                newEnumerator("SELECT v FROM (VALUES (300),(100),(200)) AS t(v)", 4);

        List<JdbcSourceSplit> splits = enumerator.enumerateSplits();

        assertThat(splits).hasSize(4);
        assertThat(splits.get(0).getParameters()).containsExactly(100);
        assertThat(splits.get(1).getParameters()).containsExactly(100, 200);
        assertThat(splits.get(2).getParameters()).containsExactly(200, 300);
        assertThat(splits.get(3).getParameters()).containsExactly(300);
    }

    @Test
    void testMultiColumnBoundaryQueryIsRejected() {
        BoundaryQuerySplitterEnumerator enumerator =
                newEnumerator("SELECT v, v AS v2 FROM (VALUES (100)) AS t(v)", 4);

        Throwable thrown = catchThrowable(enumerator::enumerateSplits);

        assertThat(thrown).hasCauseInstanceOf(IllegalStateException.class);
        assertThat(thrown.getCause()).hasMessageContaining("exactly one column");
    }

    @Test
    void testNullBoundaryValueIsRejected() {
        BoundaryQuerySplitterEnumerator enumerator =
                newEnumerator("SELECT v FROM (VALUES (100), (CAST(NULL AS INT))) AS t(v)", 4);

        Throwable thrown = catchThrowable(enumerator::enumerateSplits);

        assertThat(thrown).hasCauseInstanceOf(IllegalStateException.class);
        assertThat(thrown.getCause()).hasMessageContaining("NULL");
    }

    @Test
    void testTooManyBoundaryValuesIsRejected() {
        // numPartitions=2 allows at most 1 boundary value, but 2 are returned.
        BoundaryQuerySplitterEnumerator enumerator =
                newEnumerator("SELECT v FROM (VALUES (100),(200)) AS t(v)", 2);

        Throwable thrown = catchThrowable(enumerator::enumerateSplits);

        assertThat(thrown).hasCauseInstanceOf(IllegalStateException.class);
        assertThat(thrown.getCause()).hasMessageContaining("exceeding");
    }

    @Test
    void testFewerBoundaryValuesReducesPartitionCount() throws Exception {
        // numPartitions=10 allows up to 9 boundary values, but only 1 is returned -> 2 partitions.
        BoundaryQuerySplitterEnumerator enumerator =
                newEnumerator("SELECT v FROM (VALUES (100)) AS t(v)", 10);

        List<JdbcSourceSplit> splits = enumerator.enumerateSplits();

        assertThat(splits).hasSize(2);
    }

    @Test
    void testNoBoundaryValuesProducesSingleUnfilteredSplit() throws Exception {
        BoundaryQuerySplitterEnumerator enumerator =
                newEnumerator("SELECT v FROM (VALUES (100)) AS t(v) WHERE v > 999", 5);

        List<JdbcSourceSplit> splits = enumerator.enumerateSplits();

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getSqlTemplate()).doesNotContain("WHERE");
        assertThat(splits.get(0).getParameters()).isNull();
    }

    @Test
    void testStringColumnBoundaryQueryProducesExpectedPartitions() throws Exception {
        BoundaryQuerySplitterEnumerator enumerator =
                newEnumerator(
                        "SELECT v FROM (VALUES ('bravo'),('delta')) AS t(v)",
                        3,
                        new VarCharType());

        List<JdbcSourceSplit> splits = enumerator.enumerateSplits();

        assertThat(splits).hasSize(3);
        assertThat(splits.get(0).getParameters()).containsExactly("bravo");
        assertThat(splits.get(1).getParameters()).containsExactly("bravo", "delta");
        assertThat(splits.get(2).getParameters()).containsExactly("delta");
    }

    @Test
    void testLineageQueriesReportsBaseSqlTemplateNotBoundaryQuery() {
        BoundaryQuerySplitterEnumerator enumerator =
                newEnumerator("SELECT v FROM (VALUES (100)) AS t(v)", 4);

        assertThat(enumerator.lineageQueries()).containsExactly("SELECT * FROM my_table");
    }

    @Test
    void testIncomparableBoundaryColumnTypeIsRejected() {
        // Boundary query returns a string column, but the partition column is numeric.
        BoundaryQuerySplitterEnumerator enumerator =
                newEnumerator("SELECT v FROM (VALUES ('100')) AS t(v)", 4, new IntType());

        Throwable thrown = catchThrowable(enumerator::enumerateSplits);

        assertThat(thrown).hasCauseInstanceOf(IllegalStateException.class);
        assertThat(thrown.getCause()).hasMessageContaining("not comparable");
    }

    private BoundaryQuerySplitterEnumerator newEnumerator(String boundaryQuery, int numPartitions) {
        return newEnumerator(boundaryQuery, numPartitions, new IntType());
    }

    private BoundaryQuerySplitterEnumerator newEnumerator(
            String boundaryQuery, int numPartitions, LogicalType partitionColumnType) {
        BoundaryQuerySplitterEnumerator enumerator =
                new BoundaryQuerySplitterEnumerator(
                        "SELECT * FROM my_table",
                        boundaryQuery,
                        "id",
                        partitionColumnType,
                        dialect,
                        numPartitions);
        enumerator.start(new StaticConnectionProvider(connection));
        return enumerator;
    }

    /** Hands back a single, already-open connection - all this test needs. */
    private static class StaticConnectionProvider implements JdbcConnectionProvider {
        private final Connection connection;

        StaticConnectionProvider(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }

        @Override
        public boolean isConnectionValid() {
            return true;
        }

        @Override
        public Connection getOrEstablishConnection() {
            return connection;
        }

        @Override
        public void closeConnection() {
            // no-op: connection lifecycle is managed by the test itself
        }

        @Override
        public Connection reestablishConnection() {
            return connection;
        }
    }
}
