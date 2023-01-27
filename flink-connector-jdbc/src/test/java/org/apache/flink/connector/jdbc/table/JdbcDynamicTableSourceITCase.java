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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.templates.round2.TableManaged;
import org.apache.flink.connector.jdbc.templates.round2.TableRow;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.table.runtime.functions.table.lookup.LookupCacheManager;
import org.apache.flink.table.test.lookup.cache.LookupCacheAssert;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.templates.round2.TableBuilder2.field;
import static org.apache.flink.connector.jdbc.templates.round2.TableBuilder2.tableRow;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link JdbcDynamicTableSource}. */
public class JdbcDynamicTableSourceITCase implements JdbcTestBase {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setConfiguration(new Configuration())
                            .build());

    protected final TableRow inputTable = inputTable();

    public static StreamExecutionEnvironment env;
    public static TableEnvironment tEnv;

    protected TableRow inputTable() {
        return tableRow(
                "jdbcDynamicTableSource",
                field("id", DataTypes.BIGINT().notNull()),
                field("timestamp6_col", "TIMESTAMP", DataTypes.TIMESTAMP(6)),
                field("timestamp9_col", "TIMESTAMP", DataTypes.TIMESTAMP(9)),
                field("time_col", "TIME", DataTypes.TIME()),
                field("real_col", "REAL", DataTypes.FLOAT()),
                field("double_col", DataTypes.DOUBLE()),
                field("decimal_col", DataTypes.DECIMAL(10, 4)));
    }

    protected String[] inputTableValues() {
        return new String[] {
            "1, TIMESTAMP('2020-01-01 15:35:00.123456'), TIMESTAMP('2020-01-01 15:35:00.123456789'), TIME('15:35:00'), 1.175E-37, 1.79769E+308, 100.1234",
            "2, TIMESTAMP('2020-01-01 15:36:01.123456'), TIMESTAMP('2020-01-01 15:36:01.123456789'), TIME('15:36:01'), -1.175E-37, -1.79769E+308, 101.1234"
        };
    }

    protected Row[] rowTableValues() {
        return new Row[] {
            Row.of(
                    1,
                    "2020-01-01T15:35:00.123456",
                    "2020-01-01T15:35:00.123456789",
                    "15:35",
                    1.175E-37,
                    1.79769E+308,
                    100.1234),
            Row.of(
                    2,
                    "2020-01-01T15:36:01.123456",
                    "2020-01-01T15:36:01.123456789",
                    "15:36:01",
                    -1.175E-37,
                    -1.79769E+308,
                    101.1234)
        };
    }

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(inputTable);
    }

    @BeforeEach
    void beforeEach() throws SQLException {
        try (Connection conn = getDbMetadata().getConnection()) {
            inputTable.insertIntoTableValues(conn, inputTableValues());
        }
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterAll
    static void afterAll() {
        StreamTestSink.clear();
    }

    @Test
    public void testJdbcSource() {
        tEnv.executeSql(
                inputTable.getCreateQueryForFlink(getDbMetadata(), inputTable.getTableName()));

        assertQueryEquals("select * from " + inputTable.getTableName());
    }

    @Test
    void testProject() {
        tEnv.executeSql(
                inputTable.getCreateQueryForFlink(
                        getDbMetadata(),
                        inputTable.getTableName(),
                        "'scan.partition.column'='id'",
                        "'scan.partition.num'='2'",
                        "'scan.partition.lower-bound'='0'",
                        "'scan.partition.upper-bound'='100'"));

        String query = "SELECT id,timestamp6_col,decimal_col FROM " + inputTable.getTableName();
        List<Row> expected =
                Arrays.stream(rowTableValues())
                        .map(row -> Row.of(row.getField(0), row.getField(1), row.getField(6)))
                        .collect(Collectors.toList());
        assertQueryEquals(query, expected);
    }

    @Test
    void testLimit() {
        tEnv.executeSql(
                inputTable.getCreateQueryForFlink(
                        getDbMetadata(),
                        inputTable.getTableName(),
                        "'scan.partition.column'='id'",
                        "'scan.partition.num'='2'",
                        "'scan.partition.lower-bound'='1'",
                        "'scan.partition.upper-bound'='2'"));

        String query = "SELECT * FROM " + inputTable.getTableName() + " LIMIT 1";
        assertQueryContains(query);
    }

    @Test
    public void testFilter() throws Exception {
        tEnv.executeSql(
                inputTable.getCreateQueryForFlink(getDbMetadata(), inputTable.getTableName()));

        // create a partitioned table to ensure no regression
        String partitionedTable = "PARTITIONED_TABLE";
        tEnv.executeSql(
                inputTable.getCreateQueryForFlink(
                        getDbMetadata(),
                        partitionedTable,
                        "'scan.partition.column'='id'",
                        "'scan.partition.num'='1'",
                        "'scan.partition.lower-bound'='1'",
                        "'scan.partition.upper-bound'='1'"));

        // we create a VIEW here to test column remapping, ie. would filter push down work if we
        // create a view that depends on our source table
        tEnv.executeSql(
                String.format(
                        "CREATE VIEW FAKE_TABLE ("
                                + "idx, timestamp6_col, timestamp9_col, time_col, real_col, double_col, decimal_col"
                                + ") as (SELECT * from %s )",
                        inputTable.getTableName()));

        List<Row> twoRows = Arrays.asList(rowTableValues());

        List<Row> onlyRow1 = Collections.singletonList(rowTableValues()[0]);

        List<Row> onlyRow2 = Collections.singletonList(rowTableValues()[1]);

        List<Row> noRows = new ArrayList<>();

        // test simple filter
        assertQueryEquals("SELECT * FROM FAKE_TABLE WHERE idx = 1", onlyRow1);
        // test TIMESTAMP filter
        assertQueryEquals(
                "SELECT * FROM FAKE_TABLE WHERE timestamp6_col = TIMESTAMP '2020-01-01 15:35:00.123456'",
                onlyRow1);
        // test the IN operator
        assertQueryEquals(
                "SELECT * FROM FAKE_TABLE"
                        + " WHERE 1 = idx AND decimal_col IN (100.1234, 101.1234)",
                onlyRow1);
        // test mixing AND and OR operator
        assertQueryEquals(
                "SELECT * FROM FAKE_TABLE"
                        + " WHERE idx = 1 AND decimal_col = 100.1234 OR decimal_col = 101.1234",
                twoRows);
        // test mixing AND/OR with parenthesis, and the swapping the operand of equal expression
        assertQueryEquals(
                "SELECT * FROM FAKE_TABLE"
                        + " WHERE (2 = idx AND decimal_col = 100.1234) OR decimal_col = 101.1234",
                onlyRow2);

        // test Greater than, just to make sure we didnt break anything that we cannot pushdown
        assertQueryEquals(
                "SELECT * FROM FAKE_TABLE"
                        + " WHERE idx = 2 AND decimal_col > 100 OR decimal_col = 101.123",
                onlyRow2);

        // One more test of parenthesis
        assertQueryEquals(
                "SELECT * FROM FAKE_TABLE"
                        + " WHERE 2 = idx AND (decimal_col = 100.1234 OR real_col = 101.1234)",
                noRows);

        assertQueryEquals(
                "SELECT * FROM "
                        + partitionedTable
                        + " WHERE id = 2 AND decimal_col > 100 OR decimal_col = 101.123",
                noRows);

        assertQueryEquals(
                "SELECT * FROM "
                        + partitionedTable
                        + " WHERE 1 = id AND decimal_col IN (100.1234, 101.1234)",
                onlyRow1);
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    void testLookupJoin(Caching caching) throws Exception {
        // Create JDBC lookup table
        List<String> cachingOptions = new ArrayList<>();
        if (caching.equals(Caching.ENABLE_CACHE)) {
            cachingOptions.add("'lookup.cache.max-rows'='100'");
            cachingOptions.add("'lookup.cache.ttl'='10min'");
        }
        tEnv.executeSql(
                inputTable.getCreateQueryForFlink(
                        getDbMetadata(), "jdbc_lookup", cachingOptions.toArray(new String[0])));

        // Create and prepare a value source
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(1L, "Alice"),
                                Row.of(1L, "Alice"),
                                Row.of(2L, "Bob"),
                                Row.of(3L, "Charlie")));
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE value_source ("
                                + "`id` BIGINT,"
                                + "`name` STRING,"
                                + "`proctime` AS PROCTIME()"
                                + ") WITH ("
                                + "'connector' = 'values', "
                                + "'data-id' = '%s')",
                        dataId));

        if (caching == Caching.ENABLE_CACHE) {
            LookupCacheManager.keepCacheOnRelease(true);
        }

        // Execute lookup join
        try (CloseableIterator<Row> iterator =
                tEnv.executeSql(
                                "SELECT S.id, S.name, D.id, D.timestamp6_col, D.double_col FROM value_source"
                                        + " AS S JOIN jdbc_lookup for system_time as of S.proctime AS D ON S.id = D.id")
                        .collect()) {

            List<Row> expected = new ArrayList<>();
            expected.add(Row.of(1, "Alice", 1, "2020-01-01T15:35:00.123456", 1.79769E308));
            expected.add(Row.of(1, "Alice", 1, "2020-01-01T15:35:00.123456", 1.79769E308));
            expected.add(Row.of(2, "Bob", 2, "2020-01-01T15:36:01.123456", -1.79769E308));

            assertQueryEquals(iterator, expected);

            if (caching == Caching.ENABLE_CACHE) {
                // Validate cache
                Map<String, LookupCacheManager.RefCountedCache> managedCaches =
                        LookupCacheManager.getInstance().getManagedCaches();
                assertThat(managedCaches)
                        .as("There should be only 1 shared cache registered")
                        .hasSize(1);
                LookupCache cache =
                        managedCaches.get(managedCaches.keySet().iterator().next()).getCache();
                validateCachedValues(cache);
            }

        } finally {
            if (caching == Caching.ENABLE_CACHE) {
                LookupCacheManager.getInstance().checkAllReleased();
                LookupCacheManager.getInstance().clear();
                LookupCacheManager.keepCacheOnRelease(false);
            }
        }
    }

    private void validateCachedValues(LookupCache cache) {
        // jdbc does support project push down, the cached row has been projected
        RowData key1 = GenericRowData.of(1L);
        RowData value1 =
                GenericRowData.of(
                        1L,
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2020-01-01T15:35:00.123456")),
                        Double.valueOf("1.79769E308"));

        RowData key2 = GenericRowData.of(2L);
        RowData value2 =
                GenericRowData.of(
                        2L,
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2020-01-01T15:36:01.123456")),
                        Double.valueOf("-1.79769E308"));

        RowData key3 = GenericRowData.of(3L);

        Map<RowData, Collection<RowData>> expectedEntries = new HashMap<>();
        expectedEntries.put(key1, Collections.singletonList(value1));
        expectedEntries.put(key2, Collections.singletonList(value2));
        expectedEntries.put(key3, Collections.emptyList());

        LookupCacheAssert.assertThat(cache).containsExactlyEntriesOf(expectedEntries);
    }

    private void assertQueryEquals(String query) {
        assertQueryEquals(query, Arrays.asList(rowTableValues()));
    }

    private void assertQueryEquals(String query, List<Row> expected) {
        assertQueryEquals(tEnv.executeSql(query).collect(), expected);
    }

    private void assertQueryEquals(Iterator<Row> rows, List<Row> expected) {
        List<Row> actual = CollectionUtil.iteratorToList(rows);

        assertThat(mapToAssertion(actual)).isEqualTo(mapToAssertion(expected));
    }

    private void assertQueryContains(String query) {
        assertQueryContains(query, Arrays.asList(rowTableValues()));
    }

    private void assertQueryContains(String query, List<Row> expected) {
        List<Row> actual = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect());

        assertThat(mapToAssertion(expected))
                .as("The actual output is not a subset of the expected set.")
                .containsAll(mapToAssertion(actual));
    }

    private List<String> mapToAssertion(List<Row> rows) {
        return rows.stream().map(Row::toString).sorted().collect(Collectors.toList());
    }

    private enum Caching {
        ENABLE_CACHE,
        DISABLE_CACHE
    }
}
