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
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.table.runtime.functions.table.lookup.LookupCacheManager;
import org.apache.flink.table.test.lookup.cache.LookupCacheAssert;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link JdbcDynamicTableSource}. */
public abstract class JdbcDynamicTableSourceITCase implements DatabaseTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setConfiguration(new Configuration())
                            .build());

    private final TableRow inputTable = createInputTable();

    public static StreamExecutionEnvironment env;
    public static TableEnvironment tEnv;

    protected TableRow createInputTable() {
        return tableRow(
                "jdbDynamicTableSource",
                field("id", DataTypes.BIGINT().notNull()),
                field("decimal_col", DataTypes.DECIMAL(10, 4)),
                field("timestamp6_col", DataTypes.TIMESTAMP(6)));
    }

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(inputTable);
    }

    protected List<Row> getTestData() {
        return Arrays.asList(
                Row.of(
                        1L,
                        BigDecimal.valueOf(100.1234),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456")),
                Row.of(
                        2L,
                        BigDecimal.valueOf(101.1234),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456")));
    }

    @BeforeEach
    void beforeAll() throws SQLException {
        try (Connection conn = getMetadata().getConnection()) {
            inputTable.insertIntoTableValues(conn, getTestData());
        }
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterEach
    void afterEach() {
        StreamTestSink.clear();
    }

    @Test
    void testJdbcSource() {
        String testTable = "testTable";
        tEnv.executeSql(inputTable.getCreateQueryForFlink(getMetadata(), testTable));

        List<Row> collected = executeQuery("SELECT * FROM " + testTable);

        assertThat(collected).containsExactlyInAnyOrderElementsOf(getTestData());
    }

    @Test
    void testProject() {
        String testTable = "testTable";
        tEnv.executeSql(
                inputTable.getCreateQueryForFlink(
                        getMetadata(),
                        testTable,
                        Arrays.asList(
                                "'scan.partition.column'='id'",
                                "'scan.partition.num'='2'",
                                "'scan.partition.lower-bound'='0'",
                                "'scan.partition.upper-bound'='100'")));

        String fields = String.join(",", Arrays.copyOfRange(inputTable.getTableFields(), 0, 3));
        List<Row> collected = executeQuery(String.format("SELECT %s FROM %s", fields, testTable));

        List<Row> expected =
                getTestData().stream()
                        .map(row -> Row.of(row.getField(0), row.getField(1), row.getField(2)))
                        .collect(Collectors.toList());

        assertThat(collected).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testLimit() throws Exception {
        String testTable = "testTable";
        tEnv.executeSql(
                inputTable.getCreateQueryForFlink(
                        getMetadata(),
                        testTable,
                        Arrays.asList(
                                "'scan.partition.column'='id'",
                                "'scan.partition.num'='2'",
                                "'scan.partition.lower-bound'='1'",
                                "'scan.partition.upper-bound'='2'")));

        List<Row> collected = executeQuery("SELECT * FROM " + testTable + " LIMIT 1");

        assertThat(collected).hasSize(1);
        assertThat(getTestData())
                .as("The actual output is not a subset of the expected set.")
                .containsAll(collected);
    }

    @Test
    public void testFilter() throws Exception {
        String testTable = "testTable";
        tEnv.executeSql(inputTable.getCreateQueryForFlink(getMetadata(), testTable));

        // create a partitioned table to ensure no regression
        String partitionedTable = "PARTITIONED_TABLE";
        tEnv.executeSql(
                inputTable.getCreateQueryForFlink(
                        getMetadata(),
                        partitionedTable,
                        Arrays.asList(
                                "'scan.partition.column'='id'",
                                "'scan.partition.num'='1'",
                                "'scan.partition.lower-bound'='1'",
                                "'scan.partition.upper-bound'='1'")));

        // we create a VIEW here to test column remapping, ie. would filter push down work if we
        // create a view that depends on our source table
        tEnv.executeSql(
                String.format(
                        "CREATE VIEW FAKE_TABLE (idx, %s) as (SELECT * from %s )",
                        Arrays.stream(inputTable.getTableFields())
                                .filter(f -> !f.equals("id"))
                                .collect(Collectors.joining(",")),
                        testTable));

        Row onlyRow1 =
                getTestData().stream()
                        .filter(row -> row.getFieldAs(0).equals(1L))
                        .findAny()
                        .orElseThrow(NullPointerException::new);

        Row onlyRow2 =
                getTestData().stream()
                        .filter(row -> row.getFieldAs(0).equals(2L))
                        .findAny()
                        .orElseThrow(NullPointerException::new);

        List<Row> twoRows = getTestData();

        // test simple filter
        assertThat(executeQuery("SELECT * FROM FAKE_TABLE WHERE idx = 1"))
                .containsExactly(onlyRow1);

        // test TIMESTAMP filter
        assertThat(
                        executeQuery(
                                "SELECT * FROM FAKE_TABLE WHERE timestamp6_col = TIMESTAMP '2020-01-01 15:35:00.123456'"))
                .containsExactly(onlyRow1);

        // test the IN operator
        assertThat(
                        executeQuery(
                                "SELECT * FROM FAKE_TABLE WHERE 1 = idx AND decimal_col IN (100.1234, 101.1234)"))
                .containsExactly(onlyRow1);

        // test mixing AND and OR operator
        assertThat(
                        executeQuery(
                                "SELECT * FROM FAKE_TABLE WHERE idx = 1 AND decimal_col = 100.1234 OR decimal_col = 101.1234"))
                .containsExactlyInAnyOrderElementsOf(twoRows);

        // test mixing AND/OR with parenthesis, and the swapping the operand of equal expression
        assertThat(
                        executeQuery(
                                "SELECT * FROM FAKE_TABLE WHERE (2 = idx AND decimal_col = 100.1234) OR decimal_col = 101.1234"))
                .containsExactly(onlyRow2);

        // test Greater than, just to make sure we didnt break anything that we cannot pushdown
        assertThat(
                        executeQuery(
                                "SELECT * FROM FAKE_TABLE WHERE idx = 2 AND decimal_col > 100 OR decimal_col = 101.123"))
                .containsExactly(onlyRow2);

        // One more test of parenthesis
        assertThat(
                        executeQuery(
                                "SELECT * FROM FAKE_TABLE WHERE 2 = idx AND (decimal_col = 100.1234 OR decimal_col = 102.1234)"))
                .isEmpty();

        assertThat(
                        executeQuery(
                                "SELECT * FROM "
                                        + partitionedTable
                                        + " WHERE id = 2 AND decimal_col > 100 OR decimal_col = 101.123"))
                .isEmpty();

        assertThat(
                        executeQuery(
                                "SELECT * FROM "
                                        + partitionedTable
                                        + " WHERE 1 = id AND decimal_col IN (100.1234, 101.1234)"))
                .containsExactly(onlyRow1);
    }

    @ParameterizedTest
    @EnumSource(Caching.class)
    void testLookupJoin(Caching caching) throws Exception {
        // Create JDBC lookup table
        List<String> cachingOptions = Collections.emptyList();
        if (caching.equals(Caching.ENABLE_CACHE)) {
            cachingOptions =
                    Arrays.asList(
                            "'lookup.cache.max-rows' = '100'", "'lookup.cache.ttl' = '10min'");
        }
        tEnv.executeSql(
                inputTable.getCreateQueryForFlink(getMetadata(), "jdbc_lookup", cachingOptions));

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
                        "CREATE TABLE value_source ( "
                                + " `id` BIGINT, "
                                + " `name` STRING, "
                                + " `proctime` AS PROCTIME()"
                                + ") WITH ("
                                + " 'connector' = 'values', "
                                + " 'data-id' = '%s'"
                                + ")",
                        dataId));

        if (caching == Caching.ENABLE_CACHE) {
            LookupCacheManager.keepCacheOnRelease(true);
        }

        // Execute lookup join
        try {
            List<Row> collected =
                    executeQuery(
                            "SELECT S.id, S.name, D.id, D.timestamp6_col, D.decimal_col FROM value_source"
                                    + " AS S JOIN jdbc_lookup for system_time as of S.proctime AS D ON S.id = D.id");

            assertThat(collected).hasSize(3);

            List<Row> expected =
                    Arrays.asList(
                            Row.of(
                                    1L,
                                    "Alice",
                                    1L,
                                    LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                                    BigDecimal.valueOf(100.1234)),
                            Row.of(
                                    1L,
                                    "Alice",
                                    1L,
                                    LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                                    BigDecimal.valueOf(100.1234)),
                            Row.of(
                                    2L,
                                    "Bob",
                                    2L,
                                    LocalDateTime.parse("2020-01-01T15:36:01.123456"),
                                    BigDecimal.valueOf(101.1234)));

            assertThat(collected)
                    .as("The actual output is not a subset of the expected set")
                    .containsAll(expected);

            if (caching == Caching.ENABLE_CACHE) {
                validateCachedValues();
            }
        } finally {
            if (caching == Caching.ENABLE_CACHE) {
                LookupCacheManager.getInstance().checkAllReleased();
                LookupCacheManager.getInstance().clear();
                LookupCacheManager.keepCacheOnRelease(false);
            }
        }
    }

    private List<Row> executeQuery(String query) {
        return CollectionUtil.iteratorToList(tEnv.executeSql(query).collect());
    }

    private void validateCachedValues() {
        // Validate cache
        Map<String, LookupCacheManager.RefCountedCache> managedCaches =
                LookupCacheManager.getInstance().getManagedCaches();
        assertThat(managedCaches).as("There should be only 1 shared cache registered").hasSize(1);
        LookupCache cache = managedCaches.get(managedCaches.keySet().iterator().next()).getCache();
        // jdbc does support project push down, the cached row has been projected
        RowData key1 = GenericRowData.of(1L);
        RowData value1 =
                GenericRowData.of(
                        1L,
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(100.1234), 10, 4),
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2020-01-01T15:35:00.123456")));

        RowData key2 = GenericRowData.of(2L);
        RowData value2 =
                GenericRowData.of(
                        2L,
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(101.1234), 10, 4),
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2020-01-01T15:36:01.123456")));

        RowData key3 = GenericRowData.of(3L);

        Map<RowData, Collection<RowData>> expectedEntries = new HashMap<>();
        expectedEntries.put(key1, Collections.singletonList(value1));
        expectedEntries.put(key2, Collections.singletonList(value2));
        expectedEntries.put(key3, Collections.emptyList());

        LookupCacheAssert.assertThat(cache).containsExactlyEntriesOf(expectedEntries);
    }

    private enum Caching {
        ENABLE_CACHE,
        DISABLE_CACHE
    }
}
