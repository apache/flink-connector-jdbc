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

import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test suite for {@link JdbcRowDataLookupFunction}. */
class JdbcRowDataLookupFunctionTest extends JdbcLookupTestBase {

    private static final String[] fieldNames = new String[] {"id1", "id2", "comment1", "comment2"};
    private static final DataType[] fieldDataTypes =
            new DataType[] {
                DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
            };
    private static final String[] fieldNames2 =
            new String[] {
                "id1", "id2", "comment1", "comment2", "decimal_col", "double_col", "real_col"
            };
    private static final DataType[] fieldDataTypes2 =
            new DataType[] {
                DataTypes.INT(),
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.DECIMAL(10, 4),
                DataTypes.DOUBLE(),
                DataTypes.FLOAT()
            };

    private static final String[] lookupKeys = new String[] {"id1", "id2"};

    @ParameterizedTest(name = "withFailure = {0}")
    @ValueSource(booleans = {false, true})
    void testLookup(boolean withFailure) throws Exception {

        JdbcRowDataLookupFunction lookupFunction = buildRowDataLookupFunction(withFailure);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        lookupFunction.open(null);

        lookupFunction.eval(1, StringData.fromString("1"));
        if (withFailure) {
            // Close connection here, and this will be recovered by retry
            if (lookupFunction.getDbConnection() != null) {
                lookupFunction.getDbConnection().close();
            }
        }
        lookupFunction.eval(2, StringData.fromString("3"));

        List<String> result =
                new ArrayList<>(collector.getOutputs())
                        .stream().map(RowData::toString).sorted().collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add("+I(1,1,11-c1-v1,11-c2-v1)");
        expected.add("+I(1,1,11-c1-v2,11-c2-v2)");
        expected.add("+I(2,3,null,23-c2)");
        Collections.sort(expected);

        assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("lookupWithPredicatesProvider")
    void testEval(TestSpec testSpec) throws Exception {
        JdbcRowDataLookupFunction lookupFunction =
                buildRowDataLookupFunctionWithPredicates(
                        testSpec.withFailure, testSpec.resolvedPredicates, testSpec.pushdownParams);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);
        lookupFunction.open(null);
        lookupFunction.eval(testSpec.keys);

        if (testSpec.withFailure) {
            // Close connection here, and this will be recovered by retry
            if (lookupFunction.getDbConnection() != null) {
                lookupFunction.getDbConnection().close();
            }
        }

        List<String> result =
                new ArrayList<>(collector.getOutputs())
                        .stream().map(RowData::toString).sorted().collect(Collectors.toList());
        Collections.sort(testSpec.expected);
        assertThat(result).isEqualTo(testSpec.expected);
    }

    private static class TestSpec {

        private boolean withFailure;
        private final List<String> resolvedPredicates;
        private final Serializable[] pushdownParams;
        private final Object[] keys;
        private List<String> expected;

        private TestSpec(
                boolean withFailure,
                List<String> resolvedPredicates,
                Serializable[] pushdownParams,
                Object[] keys,
                List<String> expected) {
            this.withFailure = withFailure;
            this.resolvedPredicates = resolvedPredicates;
            this.pushdownParams = pushdownParams;
            this.keys = keys;
            this.expected = expected;
        }

        @Override
        public String toString() {
            return "TestSpec{"
                    + "withFailure="
                    + withFailure
                    + ", resolvedPredicates="
                    + resolvedPredicates
                    + ", pushdownParams="
                    + Arrays.toString(pushdownParams)
                    + ", keys="
                    + Arrays.toString(keys)
                    + ", expected="
                    + expected
                    + '}';
        }
    }

    static Collection<TestSpec> lookupWithPredicatesProvider() {
        return ImmutableList.<TestSpec>builder()
                .addAll(getTestSpecs(true))
                .addAll(getTestSpecs(false))
                .build();
    }

    @NotNull
    private static ImmutableList<TestSpec> getTestSpecs(boolean withFailure) {
        return ImmutableList.of(
                // var char single filter
                new TestSpec(
                        withFailure,
                        Collections.singletonList("(comment1 = ?)"),
                        new Serializable[] {"11-c1-v1"},
                        new Object[] {1, StringData.fromString("1")},
                        Collections.singletonList("+I(1,1,11-c1-v1,11-c2-v1,100.1011,1.1,2.2)")),
                // decimal single filter
                new TestSpec(
                        withFailure,
                        Collections.singletonList("(decimal_col = ?)"),
                        new Serializable[] {BigDecimal.valueOf(100.1011)},
                        new Object[] {1, StringData.fromString("1")},
                        Collections.singletonList("+I(1,1,11-c1-v1,11-c2-v1,100.1011,1.1,2.2)")),
                // real single filter
                new TestSpec(
                        withFailure,
                        Collections.singletonList("(real_col = ?)"),
                        new Serializable[] {2.2},
                        new Object[] {1, StringData.fromString("1")},
                        Arrays.asList(
                                "+I(1,1,11-c1-v1,11-c2-v1,100.1011,1.1,2.2)",
                                "+I(1,1,11-c1-v2,11-c2-v2,100.2022,2.2,2.2)")),
                // double single filter
                new TestSpec(
                        withFailure,
                        Collections.singletonList("(double_col = ?)"),
                        new Serializable[] {
                            1.1,
                        },
                        new Object[] {1, StringData.fromString("1")},
                        Collections.singletonList("+I(1,1,11-c1-v1,11-c2-v1,100.1011,1.1,2.2)")),
                // and
                new TestSpec(
                        withFailure,
                        Collections.singletonList("(real_col = ?) AND (double_col = ?)"),
                        new Serializable[] {2.2, 1.1},
                        new Object[] {1, StringData.fromString("1")},
                        Collections.singletonList("+I(1,1,11-c1-v1,11-c2-v1,100.1011,1.1,2.2)")),
                // or
                new TestSpec(
                        withFailure,
                        Collections.singletonList("(decimal_col = ?) OR (double_col = ?)"),
                        new Serializable[] {BigDecimal.valueOf(100.2022), 1.1},
                        new Object[] {1, StringData.fromString("1")},
                        Arrays.asList(
                                "+I(1,1,11-c1-v1,11-c2-v1,100.1011,1.1,2.2)",
                                "+I(1,1,11-c1-v2,11-c2-v2,100.2022,2.2,2.2)")));
    }

    private JdbcRowDataLookupFunction buildRowDataLookupFunction(boolean withFailure) {
        InternalJdbcConnectionOptions jdbcOptions =
                InternalJdbcConnectionOptions.builder()
                        .setDriverName(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setTableName(LOOKUP_TABLE)
                        .build();

        RowType rowType =
                RowType.of(
                        Arrays.stream(fieldDataTypes)
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new),
                        fieldNames);

        return new JdbcRowDataLookupFunction(
                jdbcOptions,
                withFailure ? 1 : LookupOptions.MAX_RETRIES.defaultValue(),
                fieldNames,
                fieldDataTypes,
                lookupKeys,
                rowType,
                Collections.emptyList(),
                new Serializable[0]);
    }

    private JdbcRowDataLookupFunction buildRowDataLookupFunctionWithPredicates(
            boolean withFailure, List<String> resolvedPredicates, Serializable[] pushdownParams) {
        InternalJdbcConnectionOptions jdbcOptions =
                InternalJdbcConnectionOptions.builder()
                        .setDriverName(getMetadata().getDriverClass())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setTableName(LOOKUP_TABLE)
                        .build();

        RowType rowType =
                RowType.of(
                        Arrays.stream(fieldDataTypes2)
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new),
                        fieldNames2);

        return new JdbcRowDataLookupFunction(
                jdbcOptions,
                withFailure ? 1 : LookupOptions.MAX_RETRIES.defaultValue(),
                fieldNames2,
                fieldDataTypes2,
                lookupKeys,
                rowType,
                resolvedPredicates,
                pushdownParams);
    }

    private static final class ListOutputCollector implements Collector<RowData> {

        private final List<RowData> output = new ArrayList<>();

        @Override
        public void collect(RowData row) {
            this.output.add(row);
        }

        @Override
        public void close() {}

        public List<RowData> getOutputs() {
            return output;
        }
    }
}
