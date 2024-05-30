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

package org.apache.flink.connector.jdbc.backward.compatibility;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcTestFixture;
import org.apache.flink.connector.jdbc.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.source.JdbcSource;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for backward compatibility. */
public class DataStreamSourceTest implements PostgresTestBase {
    public static Queue<JdbcTestFixture.TestEntry> collectedRecords;
    private static final BooksTable TEST_TABLE = new BooksTable("SourceTable");

    protected final ResultExtractor<JdbcTestFixture.TestEntry> extractor =
            resultSet ->
                    new JdbcTestFixture.TestEntry(
                            resultSet.getInt("id"),
                            resultSet.getString("title"),
                            resultSet.getString("author"),
                            // Avoid the 'null -> 0.0d' bug on calling 'getDouble'
                            (Double) resultSet.getObject("price"),
                            resultSet.getInt("qty"));

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TEST_TABLE);
    }

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @BeforeEach
    void init() throws SQLException {
        collectedRecords = new ConcurrentLinkedDeque<>();
        try (Connection conn = getMetadata().getConnection();
                PreparedStatement ps = conn.prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
            for (int i = 0; i < TEST_DATA.length; i++) {
                ps.setInt(1, TEST_DATA[i].id);
                ps.setString(2, TEST_DATA[i].title);
                ps.setString(3, TEST_DATA[i].author);
                if (TEST_DATA[i].price == null) {
                    ps.setNull(4, Types.DOUBLE);
                } else {
                    ps.setDouble(4, TEST_DATA[i].price);
                }
                ps.setInt(5, TEST_DATA[i].qty);
                ps.execute();
            }
        }
    }

    @Test
    void testReadWithoutParallelismWithoutParamsProvider() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.setParallelism(1);
        JdbcSource<JdbcTestFixture.TestEntry> jdbcSource =
                JdbcSource.<JdbcTestFixture.TestEntry>builder()
                        .setTypeInformation(TypeInformation.of(JdbcTestFixture.TestEntry.class))
                        .setSql(TEST_TABLE.getSelectAllQuery())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setUsername(getMetadata().getUsername())
                        .setPassword(getMetadata().getPassword())
                        .setDriverName(getMetadata().getDriverClass())
                        .setResultExtractor(extractor)
                        .build();
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "TestSource")
                .addSink(new TestingSinkFunction());
        env.execute();
        assertThat(collectedRecords).containsExactlyInAnyOrder(TEST_DATA);
    }

    @Test
    void testReadWithoutParallelismWithParamsProvider() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.setParallelism(1);
        JdbcSource<JdbcTestFixture.TestEntry> jdbcSource =
                JdbcSource.<JdbcTestFixture.TestEntry>builder()
                        .setTypeInformation(TypeInformation.of(JdbcTestFixture.TestEntry.class))
                        .setSql(TEST_TABLE.getSelectByIdBetweenQuery())
                        .setJdbcParameterValuesProvider(
                                new JdbcGenericParameterValuesProvider(
                                        new Serializable[][] {{1001, 1005}, {1006, 1010}}))
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setUsername(getMetadata().getUsername())
                        .setPassword(getMetadata().getPassword())
                        .setDriverName(getMetadata().getDriverClass())
                        .setResultExtractor(extractor)
                        .build();
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "TestSource")
                .addSink(new TestingSinkFunction());
        env.execute();
        assertThat(collectedRecords).containsExactlyInAnyOrder(TEST_DATA);
    }

    @Test
    void testReadWithParallelismWithoutParamsProvider() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.setParallelism(2);
        JdbcSource<JdbcTestFixture.TestEntry> jdbcSource =
                JdbcSource.<JdbcTestFixture.TestEntry>builder()
                        .setTypeInformation(TypeInformation.of(JdbcTestFixture.TestEntry.class))
                        .setSql(TEST_TABLE.getSelectAllQuery())
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setUsername(getMetadata().getUsername())
                        .setPassword(getMetadata().getPassword())
                        .setDriverName(getMetadata().getDriverClass())
                        .setResultExtractor(extractor)
                        .build();
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "TestSource")
                .addSink(new TestingSinkFunction());
        env.execute();
        assertThat(collectedRecords).containsExactlyInAnyOrder(TEST_DATA);
    }

    @Test
    void testReadWithParallelismWithParamsProvider() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.setParallelism(2);
        JdbcSource<JdbcTestFixture.TestEntry> jdbcSource =
                JdbcSource.<JdbcTestFixture.TestEntry>builder()
                        .setTypeInformation(TypeInformation.of(JdbcTestFixture.TestEntry.class))
                        .setSql(TEST_TABLE.getSelectByIdBetweenQuery())
                        .setJdbcParameterValuesProvider(
                                new JdbcGenericParameterValuesProvider(
                                        new Serializable[][] {{1001, 1005}, {1006, 1010}}))
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setUsername(getMetadata().getUsername())
                        .setPassword(getMetadata().getPassword())
                        .setDriverName(getMetadata().getDriverClass())
                        .setResultExtractor(extractor)
                        .build();
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "TestSource")
                .addSink(new TestingSinkFunction());
        env.execute();
        assertThat(collectedRecords).containsExactlyInAnyOrder(TEST_DATA);
    }

    /** A sink function to collect the records. */
    static class TestingSinkFunction implements SinkFunction<JdbcTestFixture.TestEntry> {

        @Override
        public void invoke(JdbcTestFixture.TestEntry value, Context context) throws Exception {
            collectedRecords.add(value);
        }
    }
}
