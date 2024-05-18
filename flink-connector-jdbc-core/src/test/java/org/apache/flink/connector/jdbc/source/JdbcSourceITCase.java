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

package org.apache.flink.connector.jdbc.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.assertj.core.api.Assertions.assertThat;

/** The Integration test for {@link JdbcSource}. */
class JdbcSourceITCase extends JdbcDataTestBase implements JdbcITCaseBase {

    public static Queue<TestEntry> collectedRecords;
    private final String sql = "select id, title, author, price, qty from " + INPUT_TABLE;

    @BeforeEach
    void init() {
        collectedRecords = new ConcurrentLinkedDeque<>();
    }

    @Test
    void testReadWithoutParallelismWithoutParamsProvider() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.setParallelism(1);
        JdbcSource<TestEntry> jdbcSource =
                JdbcSource.<TestEntry>builder()
                        .setTypeInformation(TypeInformation.of(TestEntry.class))
                        .setSql(sql)
                        .setDBUrl(getMetadata().getJdbcUrl())
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
        JdbcSource<TestEntry> jdbcSource =
                JdbcSource.<TestEntry>builder()
                        .setTypeInformation(TypeInformation.of(TestEntry.class))
                        .setSql(sql + " where id >= ? and id <= ?")
                        .setJdbcParameterValuesProvider(
                                new JdbcGenericParameterValuesProvider(
                                        new Serializable[][] {{1001, 1005}, {1006, 1010}}))
                        .setDBUrl(getMetadata().getJdbcUrl())
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
        JdbcSource<TestEntry> jdbcSource =
                JdbcSource.<TestEntry>builder()
                        .setTypeInformation(TypeInformation.of(TestEntry.class))
                        .setSql(sql)
                        .setDBUrl(getMetadata().getJdbcUrl())
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
        JdbcSource<TestEntry> jdbcSource =
                JdbcSource.<TestEntry>builder()
                        .setTypeInformation(TypeInformation.of(TestEntry.class))
                        .setSql(sql + " where id >= ? and id <= ?")
                        .setJdbcParameterValuesProvider(
                                new JdbcGenericParameterValuesProvider(
                                        new Serializable[][] {{1001, 1005}, {1006, 1010}}))
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setDriverName(getMetadata().getDriverClass())
                        .setResultExtractor(extractor)
                        .build();
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "TestSource")
                .addSink(new TestingSinkFunction());
        env.execute();
        assertThat(collectedRecords).containsExactlyInAnyOrder(TEST_DATA);
    }

    /** A sink function to collect the records. */
    static class TestingSinkFunction implements SinkFunction<TestEntry> {

        @Override
        public void invoke(TestEntry value, Context context) throws Exception {
            collectedRecords.add(value);
        }
    }
}
