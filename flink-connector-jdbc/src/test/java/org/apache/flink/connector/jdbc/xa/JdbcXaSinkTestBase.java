/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import javax.transaction.xa.Xid;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcITCase.TEST_ENTRY_JDBC_STATEMENT_BUILDER;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;

/**
 * // todo: javadoc case Base class for {@link JdbcXaSinkFunction} tests. In addition to {@link
 * JdbcTestBase} init it initializes/closes helpers.
 */
@Deprecated
abstract class JdbcXaSinkTestBase extends JdbcTestBase {

    JdbcXaFacadeTestHelper xaHelper;
    JdbcXaSinkTestHelper sinkHelper;

    @BeforeEach
    void initHelpers() throws Exception {
        xaHelper = new JdbcXaFacadeTestHelper(getMetadata(), INPUT_TABLE);
        sinkHelper = buildSinkHelper(createStateHandler());
    }

    private XaSinkStateHandler createStateHandler() {
        return new TestXaSinkStateHandler();
    }

    @AfterEach
    void closeHelpers() throws Exception {
        if (sinkHelper != null) {
            sinkHelper.close();
        }
        if (xaHelper != null) {
            xaHelper.close();
        }
        try (JdbcXaFacadeTestHelper xa = new JdbcXaFacadeTestHelper(getMetadata(), INPUT_TABLE)) {
            xa.cancelAllTx();
        }
    }

    JdbcXaSinkTestHelper buildSinkHelper(XaSinkStateHandler stateHandler) throws Exception {
        return new JdbcXaSinkTestHelper(buildAndInit(0, getXaFacade(), stateHandler), stateHandler);
    }

    private XaFacadeImpl getXaFacade() {
        return XaFacadeImpl.fromXaDataSource(getMetadata().buildXaDataSource());
    }

    JdbcXaSinkFunction<TestEntry> buildAndInit() throws Exception {
        return buildAndInit(Integer.MAX_VALUE, getXaFacade());
    }

    JdbcXaSinkFunction<TestEntry> buildAndInit(int batchInterval, XaFacade xaFacade)
            throws Exception {
        return buildAndInit(batchInterval, xaFacade, createStateHandler());
    }

    static JdbcXaSinkFunction<TestEntry> buildAndInit(
            int batchInterval, XaFacade xaFacade, XaSinkStateHandler state) throws Exception {
        JdbcXaSinkFunction<TestEntry> sink =
                buildSink(new SemanticXidGenerator(), xaFacade, state, batchInterval);
        sink.initializeState(buildInitCtx(false));
        sink.setInputType(TypeInformation.of(TestEntry.class), getExecutionConfig(false));
        sink.open(new Configuration());
        return sink;
    }

    static JdbcXaSinkFunction<TestEntry> buildSink(
            XidGenerator xidGenerator,
            XaFacade xaFacade,
            XaSinkStateHandler state,
            int batchInterval) {
        JdbcOutputFormat<TestEntry, TestEntry, JdbcBatchStatementExecutor<TestEntry>> format =
                new JdbcOutputFormat<>(
                        xaFacade,
                        JdbcExecutionOptions.builder()
                                .withBatchIntervalMs(batchInterval)
                                .withMaxRetries(0)
                                .build(),
                        () ->
                                JdbcBatchStatementExecutor.simple(
                                        String.format(INSERT_TEMPLATE, INPUT_TABLE),
                                        TEST_ENTRY_JDBC_STATEMENT_BUILDER));
        JdbcXaSinkFunction<TestEntry> sink =
                new JdbcXaSinkFunction<>(
                        format,
                        xaFacade,
                        xidGenerator,
                        state,
                        JdbcExactlyOnceOptions.builder().withRecoveredAndRollback(true).build(),
                        new XaGroupOpsImpl(xaFacade));
        sink.setRuntimeContext(TEST_RUNTIME_CONTEXT);
        return sink;
    }

    static final RuntimeContext TEST_RUNTIME_CONTEXT = getRuntimeContext(new JobID());

    static final SinkFunction.Context TEST_SINK_CONTEXT =
            new SinkFunction.Context() {
                @Override
                public long currentProcessingTime() {
                    return 0;
                }

                @Override
                public long currentWatermark() {
                    return 0;
                }

                @Override
                public Long timestamp() {
                    return 0L;
                }
            };

    static class TestXaSinkStateHandler implements XaSinkStateHandler {
        private static final long serialVersionUID = 1L;

        private JdbcXaSinkFunctionState stored;

        @Override
        public JdbcXaSinkFunctionState load(FunctionInitializationContext context) {
            List<CheckpointAndXid> prepared =
                    stored != null
                            ? stored.getPrepared().stream()
                                    .map(CheckpointAndXid::asRestored)
                                    .collect(Collectors.toList())
                            : Collections.emptyList();
            Collection<Xid> hanging =
                    stored != null ? stored.getHanging() : Collections.emptyList();
            return JdbcXaSinkFunctionState.of(prepared, hanging);
        }

        @Override
        public void store(JdbcXaSinkFunctionState state) {
            stored = state;
        }

        JdbcXaSinkFunctionState get() {
            return stored;
        }

        @Override
        public String toString() {
            return stored == null ? null : stored.toString();
        }
    }

    static StateInitializationContextImpl buildInitCtx(boolean restored) {
        return new StateInitializationContextImpl(
                restored ? 1L : null,
                new DefaultOperatorStateBackend(
                        new ExecutionConfig(),
                        new CloseableRegistry(),
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        null),
                null,
                null,
                null);
    }
}
