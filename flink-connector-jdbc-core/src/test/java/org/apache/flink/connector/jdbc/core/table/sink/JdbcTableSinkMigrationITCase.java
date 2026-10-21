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

package org.apache.flink.connector.jdbc.core.table.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.jdbc.derby.DerbyTestBase;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The 4.x Table sink was a {@code SinkFunction}, this one is a Sink V2. A job that stopped with a
 * savepoint on the old sink restores it on the new one when its operators carry uids, the way
 * {@code FileSinkMigrationITCase} in Flink covers {@code StreamingFileSink} and {@code FileSink}.
 * The nested copy of the 4.x sink function ties this test to Flink's legacy sink package; a
 * committed savepoint fixture replaces it once that package goes.
 */
class JdbcTableSinkMigrationITCase implements DerbyTestBase, JdbcITCaseBase {

    private static final String SOURCE_UID = "source";
    private static final String SINK_UID = "sink";

    private final TableRow outputTable =
            tableRow("migrationTable", field("id", DataTypes.BIGINT().notNull()));

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(outputTable);
    }

    @Test
    @SuppressWarnings("deprecation")
    void testRestoreFromTheSinkFunctionSavepoint(@TempDir Path savepointDir) throws Exception {
        JdbcDynamicTableSink tableSink =
                (JdbcDynamicTableSink)
                        createTableSink(outputTable.getTableResolvedSchema(), options());

        StreamExecutionEnvironment legacy = environment(null);
        ids(legacy)
                .addSink(new LegacyJdbcSinkFunction(tableSink.outputFormatBuilder().build()))
                .uid(SINK_UID);
        JobClient legacyJob = legacy.executeAsync("legacy sink");
        waitForRows(1);
        String savepoint =
                legacyJob
                        .stopWithSavepoint(
                                false, savepointDir.toString(), SavepointFormatType.CANONICAL)
                        .get();
        int rowsAtSavepoint = countRows();

        StreamExecutionEnvironment restored = environment(savepoint);
        Sink<RowData> sink =
                ((SinkV2Provider)
                                tableSink.getSinkRuntimeProvider(
                                        new SinkRuntimeProviderContext(false)))
                        .createSink();
        ids(restored).sinkTo(sink).uid(SINK_UID);
        JobClient restoredJob = restored.executeAsync("sink v2");
        waitForRows(rowsAtSavepoint + 1);
        restoredJob.cancel().get();

        List<Long> ids =
                outputTable.selectAllTable(getMetadata()).stream()
                        .map(row -> (Long) row.getField(0))
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(ids)
                .hasSizeGreaterThan(rowsAtSavepoint)
                .containsExactlyElementsOf(
                        LongStream.range(0, ids.size()).boxed().collect(Collectors.toList()));
    }

    private Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "jdbc");
        options.put("url", getMetadata().getJdbcUrl());
        options.put("username", getMetadata().getUsername());
        options.put("password", getMetadata().getPassword());
        options.put("table-name", outputTable.getTableName());
        options.put("sink.buffer-flush.interval", "0");
        return options;
    }

    private static StreamExecutionEnvironment environment(@Nullable String savepoint) {
        Configuration config = new Configuration();
        if (savepoint != null) {
            // no allowNonRestoredState: a state the new graph cannot place fails the restore
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        return env;
    }

    private DataStream<RowData> ids(StreamExecutionEnvironment env) {
        RowType rowType =
                (RowType)
                        outputTable
                                .getTableResolvedSchema()
                                .toPhysicalRowDataType()
                                .getLogicalType();
        DataGeneratorSource<RowData> source =
                new DataGeneratorSource<>(
                        id -> GenericRowData.of(id),
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perCheckpoint(10),
                        InternalTypeInfo.of(rowType));
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "ids").uid(SOURCE_UID);
    }

    private int countRows() throws SQLException {
        return outputTable.selectAllTable(getMetadata()).size();
    }

    private void waitForRows(int atLeast) throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
        while (countRows() < atLeast) {
            assertThat(deadline.hasTimeLeft())
                    .as("%s rows in %s", atLeast, outputTable.getTableName())
                    .isTrue();
            Thread.sleep(100);
        }
    }

    /** The 4.x {@code GenericJdbcSinkFunction}, kept to take the savepoint this test restores. */
    @SuppressWarnings("deprecation")
    private static class LegacyJdbcSinkFunction extends RichSinkFunction<RowData>
            implements CheckpointedFunction, InputTypeConfigurable {

        private final JdbcOutputFormat<RowData, ?, ?> outputFormat;
        private JdbcOutputSerializer<RowData> serializer;

        LegacyJdbcSinkFunction(JdbcOutputFormat<RowData, ?, ?> outputFormat) {
            this.outputFormat = outputFormat;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            serializer.withObjectReuseEnabled(getRuntimeContext().isObjectReuseEnabled());
            outputFormat.open(serializer);
        }

        @Override
        public void invoke(RowData value, Context context) throws IOException {
            outputFormat.writeRecord(value);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            outputFormat.flush();
        }

        @Override
        public void close() {
            outputFormat.close();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
            this.serializer =
                    JdbcOutputSerializer.of(
                            ((TypeInformation<RowData>) type)
                                    .createSerializer(executionConfig.getSerializerConfig()));
        }
    }
}
