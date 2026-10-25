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

package org.apache.flink.connector.jdbc.core.datastream.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.xa.SimpleXaConnectionProvider;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runs the exactly-once sink tests with a transaction id prefix. */
class ExactlyOnceTransactionIdPrefixJdbcSinkTest extends ExactlyOnceJdbcSinkTest {

    @Override
    protected <T> JdbcSink<T> finishSink(JdbcSinkBuilder<T> builder) {
        return builder.withExecutionOptions(
                        JdbcExecutionOptions.builder().withMaxRetries(0).build())
                .buildExactlyOnce(
                        JdbcExactlyOnceOptions.builder().withTransactionIdPrefix("books").build(),
                        getMetadata().getXaSourceSupplier());
    }

    /**
     * A job fails after two expired checkpoints, with transactions prepared for both. A new job
     * restores the last completed checkpoint at a lower parallelism and must roll them back, also
     * the one its restored state doesn't name.
     */
    @Test
    void testRestoreIntoNewJobAfterAFailure(@TempDir Path checkpoints) throws Exception {
        SharedReference<AtomicInteger> failed = sharedObjects.add(new AtomicInteger());
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpoints.toUri().toString());
        config.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(2);
        env.enableCheckpointing(200L);
        env.getCheckpointConfig().setCheckpointTimeout(300L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);
        addRecoveryPipeline(env, failed);
        assertThatThrownBy(env::execute).isInstanceOf(JobExecutionException.class);
        assertThat(failed.get().get()).isEqualTo(4);
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            assertThat(xa.recover()).isNotEmpty();
        }

        config = new Configuration();
        config.set(StateRecoveryOptions.SAVEPOINT_PATH, latestCheckpoint(checkpoints));
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        addRecoveryPipeline(env, failed);
        env.execute();

        assertWrittenOnce(RECOVERY_RECORDS);
    }

    private void addRecoveryPipeline(
            StreamExecutionEnvironment env, SharedReference<AtomicInteger> failed) {
        DataGeneratorSource<BooksTable.BookEntry> books =
                new DataGeneratorSource<>(
                        ExactlyOnceJdbcSinkTest::book,
                        RECOVERY_RECORDS,
                        // records keep coming while checkpoints expire
                        RateLimiterStrategy.perSecond(40),
                        TypeInformation.of(BooksTable.BookEntry.class));
        env.fromSource(books, WatermarkStrategy.noWatermarks(), "books")
                .uid("books")
                // both subtasks let checkpoints 2 and 3 expire
                .map(new FailingSnapshotMapper(failed, 2, 4, true))
                .uid("mapper")
                .sinkTo(recoverySink())
                .uid("sink");
    }

    private static String latestCheckpoint(Path checkpoints) throws IOException {
        try (Stream<Path> files = Files.walk(checkpoints)) {
            return files.filter(path -> path.getFileName().toString().equals("_metadata"))
                    .map(Path::getParent)
                    .max(
                            Comparator.comparingInt(
                                    path ->
                                            Integer.parseInt(
                                                    path.getFileName()
                                                            .toString()
                                                            .substring("chk-".length()))))
                    .orElseThrow(() -> new AssertionError("no completed checkpoint"))
                    .toUri()
                    .toString();
        }
    }
}
