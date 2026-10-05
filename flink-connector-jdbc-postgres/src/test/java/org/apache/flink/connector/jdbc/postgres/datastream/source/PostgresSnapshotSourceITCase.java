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

package org.apache.flink.connector.jdbc.postgres.datastream.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionOptions;
import org.apache.flink.connector.jdbc.core.datastream.source.JdbcSource;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.TableSplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.core.datastream.source.reader.extractor.RowResultExtractor;
import org.apache.flink.connector.jdbc.postgres.PostgresTestBase;
import org.apache.flink.connector.jdbc.postgres.datastream.PostgresJdbc;
import org.apache.flink.connector.jdbc.postgres.datastream.PostgresJdbcConsumer;
import org.apache.flink.connector.jdbc.postgres.datastream.connection.PostgresConnectionOptions;
import org.apache.flink.connector.jdbc.postgres.datastream.connection.PostgresConnectionProvider;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for the snapshot splitter reading through a real {@link JdbcSource}: the split
 * enumerator exports a snapshot on the JobManager, stamps its id onto every split, and the reader
 * connections join it. A row committed concurrently <em>while the job is reading</em> must stay
 * invisible to the reader even though it is committed before the reader issues its final split
 * query. A second test covers at-least-once delivery across a mid-enumeration task failover.
 */
class PostgresSnapshotSourceITCase implements PostgresTestBase, JdbcITCaseBase {

    private static final String TABLE = "snapshot_e2e";
    private static final int SENTINEL_ID = 100;
    private static final int POISON_ID = 8;
    private static final int ROW_COUNT = 10;

    private static final java.util.regex.Pattern URL_PARTS =
            java.util.regex.Pattern.compile("jdbc:postgresql://([^:/]+):(\\d+)/([^?]+)");

    private static final Queue<Integer> COLLECTED_IDS = new ConcurrentLinkedQueue<>();

    /**
     * Deliberately static and transient-by-absence from the extractor: a credential-bearing URL
     * must not become a serialized field of a user function (it would travel inside the job graph
     * and checkpoints, readable from the Web UI and state stores).
     */
    private static String jdbcUrlWithCredentials;

    private static final AtomicBoolean SENTINEL_INSERTED = new AtomicBoolean();
    private static final AtomicBoolean POISON_THROWN = new AtomicBoolean();

    @BeforeEach
    void setUpTable() throws Exception {
        COLLECTED_IDS.clear();
        SENTINEL_INSERTED.set(false);
        POISON_THROWN.set(false);
        jdbcUrlWithCredentials = getMetadata().getJdbcUrlWithCredentials();

        StringBuilder inserts = new StringBuilder("INSERT INTO " + TABLE + " (id, val) VALUES ");
        for (int i = 1; i <= ROW_COUNT; i++) {
            inserts.append("(")
                    .append(i)
                    .append(", 'v")
                    .append(i)
                    .append("')")
                    .append(i == ROW_COUNT ? "" : ",");
        }
        execute(
                "DROP TABLE IF EXISTS " + TABLE,
                "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, val TEXT)",
                inserts.toString());
    }

    @Test
    void readerDoesNotSeeRowsCommittedDuringTheSnapshotRead() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);

        // Built through the PostgresJdbc facade so the convenience entry point stays exercised.
        java.util.regex.Matcher urlParts = URL_PARTS.matcher(getMetadata().getJdbcUrl());
        assertThat(urlParts.find()).isTrue();
        JdbcSource<Row> source =
                PostgresJdbc.of(
                                PostgresConnectionOptions.create()
                                        .withHost(urlParts.group(1))
                                        .withPort(Integer.parseInt(urlParts.group(2)))
                                        .withDatabase(urlParts.group(3))
                                        .withUsername(getMetadata().getUsername())
                                        .withPassword(getMetadata().getPassword()))
                        .asConsumer()
                        .withSplitterEnumerator(
                                TableSplitterEnumerator.builder()
                                        .withCatalogName("")
                                        .withSchemaName("public")
                                        .withTableName(TABLE)
                                        .withColumnNames("id", "val")
                                        // Small chunks so the job reads through several split
                                        // queries: the sentinel is committed after the first
                                        // query and must stay invisible to all later ones.
                                        .withChunkSize(2)
                                        .build())
                        .build(new SentinelInsertingExtractor());

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "SnapshotSource")
                .addSink(new CollectSinkFunction());
        env.execute();

        // The concurrent insert really happened while the job was running (it is triggered by the
        // first record the reader extracts).
        assertThat(SENTINEL_INSERTED)
                .as("sentinel insert should have been triggered during the read")
                .isTrue();

        assertThat(COLLECTED_IDS)
                .as("reader must observe exactly the snapshot taken before the sentinel commit")
                .containsExactlyInAnyOrder(
                        IntStream.rangeClosed(1, ROW_COUNT).boxed().toArray(Integer[]::new));

        // Sanity check: the sentinel is committed and visible to a session that does not join the
        // snapshot — otherwise the assertion above would pass trivially.
        try (Connection connection =
                        DriverManager.getConnection(getMetadata().getJdbcUrlWithCredentials());
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM public." + TABLE)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1))
                    .as("sentinel must be committed and visible outside the snapshot")
                    .isEqualTo(ROW_COUNT + 1);
        }
    }

    @Test
    void failoverDuringEnumerationCompletesWithAllRows() throws Exception {
        // At-least-once round-trip regression for the checkpoint/restore path: attempt 1 exports
        // the shared snapshot and checkpoints capture the splitter progress plus the not-yet-read
        // splits; the task then fails mid-enumeration and the job restarts from the checkpoint.
        // Restored splits (including their checkpointed snapshot ids, re-stamped by the enumerator
        // when its own export is newer) must still be readable, and every row must eventually
        // arrive. The deterministic B1 re-stamp case — a restored split meeting a snapshot
        // exported by a different run — is pinned by
        // JdbcSourceEnumeratorTest#testStaleSnapshotIdIsRefreshedAtAssignment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 10, 200L);
        env.enableCheckpointing(100);
        env.setParallelism(1);

        JdbcSource<Row> source =
                JdbcSource.<Row>builder()
                        .setConnectionProvider(new PostgresConnectionProvider(connectionOptions()))
                        .setSplitter(
                                TableSplitterEnumerator.builder()
                                        .withCatalogName("")
                                        .withSchemaName("public")
                                        .withTableName(TABLE)
                                        .withColumnNames("id", "val")
                                        // One PK value per chunk: many splits, so several
                                        // checkpoints capture an unassigned backlog carrying the
                                        // first attempt's snapshot id before the poison fires.
                                        .withChunkSize(1)
                                        .build())
                        // At-least-once keeps restored unprocessed splits (re-stamped) and re-reads
                        // in-flight ones, so every row must eventually arrive.
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setResultExtractor(new PoisonPillExtractor())
                        .setTypeInformation(TypeInformation.of(Row.class))
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "SnapshotRestoreSource")
                .addSink(new CollectSinkFunction());
        env.execute();

        assertThat(POISON_THROWN)
                .as("the injected failure must have fired to exercise the restore path")
                .isTrue();
        assertThat(new HashSet<>(COLLECTED_IDS))
                .as("all rows of the table's snapshot must arrive after the failover")
                .contains(IntStream.rangeClosed(1, ROW_COUNT).boxed().toArray(Integer[]::new));
    }

    private ConnectionOptions connectionOptions() {
        return ConnectionOptions.builder()
                .withUrl(getMetadata().getJdbcUrl())
                .withDriverName(getMetadata().getDriverClass())
                .withUsername(getMetadata().getUsername())
                .withPassword(getMetadata().getPassword())
                .build();
    }

    /**
     * Extracts rows and, when the reader extracts its very first record, commits a sentinel row
     * from a separate session. That moment is strictly between the reader's first and last split
     * query, so a reader not honoring the shared snapshot would observe the sentinel in one of the
     * later chunks.
     */
    private static class SentinelInsertingExtractor
            implements PostgresJdbcConsumer.JdbcExtractor<Row> {

        private static final long serialVersionUID = 1L;

        private final RowResultExtractor delegate = new RowResultExtractor();

        @Override
        public TypeInformation<Row> typeInformation() {
            return TypeInformation.of(Row.class);
        }

        @Override
        public Row extract(ResultSet resultSet) throws SQLException {
            if (SENTINEL_INSERTED.compareAndSet(false, true)) {
                try (Connection connection = DriverManager.getConnection(jdbcUrlWithCredentials);
                        Statement statement = connection.createStatement()) {
                    statement.executeUpdate(
                            "INSERT INTO public."
                                    + TABLE
                                    + " (id, val) VALUES ("
                                    + SENTINEL_ID
                                    + ", 'sentinel')");
                }
            }
            return delegate.extract(resultSet);
        }
    }

    /**
     * Reads each row slowly (so checkpoints complete mid-enumeration) and throws exactly once when
     * extracting the poison row, deterministically failing the job so at-least-once delivery must
     * re-read the in-flight and unassigned splits after the restart.
     */
    private static class PoisonPillExtractor implements ResultExtractor<Row> {

        private static final long serialVersionUID = 1L;

        private final RowResultExtractor delegate = new RowResultExtractor();

        @Override
        public Row extract(ResultSet resultSet) throws SQLException {
            Row row = delegate.extract(resultSet);
            try {
                Thread.sleep(60);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException(e);
            }
            if (Integer.valueOf(POISON_ID).equals(row.getField(0))
                    && POISON_THROWN.compareAndSet(false, true)) {
                throw new RuntimeException("injected mid-run failure (test)");
            }
            return row;
        }
    }

    /** Collects the first (id) field of every row into {@link #COLLECTED_IDS}. */
    private static class CollectSinkFunction implements SinkFunction<Row> {

        @Override
        public void invoke(Row value, Context context) {
            COLLECTED_IDS.add((Integer) value.getField(0));
        }
    }

    private void execute(String... statements) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(getMetadata().getJdbcUrlWithCredentials());
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
