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

import org.apache.flink.FlinkVersion;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.JdbcTestFixture;
import org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import org.apache.flink.connector.jdbc.testutils.databases.derby.DerbyDatabase;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.transaction.xa.Xid;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Optional.of;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.CP0;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.cleanUpDatabasesStatic;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.initSchema;
import static org.apache.flink.connector.jdbc.xa.JdbcXaSinkTestBase.buildInitCtx;
import static org.apache.flink.streaming.util.OperatorSnapshotUtil.readStateHandle;
import static org.apache.flink.streaming.util.OperatorSnapshotUtil.writeStateHandle;

/** Tests state migration for {@link JdbcXaSinkFunction}. */
public class JdbcXaSinkMigrationTest extends JdbcTestBase {

    // write a snapshot:
    // java <CLASS_NAME> <VERSION>
    // or
    // mvn exec:java -Dexec.mainClass="<CLASS_NAME>" -Dexec.args='<VERSION>'
    // -Dexec.classpathScope=test -Dexec.cleanupDaemonThreads=false
    public static void main(String[] args) throws Exception {
        new DerbyDatabase().startDatabase();
        JdbcXaSinkMigrationTest test = new JdbcXaSinkMigrationTest();
        test.writeSnapshot(parseVersionArg(args));
    }

    public static Collection<FlinkVersion> getReadVersions() {
        return Collections.emptyList();
    }

    @ParameterizedTest
    @MethodSource("getReadVersions")
    @Disabled // as getReadVersions is empty and fails
    void testCommitFromSnapshot(FlinkVersion readVersion) throws Exception {
        preparePendingTransaction();
        try (OneInputStreamOperatorTestHarness<TestEntry, Object> harness =
                createHarness(buildSink())) {
            harness.initializeState(readStateHandle(getSnapshotPath(readVersion)));
            harness.open();
        }
        try (JdbcXaFacadeTestHelper h =
                new JdbcXaFacadeTestHelper(getMetadata(), JdbcTestFixture.INPUT_TABLE)) {
            h.assertDbContentsEquals(CP0);
        }
    }

    @AfterEach
    void cleanUp() throws Exception {
        cancelAllTx();
    }

    private void preparePendingTransaction() throws Exception {
        try (JdbcXaSinkTestHelper sinkHelper =
                new JdbcXaSinkTestHelper(buildSink(), new XaSinkStateHandlerImpl())) {
            sinkHelper.getSinkFunction().initializeState(buildInitCtx(false));
            sinkHelper.getSinkFunction().open(new Configuration());
            sinkHelper.emitAndSnapshot(CP0);
        }
    }

    private OperatorSubtaskState captureState() throws Exception {
        try (JdbcXaSinkTestHelper sinkHelper =
                new JdbcXaSinkTestHelper(buildSink(), new XaSinkStateHandlerImpl())) {
            try (OneInputStreamOperatorTestHarness<TestEntry, Object> harness =
                    createHarness(sinkHelper.getSinkFunction())) {
                harness.initializeEmptyState();
                harness.open();
                sinkHelper.emit(CP0);
                return harness.snapshot(0L, 0L);
            }
        }
    }

    private static XidGenerator getXidGenerator() {
        final AtomicInteger txCounter = new AtomicInteger();
        return new XidGenerator() {
            @Override
            public Xid generateXid(JobSubtask subtask, long checkpointId) {
                return new TestXid(txCounter.incrementAndGet(), 0, 0);
            }

            @Override
            public boolean belongsToSubtask(Xid xid, JobSubtask subtask) {
                return false;
            }
        };
    }

    private String getSnapshotPath(FlinkVersion version) {
        return String.format(
                "src/test/resources/jdbc-exactly-once-sink-migration-%s-snapshot", version);
    }

    private static OneInputStreamOperatorTestHarness<TestEntry, Object> createHarness(
            JdbcXaSinkFunction<TestEntry> sink) throws Exception {
        OneInputStreamOperatorTestHarness<TestEntry, Object> harness =
                new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink));
        harness.setup();
        return harness;
    }

    private static FlinkVersion parseVersionArg(String[] args) {
        return (args == null || args.length == 0 ? Optional.<String>empty() : of(args[0]))
                .flatMap(FlinkVersion::byCode)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Please specify a version as a 1st parameter. Valid values are: "
                                                + Arrays.toString(FlinkVersion.values())));
    }

    private JdbcXaSinkFunction<TestEntry> buildSink() {
        return JdbcXaSinkTestBase.buildSink(
                getXidGenerator(),
                XaFacadeImpl.fromXaDataSource(getMetadata().buildXaDataSource()),
                new XaSinkStateHandlerImpl(new XaSinkStateSerializer()),
                1);
    }

    private void cancelAllTx() throws Exception {
        try (JdbcXaFacadeTestHelper xa =
                new JdbcXaFacadeTestHelper(getMetadata(), JdbcTestFixture.INPUT_TABLE)) {
            xa.cancelAllTx();
        }
    }

    private void writeSnapshot(FlinkVersion flinkVersion) throws Exception {
        String path = getSnapshotPath(flinkVersion);

        //        Files.createFile(Paths.get(path));/
        Preconditions.checkArgument(
                !Files.exists(Paths.get(path)),
                String.format("snapshot for version %s already exist: %s", flinkVersion, path));
        initSchema(getMetadata());
        try {
            writeStateHandle(captureState(), path);
        } finally {
            cancelAllTx();
            cleanUpDatabasesStatic(getMetadata());
        }
    }

    private static class TestXid implements Xid {
        private final int gtrid;
        private final int bqual;
        private final int format;

        private TestXid(int gtrid, int bqual, int format) {
            this.gtrid = gtrid;
            this.bqual = bqual;
            this.format = format;
        }

        @Override
        public int getFormatId() {
            return format;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return String.valueOf(gtrid).getBytes();
        }

        @Override
        public byte[] getBranchQualifier() {
            return String.valueOf(gtrid).getBytes();
        }
    }
}
