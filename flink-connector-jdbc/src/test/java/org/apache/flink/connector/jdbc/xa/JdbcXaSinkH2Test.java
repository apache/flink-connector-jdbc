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

import org.apache.flink.connector.jdbc.JdbcTestFixture;
import org.apache.flink.connector.jdbc.databases.h2.H2XaTestBase;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.databases.h2.H2XaDatabase;
import org.apache.flink.connector.jdbc.testutils.databases.h2.xa.H2XaDsWrapper;

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link JdbcXaSinkFunction} tests using H2 DB. H2 uses MVCC (so we can e.g. count records while
 * transaction is not yet committed). But XA support isn't full, so for some scenarios {@link
 * H2XaDsWrapper wrapper} is used, and for some - Derby.
 */
class JdbcXaSinkH2Test extends JdbcXaSinkTestBase implements H2XaTestBase {

    @Override
    public DatabaseMetadata getMetadata() {
        return H2XaDatabase.getMetadata();
    }

    @Test
    void testIgnoreDuplicatedNotification() throws Exception {
        sinkHelper.emitAndCheckpoint(JdbcTestFixture.CP0);
        sinkHelper.notifyCheckpointComplete(JdbcTestFixture.CP0.id);
    }

    /** RM may return {@link javax.transaction.xa.XAResource#XA_RDONLY XA_RDONLY} error. */
    @Test
    void testEmptyCheckpoint() throws Exception {
        sinkHelper.snapshotState(0);
    }

    @Test
    void testHappyFlow() throws Exception {
        sinkHelper.emit(TEST_DATA[0]);
        assertThat(xaHelper.countInDb())
                .as("record should not be inserted before the checkpoint started.")
                .isEqualTo(0);

        sinkHelper.snapshotState(Long.MAX_VALUE);
        assertThat(xaHelper.countInDb())
                .as("record should not be inserted before the checkpoint completed.")
                .isEqualTo(0);

        sinkHelper.notifyCheckpointComplete(Long.MAX_VALUE);
        assertThat(xaHelper.countInDb())
                .as("record should be inserted after the checkpoint completed.")
                .isEqualTo(1);
    }

    @Test
    void testTwoCheckpointsWithoutData() throws Exception {
        JdbcXaSinkTestHelper sinkHelper = this.sinkHelper;
        sinkHelper.snapshotState(1);
        sinkHelper.snapshotState(2);
        assertThat(xaHelper.countInDb()).isEqualTo(0);
    }
}
