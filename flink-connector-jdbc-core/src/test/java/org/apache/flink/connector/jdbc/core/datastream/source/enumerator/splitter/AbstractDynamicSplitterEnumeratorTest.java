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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter;

import org.apache.flink.connector.jdbc.core.datastream.source.split.CheckpointedOffset;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class AbstractDynamicSplitterEnumeratorTest {

    private CountingConnectionProvider connectionProvider;

    @BeforeEach
    void setUp() {
        connectionProvider = new CountingConnectionProvider();
    }

    @Test
    void testStartDoesNotOpenConnection() {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());

        enumerator.start(connectionProvider);

        assertThat(connectionProvider.getOrEstablishConnectionCallCount).isZero();
        assertThat(connectionProvider.closeConnectionCallCount).isZero();
    }

    @Test
    void testFirstEnumerateSplitsDiscoversOnce() throws Exception {
        List<JdbcSourceSplit> expected = singletonSplitList();
        TestEnumerator enumerator = new TestEnumerator(expected);
        enumerator.start(connectionProvider);

        List<JdbcSourceSplit> result = enumerator.enumerateSplits();

        assertThat(result).isEqualTo(expected);
        assertThat(enumerator.discoverCallCount).isEqualTo(1);
        assertThat(connectionProvider.getOrEstablishConnectionCallCount).isEqualTo(1);
    }

    @Test
    void testRepeatedEnumerateSplitsOnlyDiscoversOnce() throws Exception {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());
        enumerator.start(connectionProvider);

        enumerator.enumerateSplits();
        List<JdbcSourceSplit> second = enumerator.enumerateSplits();

        assertThat(second).isEmpty();
        assertThat(enumerator.discoverCallCount).isEqualTo(1);
        assertThat(connectionProvider.getOrEstablishConnectionCallCount).isEqualTo(1);
    }

    @Test
    void testIsAllSplitsFinishedTracksState() throws Exception {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());
        enumerator.start(connectionProvider);

        assertThat(enumerator.isAllSplitsFinished()).isFalse();

        // The call that returns the discovered splits deliberately does not flip finished itself
        // (see AbstractDynamicSplitterEnumerator's class javadoc) - it only flips on this second,
        // now-empty call.
        enumerator.enumerateSplits();
        assertThat(enumerator.isAllSplitsFinished()).isFalse();

        enumerator.enumerateSplits();
        assertThat(enumerator.isAllSplitsFinished()).isTrue();
    }

    @Test
    void testRestoreStateSkipsRediscovery() throws Exception {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());

        enumerator.restoreState(true);
        enumerator.start(connectionProvider);
        List<JdbcSourceSplit> result = enumerator.enumerateSplits();

        assertThat(result).isEmpty();
        assertThat(enumerator.discoverCallCount).isZero();
        assertThat(connectionProvider.getOrEstablishConnectionCallCount).isZero();
    }

    @Test
    void testCloseDelegatesToConnectionProvider() {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());
        enumerator.start(connectionProvider);

        enumerator.close();

        assertThat(connectionProvider.closeConnectionCallCount).isEqualTo(1);
    }

    @Test
    void testCloseWithoutStartIsNoOp() {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());

        enumerator.close();

        assertThat(connectionProvider.closeConnectionCallCount).isZero();
    }

    @Test
    void testSerializableStateReflectsFinishedFlag() throws Exception {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());
        enumerator.start(connectionProvider);

        assertThat(enumerator.serializableState()).isEqualTo(Boolean.FALSE);

        enumerator.enumerateSplits();
        assertThat(enumerator.serializableState()).isEqualTo(Boolean.FALSE);

        enumerator.enumerateSplits();
        assertThat(enumerator.serializableState()).isEqualTo(Boolean.TRUE);
    }

    @Test
    void testCheckpointBetweenDiscoveryAndIntegrationNeverLosesSplits() throws Exception {
        // Regression test for the race a reviewer found: a checkpoint landing right after
        // enumerateSplits() returns the discovered splits, but before JdbcSourceEnumerator has
        // integrated them, must not persist finished == true without those splits - otherwise a
        // restore from that checkpoint loses them for good. Simulates that ordering directly:
        // read the discovered splits, snapshot state *before* touching finished again, then
        // simulate the follow-up poll a real JdbcSourceEnumerator issues once idle.
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());
        enumerator.start(connectionProvider);

        List<JdbcSourceSplit> discovered = enumerator.enumerateSplits();
        Object stateRightAfterDiscovery = enumerator.serializableState();

        assertThat(discovered).isNotEmpty();
        assertThat(stateRightAfterDiscovery)
                .as("must not report finished before the caller could integrate the splits")
                .isEqualTo(Boolean.FALSE);
    }

    @Test
    void testEnumeratorSurvivesJavaSerialization() {
        // JdbcSource, which holds a SplitterEnumerator instance as a field, is itself Java
        // Serializable so Flink can ship it as part of the job graph. Object (used for the
        // discovery lock) isn't Serializable, so that field must stay transient and be
        // reconstructed on the other side - otherwise every job using a splitter built on this
        // base class would fail at submission time with a NotSerializableException.
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());

        assertThatCode(
                        () -> {
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                                oos.writeObject(enumerator);
                            }
                            try (ObjectInputStream ois =
                                    new ObjectInputStream(
                                            new ByteArrayInputStream(baos.toByteArray()))) {
                                ois.readObject();
                            }
                        })
                .doesNotThrowAnyException();
    }

    private static List<JdbcSourceSplit> singletonSplitList() {
        return Collections.singletonList(
                new JdbcSourceSplit("0", "SELECT 1", null, new CheckpointedOffset()));
    }

    /** Minimal concrete subclass exercising the abstract discovery hooks. */
    private static class TestEnumerator extends AbstractDynamicSplitterEnumerator {
        private final List<JdbcSourceSplit> splitsToReturn;
        private int discoverCallCount = 0;

        TestEnumerator(List<JdbcSourceSplit> splitsToReturn) {
            this.splitsToReturn = splitsToReturn;
        }

        @Override
        protected List<JdbcSourceSplit> discoverSplits(Connection connection) {
            discoverCallCount++;
            return splitsToReturn;
        }

        @Override
        protected String describeSource() {
            return "test-source";
        }
    }

    /** Tracks call counts instead of returning a real connection - no test here needs one. */
    private static class CountingConnectionProvider implements JdbcConnectionProvider {
        private int getOrEstablishConnectionCallCount = 0;
        private int closeConnectionCallCount = 0;

        @Override
        public Connection getConnection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Properties getProperties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isConnectionValid() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection getOrEstablishConnection() throws SQLException {
            getOrEstablishConnectionCallCount++;
            return null;
        }

        @Override
        public void closeConnection() {
            closeConnectionCallCount++;
        }

        @Override
        public Connection reestablishConnection() {
            throw new UnsupportedOperationException();
        }
    }
}
