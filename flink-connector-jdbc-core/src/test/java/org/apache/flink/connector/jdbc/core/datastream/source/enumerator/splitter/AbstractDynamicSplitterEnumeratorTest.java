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

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AbstractDynamicSplitterEnumeratorTest {

    private JdbcConnectionProvider connectionProvider;

    @BeforeEach
    void setUp() throws Exception {
        connectionProvider = mock(JdbcConnectionProvider.class);
        when(connectionProvider.getOrEstablishConnection()).thenReturn(mock(Connection.class));
    }

    @Test
    void testStartDoesNotOpenConnection() {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());

        enumerator.start(connectionProvider);

        verifyNoInteractions(connectionProvider);
    }

    @Test
    void testFirstEnumerateSplitsDiscoversOnce() throws Exception {
        List<JdbcSourceSplit> expected = singletonSplitList();
        TestEnumerator enumerator = new TestEnumerator(expected);
        enumerator.start(connectionProvider);

        List<JdbcSourceSplit> result = enumerator.enumerateSplits();

        assertThat(result).isEqualTo(expected);
        assertThat(enumerator.discoverCallCount).isEqualTo(1);
        verify(connectionProvider, times(1)).getOrEstablishConnection();
    }

    @Test
    void testRepeatedEnumerateSplitsOnlyDiscoversOnce() throws Exception {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());
        enumerator.start(connectionProvider);

        enumerator.enumerateSplits();
        List<JdbcSourceSplit> second = enumerator.enumerateSplits();

        assertThat(second).isEmpty();
        assertThat(enumerator.discoverCallCount).isEqualTo(1);
        verify(connectionProvider, times(1)).getOrEstablishConnection();
    }

    @Test
    void testIsAllSplitsFinishedTracksState() throws Exception {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());
        enumerator.start(connectionProvider);

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
        verifyNoInteractions(connectionProvider);
    }

    @Test
    void testCloseDelegatesToConnectionProvider() {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());
        enumerator.start(connectionProvider);

        enumerator.close();

        verify(connectionProvider, times(1)).closeConnection();
    }

    @Test
    void testCloseWithoutStartIsNoOp() {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());

        enumerator.close();

        verifyNoInteractions(connectionProvider);
    }

    @Test
    void testSerializableStateReflectsFinishedFlag() throws Exception {
        TestEnumerator enumerator = new TestEnumerator(singletonSplitList());
        enumerator.start(connectionProvider);

        assertThat(enumerator.serializableState()).isEqualTo(Boolean.FALSE);

        enumerator.enumerateSplits();

        assertThat(enumerator.serializableState()).isEqualTo(Boolean.TRUE);
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
}
