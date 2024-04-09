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

package org.apache.flink.connector.jdbc.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.jdbc.source.split.CheckpointedOffset;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link JdbcSourceEnumerator}. */
class JdbcSourceEnumeratorTest {

    private static long splitId = 1L;

    private TestingSplitEnumeratorContext<JdbcSourceSplit> context;
    private JdbcSourceSplit split;
    private JdbcSourceEnumerator enumerator;

    @BeforeEach
    void setup() {
        this.context = new TestingSplitEnumeratorContext<>(4);
        this.split = createRandomSplit();
        this.enumerator = createEnumerator(context, split);
    }

    @Test
    void testCheckpointNoSplitRequested() throws Exception {
        JdbcSourceEnumeratorState state = enumerator.snapshotState(1L);
        assertThat(state.getPendingSplits()).isEmpty();
        assertThat(state.getRemainingSplits()).contains(split);
    }

    @Test
    void testSplitRequestForRegisteredReader() throws Exception {
        context.registerReader(3, "somehost");
        enumerator.addReader(3);
        enumerator.handleSplitRequest(3, "somehost");
        assertThat(enumerator.snapshotState(1L).getRemainingSplits()).isEmpty();
        assertThat(context.getSplitAssignments().get(3).getAssignedSplits()).contains(split);
    }

    @Test
    void testSplitRequestForNonRegisteredReader() throws Exception {
        enumerator.handleSplitRequest(3, "somehost");
        assertThat(context.getSplitAssignments()).doesNotContainKey(3);
        assertThat(enumerator.snapshotState(1L).getRemainingSplits()).contains(split);
    }

    @Test
    void testNoMoreSplits() {
        // first split assignment
        context.registerReader(1, "somehost");
        enumerator.addReader(1);
        enumerator.handleSplitRequest(1, "somehost");

        // second request has no more split
        enumerator.handleSplitRequest(1, "somehost");

        assertThat(context.getSplitAssignments().get(1).getAssignedSplits()).contains(split);
        assertThat(context.getSplitAssignments().get(1).hasReceivedNoMoreSplitsSignal()).isTrue();
    }

    private static JdbcSourceSplit createRandomSplit() {
        return new JdbcSourceSplit(
                String.valueOf(splitId++),
                "select 1",
                new Serializable[] {0},
                0,
                new CheckpointedOffset(0, 0));
    }

    private static JdbcSourceEnumerator createEnumerator(
            final SplitEnumeratorContext<JdbcSourceSplit> context,
            final JdbcSourceSplit... splits) {

        return new JdbcSourceEnumerator(
                context,
                new JdbcSqlSplitEnumeratorBase<JdbcSourceSplit>(null) {
                    @Override
                    public List<JdbcSourceSplit> enumerateSplits() throws IOException {
                        return Collections.emptyList();
                    }
                },
                Arrays.stream(splits).collect(Collectors.toList()));
    }
}
