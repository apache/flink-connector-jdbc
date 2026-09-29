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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator;

import org.apache.flink.connector.jdbc.core.datastream.source.split.CheckpointedOffset;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplitSerializer;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link
 * org.apache.flink.connector.jdbc.core.datastream.source.enumerator.JdbcSourceEnumStateSerializer}.
 */
class JdbcSourceEnumStateSerializerTest {

    private final JdbcSourceEnumeratorState state =
            new JdbcSourceEnumeratorState(
                    Arrays.asList(new JdbcSourceSplit("1", "select 1", null, null)),
                    Arrays.asList(
                            new JdbcSourceSplit(
                                    "1",
                                    "select 1",
                                    new Serializable[] {new Integer(0)},
                                    new CheckpointedOffset(0, 10))),
                    Arrays.asList(new JdbcSourceSplit("1", "select 1", null, null)),
                    null);
    private final JdbcSourceEnumeratorState mockedState = new MockedJdbcSourceEnumState(state);
    private final JdbcSourceEnumStateSerializer serializer =
            new JdbcSourceEnumStateSerializer(new JdbcSourceSplitSerializer());
    private final JdbcSourceEnumStateSerializer mockedSerializer =
            new JdbcSourceEnumStateSerializer(new JdbcSourceSplitSerializer()) {
                @Override
                public int getVersion() {
                    // clearly outside the known versions {0, 1}
                    return new Random().nextInt(10) + 2;
                }
            };

    @Test
    void testSerialize() throws IOException {
        // Test for un-matched instance of splits.
        assertThatThrownBy(() -> serializer.serialize(mockedState))
                .isInstanceOf(IllegalArgumentException.class);

        // Test for matched version.
        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(state)))
                .isEqualTo(state);
    }

    @Test
    void testDeserialize() throws IOException {
        // Test for un-matched version.
        assertThatThrownBy(
                        () ->
                                mockedSerializer.deserialize(
                                        mockedSerializer.getVersion(), serializer.serialize(state)))
                .isInstanceOf(IOException.class);

        // Test for matched version.
        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(state)))
                .isEqualTo(state);
    }

    @Test
    void testRoundTripWithSnapshotIdSplits() throws IOException {
        JdbcSourceEnumeratorState snapshotState =
                new JdbcSourceEnumeratorState(
                        List.of(),
                        List.of(),
                        List.of(
                                new JdbcSourceSplit(
                                        "1",
                                        "select 1",
                                        new Serializable[] {7},
                                        new CheckpointedOffset(3, 4),
                                        "0-ABC-12")),
                        null);

        JdbcSourceEnumeratorState deserialized =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(snapshotState));

        assertThat(deserialized).isEqualTo(snapshotState);
        assertThat(deserialized.getRemainingSplits().get(0).getGlobalSnapshotId())
                .isEqualTo("0-ABC-12");
    }

    @Test
    void testLegacyVersionZeroBytesStillDeserialize() throws IOException {
        byte[] legacyBytes = writeLegacyEnumStateFormat(state);

        JdbcSourceEnumeratorState deserialized = serializer.deserialize(0, legacyBytes);

        assertThat(deserialized).isEqualTo(state);
    }

    /** Writes enum state in the pre-snapshot (version 0) wire format, including legacy splits. */
    private static byte[] writeLegacyEnumStateFormat(JdbcSourceEnumeratorState enumeratorState)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            writeLegacySplits(out, enumeratorState.getCompletedSplits());
            writeLegacySplits(out, enumeratorState.getPendingSplits());
            writeLegacySplits(out, enumeratorState.getRemainingSplits());
            byte[] udsBytes =
                    InstantiationUtil.serializeObject(
                            enumeratorState.getOptionalUserDefinedSplitEnumeratorState());
            out.writeInt(udsBytes.length);
            out.write(udsBytes);
        }
        return baos.toByteArray();
    }

    private static void writeLegacySplits(DataOutputStream out, List<JdbcSourceSplit> splits)
            throws IOException {
        out.writeInt(splits.size());
        for (JdbcSourceSplit split : splits) {
            out.writeUTF(split.splitId());
            out.writeUTF(split.getSqlTemplate());
            byte[] paramsBytes = InstantiationUtil.serializeObject(split.getParameters());
            out.writeInt(paramsBytes.length);
            out.write(paramsBytes);
            byte[] chkOffset = InstantiationUtil.serializeObject(split.getCheckpointedOffset());
            out.writeInt(chkOffset.length);
            out.write(chkOffset);
        }
    }

    static class MockedJdbcSourceEnumState extends JdbcSourceEnumeratorState {

        public MockedJdbcSourceEnumState(JdbcSourceEnumeratorState state) {
            super(
                    state.getCompletedSplits(),
                    state.getPendingSplits(),
                    state.getRemainingSplits(),
                    state.getOptionalUserDefinedSplitEnumeratorState());
        }
    }
}
