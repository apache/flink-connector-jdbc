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

import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The serializer for {@link JdbcSourceEnumeratorState}. */
public class JdbcSourceEnumStateSerializer
        implements SimpleVersionedSerializer<JdbcSourceEnumeratorState>, Serializable {

    private static final int CURRENT_VERSION = 0;

    private final JdbcSourceSplitSerializer splitSerializer;

    public JdbcSourceEnumStateSerializer(JdbcSourceSplitSerializer splitSerializer) {
        this.splitSerializer = Preconditions.checkNotNull(splitSerializer);
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(JdbcSourceEnumeratorState state) throws IOException {

        checkArgument(
                state.getClass() == JdbcSourceEnumeratorState.class,
                "Cannot serialize classes of %s",
                JdbcSourceEnumeratorState.class.getSimpleName());

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            serializeJdbcSourceSplits(out, state.getCompletedSplits());
            serializeJdbcSourceSplits(out, state.getPendingSplits());
            serializeJdbcSourceSplits(out, state.getRemainingSplits());
            byte[] udsBytes =
                    InstantiationUtil.serializeObject(
                            state.getOptionalUserDefinedSplitEnumeratorState());
            out.writeInt(udsBytes.length);
            out.write(udsBytes);
            out.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void serializeJdbcSourceSplits(
            DataOutputStream out, List<JdbcSourceSplit> jdbcSourceSplits) throws Exception {

        out.writeInt(jdbcSourceSplits.size());
        for (JdbcSourceSplit sourceSplit : jdbcSourceSplits) {
            Preconditions.checkNotNull(sourceSplit);
            splitSerializer.serializeJdbcSourceSplit(out, sourceSplit);
        }
    }

    @Override
    public JdbcSourceEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {

        if (version != CURRENT_VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            List<JdbcSourceSplit> completedSplits = deserializeSourceSplits(in);
            List<JdbcSourceSplit> pendingSplits = deserializeSourceSplits(in);
            List<JdbcSourceSplit> remainingSplits = deserializeSourceSplits(in);
            int bytesLen = in.readInt();
            byte[] bytes = new byte[bytesLen];
            in.read(bytes);
            return new JdbcSourceEnumeratorState(
                    completedSplits,
                    pendingSplits,
                    remainingSplits,
                    InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<JdbcSourceSplit> deserializeSourceSplits(DataInputStream in) throws Exception {
        int size = in.readInt();
        List<JdbcSourceSplit> jdbcSourceSplits = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            jdbcSourceSplits.add(splitSerializer.deserializeJdbcSourceSplit(in));
        }
        return jdbcSourceSplits;
    }
}
