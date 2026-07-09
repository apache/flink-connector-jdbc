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

package org.apache.flink.connector.jdbc.core.datastream.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The class is used to de/serialize the {@link JdbcSourceSplit}. */
public class JdbcSourceSplitSerializer implements SimpleVersionedSerializer<JdbcSourceSplit> {

    private static final int CURRENT_VERSION = 1;
    private static final int LEGACY_VERSION_NO_SNAPSHOT = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(JdbcSourceSplit split) throws IOException {

        checkArgument(
                split.getClass() == JdbcSourceSplit.class,
                "Cannot serialize classes of JdbcSourceSplit");

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            serializeJdbcSourceSplit(out, split);

            out.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public JdbcSourceSplit deserialize(int version, byte[] serialized) throws IOException {

        if (version != CURRENT_VERSION && version != LEGACY_VERSION_NO_SNAPSHOT) {
            throw new IOException("Unknown version: " + version);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            return deserializeJdbcSourceSplit(version, in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void serializeJdbcSourceSplit(DataOutputStream out, JdbcSourceSplit sourceSplit)
            throws IOException {
        out.writeUTF(sourceSplit.splitId());
        out.writeUTF(sourceSplit.getSqlTemplate());

        byte[] paramsBytes = InstantiationUtil.serializeObject(sourceSplit.getParameters());
        out.writeInt(paramsBytes.length);
        out.write(paramsBytes);

        // The checkpointed offset is two plain longs — encoding it as a Java-serialized object
        // would cost a full ObjectOutputStream per split on every checkpoint. (Only the version-0
        // wire format used object encoding; v1 was never released with it.)
        CheckpointedOffset checkpointedOffset = sourceSplit.getCheckpointedOffset();
        out.writeBoolean(checkpointedOffset != null);
        if (checkpointedOffset != null) {
            out.writeLong(checkpointedOffset.getOffset());
            out.writeLong(checkpointedOffset.getRecordsAfterOffset());
        }

        String globalSnapshotId = sourceSplit.getGlobalSnapshotId();
        out.writeBoolean(globalSnapshotId != null);
        if (globalSnapshotId != null) {
            out.writeUTF(globalSnapshotId);
        }
    }

    /**
     * Reads a split in the pre-snapshot (version 0) wire format.
     *
     * @deprecated use {@link #deserializeJdbcSourceSplit(int, DataInputStream)} with the stored
     *     serializer version so newer formats are handled correctly.
     */
    @Deprecated
    public JdbcSourceSplit deserializeJdbcSourceSplit(DataInputStream in)
            throws IOException, ClassNotFoundException {
        return deserializeJdbcSourceSplit(LEGACY_VERSION_NO_SNAPSHOT, in);
    }

    public JdbcSourceSplit deserializeJdbcSourceSplit(int version, DataInputStream in)
            throws IOException, ClassNotFoundException {
        String id = in.readUTF();
        String sqlTemplate = in.readUTF();
        int parametersLen = in.readInt();
        byte[] parametersBytes = new byte[parametersLen];
        in.read(parametersBytes);
        Serializable[] params =
                InstantiationUtil.deserializeObject(
                        parametersBytes, in.getClass().getClassLoader());

        CheckpointedOffset chkOffset;
        if (version == LEGACY_VERSION_NO_SNAPSHOT) {
            int chkOffsetBytesLen = in.readInt();
            byte[] chkOffsetBytes = new byte[chkOffsetBytesLen];
            in.read(chkOffsetBytes);
            chkOffset =
                    InstantiationUtil.deserializeObject(
                            chkOffsetBytes, CheckpointedOffset.class.getClassLoader());
        } else {
            chkOffset =
                    in.readBoolean() ? new CheckpointedOffset(in.readLong(), in.readLong()) : null;
        }

        String globalSnapshotId = null;
        if (version >= CURRENT_VERSION && in.readBoolean()) {
            globalSnapshotId = in.readUTF();
        }

        return new JdbcSourceSplit(id, sqlTemplate, params, chkOffset, globalSnapshotId);
    }
}
