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

package org.apache.flink.connector.jdbc.source.split;

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

    private static final int CURRENT_VERSION = 0;

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

        if (version != CURRENT_VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            return deserializeJdbcSourceSplit(in);
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

        out.writeInt(sourceSplit.getOffset());

        CheckpointedOffset checkpointedOffset = sourceSplit.getCheckpointedOffset();
        byte[] chkOffset = InstantiationUtil.serializeObject(checkpointedOffset);
        out.writeInt(chkOffset.length);
        out.write(chkOffset);
    }

    public JdbcSourceSplit deserializeJdbcSourceSplit(DataInputStream in)
            throws IOException, ClassNotFoundException {
        String id = in.readUTF();
        String sqlTemplate = in.readUTF();
        int parametersLen = in.readInt();
        byte[] parametersBytes = new byte[parametersLen];
        in.read(parametersBytes);
        Serializable[] params =
                InstantiationUtil.deserializeObject(
                        parametersBytes, in.getClass().getClassLoader());

        int offset = in.readInt();

        int chkOffsetBytesLen = in.readInt();
        byte[] chkOffsetBytes = new byte[chkOffsetBytesLen];
        in.read(chkOffsetBytes);
        CheckpointedOffset chkOffset =
                InstantiationUtil.deserializeObject(chkOffsetBytes, in.getClass().getClassLoader());

        return new JdbcSourceSplit(id, sqlTemplate, params, offset, chkOffset);
    }
}
