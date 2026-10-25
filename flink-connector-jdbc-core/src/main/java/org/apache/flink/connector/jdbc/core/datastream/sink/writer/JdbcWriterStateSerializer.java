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

package org.apache.flink.connector.jdbc.core.datastream.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** {@link JdbcWriterState} serializer. */
@Internal
public class JdbcWriterStateSerializer implements SimpleVersionedSerializer<JdbcWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriterStateSerializer.class);

    @Override
    public int getVersion() {
        // version 3 adds the transaction id prefix
        return 3;
    }

    @Override
    public byte[] serialize(JdbcWriterState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        writeTransactionIds(out, state.getHanging());
        writeTransactionIds(out, state.getPrepared());
        out.flush();
        return bytes.toByteArray();
    }

    private static void writeTransactionIds(
            DataOutputStream out, Collection<TransactionId> transactionIds) throws IOException {
        out.writeInt(transactionIds.size());
        for (TransactionId tid : transactionIds) {
            byte[] tIdBytes = tid.serialize();
            out.writeByte(tIdBytes.length);
            out.write(tIdBytes);
            byte[] prefix = tid.getPrefixBytes();
            out.writeByte(prefix == null ? -1 : prefix.length);
            if (prefix != null) {
                out.write(prefix);
            }
        }
    }

    @Override
    public JdbcWriterState deserialize(int version, byte[] serialized) throws IOException {
        if (version == getVersion()) {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
            List<TransactionId> hanging = readTransactionIds(in);
            List<TransactionId> prepared = readTransactionIds(in);
            return JdbcWriterState.of(prepared, hanging);
        }
        if (version == 2) {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return deserializeV2(in);
        }

        LOG.error("Unknown version of state: {}", version);
        return JdbcWriterState.empty();
    }

    private static List<TransactionId> readTransactionIds(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<TransactionId> transactionIds = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            byte[] bytes = new byte[in.readUnsignedByte()];
            in.readFully(bytes);
            byte[] prefix = null;
            int prefixLength = in.readByte();
            if (prefixLength >= 0) {
                prefix = new byte[prefixLength];
                in.readFully(prefix);
            }
            transactionIds.add(TransactionId.deserialize(bytes).withPrefix(prefix));
        }
        return transactionIds;
    }

    private JdbcWriterState deserializeV2(DataInputDeserializer in) throws IOException {
        int hangingSize = in.readInt();
        List<TransactionId> hanging = new ArrayList<>(hangingSize);
        for (int i = 0; i < hangingSize; i++) {
            byte len = in.readByte();
            byte[] bytes = new byte[len];
            in.read(bytes, 0, len);
            hanging.add(TransactionId.deserialize(bytes));
        }
        int preparedSize = in.readInt();
        List<TransactionId> prepared = new ArrayList<>(preparedSize);
        for (int i = 0; i < preparedSize; i++) {
            byte len = in.readByte();
            byte[] bytes = new byte[len];
            in.read(bytes, 0, len);
            prepared.add(TransactionId.deserialize(bytes));
        }
        return JdbcWriterState.of(prepared, hanging);
    }
}
