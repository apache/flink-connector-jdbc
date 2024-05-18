package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** {@link JdbcWriterState} serializer. */
@Internal
public class JdbcWriterStateSerializer implements SimpleVersionedSerializer<JdbcWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriterStateSerializer.class);

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(JdbcWriterState state) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(1);
        out.writeInt(state.getHanging().size());
        for (TransactionId tid : state.getHanging()) {
            byte[] tIdBytes = tid.serialize();
            out.writeByte(tIdBytes.length);
            out.write(tIdBytes, 0, tIdBytes.length);
        }
        out.writeInt(state.getPrepared().size());
        for (TransactionId tid : state.getPrepared()) {
            byte[] tIdBytes = tid.serialize();
            out.writeByte(tIdBytes.length);
            out.write(tIdBytes, 0, tIdBytes.length);
        }
        return out.getSharedBuffer();
    }

    @Override
    public JdbcWriterState deserialize(int version, byte[] serialized) throws IOException {
        if (version == getVersion()) {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return deserializeV2(in);
        }

        LOG.error("Unknown version of state: {}", version);
        return JdbcWriterState.empty();
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
