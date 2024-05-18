package org.apache.flink.connector.jdbc.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.xa.XidSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import javax.transaction.xa.Xid;

import java.io.IOException;

/** {@link JdbcCommitable} serializer. */
@Internal
public class JdbcCommitableSerializer implements SimpleVersionedSerializer<JdbcCommitable> {

    private final TypeSerializer<Xid> xidSerializer = new XidSerializer();

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(JdbcCommitable commitable) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(1);
        xidSerializer.serialize(commitable.getXid(), out);
        return out.getSharedBuffer();
    }

    @Override
    public JdbcCommitable deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        return JdbcCommitable.of(xidSerializer.deserialize(in));
    }
}
