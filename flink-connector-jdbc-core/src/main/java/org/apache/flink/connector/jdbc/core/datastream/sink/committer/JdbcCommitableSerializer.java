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

package org.apache.flink.connector.jdbc.core.datastream.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.xid.XidSerializer;
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
