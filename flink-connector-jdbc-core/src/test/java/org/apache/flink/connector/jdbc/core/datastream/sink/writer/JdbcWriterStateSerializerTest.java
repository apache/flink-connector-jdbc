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

import org.apache.flink.api.common.JobID;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test for {@link
 * org.apache.flink.connector.jdbc.core.datastream.sink.writer.JdbcWriterStateSerializer}.
 */
class JdbcWriterStateSerializerTest {

    @Test
    void testBasicSerDe() throws IOException {
        TransactionId baseTid =
                TransactionId.create(
                        JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes(), 1, 2);

        JdbcWriterState original =
                JdbcWriterState.of(
                        Arrays.asList(baseTid.withBranch(1001L), baseTid.withBranch(1002L)),
                        Arrays.asList(baseTid.withBranch(2001L), baseTid.withBranch(2002L)));

        JdbcWriterStateSerializer tester = new JdbcWriterStateSerializer();

        byte[] serialized = tester.serialize(original);
        JdbcWriterState deserialized = tester.deserialize(tester.getVersion(), serialized);

        assertThat(deserialized).isEqualTo(original);
    }
}
