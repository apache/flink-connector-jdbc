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
import org.apache.flink.util.StringUtils;

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

    @Test
    void testSerDeWithPrefix() throws IOException {
        TransactionId baseTid = TransactionId.create("orders", new JobID().getBytes(), 1, 2);
        JdbcWriterState original =
                JdbcWriterState.of(
                        Arrays.asList(baseTid.withBranch(1001L)),
                        Arrays.asList(baseTid.withBranch(2001L)));
        JdbcWriterStateSerializer tester = new JdbcWriterStateSerializer();

        JdbcWriterState deserialized =
                tester.deserialize(tester.getVersion(), tester.serialize(original));

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.getHanging().iterator().next().getPrefix()).hasValue("orders");
    }

    /** Pins the layout of version 3, so that a change to it can't go unnoticed. */
    @Test
    void testVersion3Layout() throws IOException {
        TransactionId baseTid =
                TransactionId.create(
                        "orders",
                        JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes(),
                        1,
                        2);
        JdbcWriterState state =
                JdbcWriterState.of(
                        Arrays.asList(baseTid.withBranch(1001L)),
                        Arrays.asList(baseTid.withBranch(2001L)));
        byte[] version3 =
                StringUtils.hexStringToByte(
                        "00000001446b64d8a9a951e2e8767ae952ad951706000000010000000200000000000007"
                                + "d10000000000000000000000000000000000000000000000000000000000000000000000"
                                + "00066f726465727300000001446b64d8a9a951e2e8767ae952ad95170600000001000000"
                                + "0200000000000003e9000000000000000000000000000000000000000000000000000000"
                                + "000000000000000000066f7264657273");
        JdbcWriterStateSerializer serializer = new JdbcWriterStateSerializer();

        assertThat(serializer.getVersion()).isEqualTo(3);
        assertThat(serializer.serialize(state)).isEqualTo(version3);
        assertThat(serializer.deserialize(3, version3)).isEqualTo(state);
    }

    /** Version 2 is the state of 4.1.0, written by the serializer of that release. */
    @Test
    void testDeserializeVersion2() throws IOException {
        TransactionId baseTid =
                TransactionId.create(
                        JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes(), 1, 2);
        byte[] version2 =
                StringUtils.hexStringToByte(
                        "00000001446b64d8a9a951e2e8767ae952ad951706000000010000000200000000000007"
                                + "d10000000000000000000000000000000000000000000000000000000000000000000000"
                                + "0000000001446b64d8a9a951e2e8767ae952ad9517060000000100000002000000000000"
                                + "03e900000000000000000000000000000000000000000000000000000000000000000000"
                                + "0000");

        JdbcWriterState deserialized = new JdbcWriterStateSerializer().deserialize(2, version2);

        assertThat(deserialized)
                .isEqualTo(
                        JdbcWriterState.of(
                                Arrays.asList(baseTid.withBranch(1001L)),
                                Arrays.asList(baseTid.withBranch(2001L))));
        assertThat(deserialized.getHanging().iterator().next().getGlobalTransactionId())
                .isEqualTo(baseTid.getGlobalTransactionId());
    }
}
