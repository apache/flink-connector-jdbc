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

package org.apache.flink.connector.jdbc.datasource.transactions.xa.domain;

import org.apache.flink.api.common.JobID;

import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.util.StringUtils.byteToHexString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionIdTest {
    @Test
    void testTransactionIdCreation() throws IOException {

        byte[] jobIdBytes = JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes();
        int subtaskId = 1;
        int numberOfSubtasks = 2;
        long checkpoint = 1234L;
        TransactionId tid =
                TransactionId.create(jobIdBytes, subtaskId, numberOfSubtasks)
                        .withBranch(checkpoint);

        assertThat(tid.getFormatId()).isEqualTo(202);
        assertThat(byteToHexString(tid.getGlobalTransactionId()))
                .isEqualTo("6b64d8a9a951e2e8767ae952ad951706000000010000000000000000000000000000");
        assertThat(byteToHexString(tid.getBranchQualifier()))
                .isEqualTo("0000000200000000000004d200");

        TransactionId fromXid =
                TransactionId.restoreFromXid(
                        tid.getFormatId(), tid.getGlobalTransactionId(), tid.getBranchQualifier());
        assertThat(tid).isEqualTo(fromXid);
    }

    @Test
    void testTransactionIdWithPrefix() throws IOException {
        byte[] jobIdBytes = JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes();
        TransactionId tid = TransactionId.create("orders", jobIdBytes, 1, 2).withBranch(1234L);

        assertThat(tid.getFormatId()).isEqualTo(203);
        assertThat(tid.getPrefix()).hasValue("orders");
        // "orders" in UTF-8 is 6f7264657273
        assertThat(byteToHexString(tid.getGlobalTransactionId()))
                .isEqualTo("066f72646572736b64d8a9a951e2e8767ae952ad95170600000001");
        assertThat(byteToHexString(tid.getBranchQualifier())).isEqualTo("0000000200000000000004d2");

        TransactionId fromXid =
                TransactionId.restoreFromXid(
                        tid.getFormatId(), tid.getGlobalTransactionId(), tid.getBranchQualifier());
        assertThat(fromXid).isEqualTo(tid);
        assertThat(fromXid.getPrefix()).hasValue("orders");
        assertThat(fromXid).isNotEqualTo(TransactionId.create(jobIdBytes, 1, 2).withBranch(1234L));
    }

    @Test
    void testTransactionIdWithLongestPrefix() {
        char[] prefix = new char[TransactionId.MAX_PREFIX_BYTES];
        Arrays.fill(prefix, 'p');
        TransactionId tid =
                TransactionId.create(new String(prefix), new byte[JobID.SIZE], 0, 1).withBranch(0L);

        assertThat(tid.getGlobalTransactionId()).hasSize(Xid.MAXGTRIDSIZE);

        assertThatThrownBy(
                        () ->
                                TransactionId.create(
                                        new String(prefix) + "p", new byte[JobID.SIZE], 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** The writer state stores the prefix next to what serialize() writes. */
    @Test
    void testSerializeWithPrefix() {
        TransactionId tid =
                TransactionId.create("orders", new JobID().getBytes(), 3, 4)
                        .withBranch(5L)
                        .withAttemptsIncremented();

        TransactionId deserialized =
                TransactionId.deserialize(tid.serialize()).withPrefix(tid.getPrefixBytes());

        assertThat(deserialized.equals(tid, true)).isFalse();
        assertThat(deserialized.equals(tid, false)).isTrue();
        assertThat(deserialized.getAttempts()).isEqualTo(1);
        assertThat(deserialized.getPrefix()).hasValue("orders");
        assertThat(TransactionId.deserialize(tid.serialize()).getPrefix()).isEmpty();
    }

    @Test
    void testPrefixedXidOfWrongLength() {
        TransactionId tid =
                TransactionId.create("orders", new byte[JobID.SIZE], 0, 1).withBranch(0L);
        byte[] longer = Arrays.copyOf(tid.getGlobalTransactionId(), Xid.MAXGTRIDSIZE);

        assertThatThrownBy(() -> TransactionId.restoreFromXid(203, new byte[3], new byte[0]))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(
                        () -> TransactionId.restoreFromXid(203, longer, tid.getBranchQualifier()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testTransactionIdWithMaxValues() {

        byte[] jobIdBytes = new byte[JobID.SIZE];
        TransactionId tid =
                TransactionId.create(jobIdBytes, Integer.MAX_VALUE, Integer.MAX_VALUE)
                        .withBranch(Long.MAX_VALUE);

        assertThat(tid.getFormatId()).isEqualTo(202);
        assertThat(tid.getGlobalTransactionId().length).isLessThanOrEqualTo(Xid.MAXGTRIDSIZE);

        assertThat(byteToHexString(tid.getGlobalTransactionId()))
                .isEqualTo("000000000000000000000000000000007fffffff0000000000000000000000000000");

        assertThat(tid.getBranchQualifier().length).isLessThanOrEqualTo(Xid.MAXBQUALSIZE);
        assertThat(byteToHexString(tid.getBranchQualifier()))
                .isEqualTo("7fffffff7fffffffffffffff00");
    }

    @Test
    void testEqualsAndHashcode() {
        TransactionId tid =
                TransactionId.create(
                        JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes(), 1, 2);

        assertThat(tid.equals(tid.withBranch(123L))).isFalse();
        assertThat(tid.withBranch(123L).equals(tid.withBranch(123L))).isTrue();

        assertThat(tid.equals(tid.withAttemptsIncremented())).isTrue();
        assertThat(tid.equals(tid.withAttemptsIncremented(), true)).isFalse();

        assertThat(tid.hashCode()).isEqualTo(tid.withAttemptsIncremented().hashCode());
        assertThat(tid.hashCode(true)).isNotEqualTo(tid.withAttemptsIncremented().hashCode(true));

        TransactionId rid =
                TransactionId.restore(
                        JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes(),
                        1,
                        2,
                        123L,
                        0);

        assertThat(Arrays.asList(tid.withBranch(123L))).isEqualTo(Arrays.asList(rid));
    }
}
