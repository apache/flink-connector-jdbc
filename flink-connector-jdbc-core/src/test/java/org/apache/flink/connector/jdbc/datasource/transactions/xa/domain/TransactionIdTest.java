package org.apache.flink.connector.jdbc.datasource.transactions.xa.domain;

import org.apache.flink.api.common.JobID;

import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.util.StringUtils.byteToHexString;
import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    void testBelongsTo() {
        TransactionId tid1 =
                TransactionId.create(
                        JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes(), 1, 2);
        TransactionId tid2 =
                TransactionId.create(
                        JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes(), 2, 2);
        TransactionId tid3 =
                TransactionId.create(
                        JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes(), 3, 3);

        assertThat(tid1.belongsTo(tid1)).isTrue();
        assertThat(tid1.belongsTo(tid2)).isFalse();
        assertThat(tid1.belongsTo(tid3)).isTrue();

        assertThat(tid2.belongsTo(tid1)).isFalse();
        assertThat(tid2.belongsTo(tid2)).isTrue();
        assertThat(tid2.belongsTo(tid3)).isTrue();

        assertThat(tid3.belongsTo(tid1)).isFalse();
        assertThat(tid3.belongsTo(tid2)).isFalse();
        assertThat(tid3.belongsTo(tid3)).isTrue();
    }
}
