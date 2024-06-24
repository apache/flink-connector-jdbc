package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Smoke test for {@link JdbcWriterState}. */
class JdbcWriterStateTest {

    @Test
    void testEquals() {
        assertThat(JdbcWriterState.empty()).isEqualTo(JdbcWriterState.empty());

        TransactionId baseTid =
                TransactionId.create(
                        JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706").getBytes(), 1, 2);

        JdbcWriterState state1 =
                JdbcWriterState.of(
                        Arrays.asList(baseTid.withBranch(1001L), baseTid.withBranch(1002L)),
                        Arrays.asList(baseTid.withBranch(2001L), baseTid.withBranch(2002L)));
        JdbcWriterState state2 =
                JdbcWriterState.of(
                        Arrays.asList(baseTid.withBranch(1001L), baseTid.withBranch(1002L)),
                        Arrays.asList(baseTid.withBranch(2001L), baseTid.withBranch(2002L)));

        assertThat(state1).isEqualTo(state2);
    }
}
