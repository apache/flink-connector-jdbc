package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.Sink;

import java.io.Serializable;

/** Job identifier. */
@Internal
class JobSubtask implements Serializable {

    private final byte[] jobId;
    private final int subtaskId;
    private final int numberOfSubtasks;

    public JobSubtask(byte[] jobId, int subtaskId, int numberOfSubtasks) {
        this.jobId = jobId;
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
    }

    public byte[] getJobId() {
        return jobId;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    public static JobSubtask of(RuntimeContext context) {
        return new JobSubtask(
                context.getJobId().getBytes(),
                context.getIndexOfThisSubtask(),
                context.getNumberOfParallelSubtasks());
    }

    public static JobSubtask of(Sink.InitContext context) {
        return new JobSubtask(
                context.getJobId().getBytes(),
                context.getSubtaskId(),
                context.getNumberOfParallelSubtasks());
    }

    public static JobSubtask empty() {
        return new JobSubtask("".getBytes(), 0, 0);
    }
}
