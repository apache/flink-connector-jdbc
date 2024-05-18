package org.apache.flink.connector.jdbc.datasource.transactions.xa.domain;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.util.StringUtils.byteToHexString;

/** A simple {@link Xid} implementation. */
@Internal
public class TransactionId implements Xid, Serializable {

    private static final long serialVersionUID = 1L;

    private static final int FORMAT_ID = 202;

    private final byte[] jobId;
    private final int subtaskId;
    private final int numberOfSubtasks;
    private final long checkpointId;
    private final int attempts;
    private final boolean restored;

    private TransactionId(
            byte[] jobId,
            int subtaskId,
            int numberOfSubtasks,
            long checkpointId,
            int attempts,
            boolean restored) {
        this.jobId = jobId;
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
        this.checkpointId = checkpointId;
        this.attempts = attempts;
        this.restored = restored;
    }

    public static TransactionId empty() {
        return create(new byte[JobID.SIZE], 0, 0);
    }

    public static TransactionId create(byte[] jobId, int subtaskId, int numberOfSubtasks) {
        return new TransactionId(jobId, subtaskId, numberOfSubtasks, -1, 0, false);
    }

    public static TransactionId restore(
            byte[] jobId, int subtaskId, int numberOfSubtasks, long checkpointId, int attempts) {
        return new TransactionId(jobId, subtaskId, numberOfSubtasks, checkpointId, attempts, true);
    }

    public static TransactionId createFromXid(
            int formatId, byte[] globalTransactionId, byte[] branchQualifier) throws IOException {
        return fromXid(formatId, globalTransactionId, branchQualifier, false);
    }

    public static TransactionId restoreFromXid(
            int formatId, byte[] globalTransactionId, byte[] branchQualifier) throws IOException {
        return fromXid(formatId, globalTransactionId, branchQualifier, true);
    }

    public static TransactionId fromXid(
            int formatId, byte[] globalTransactionId, byte[] branchQualifier, boolean restored)
            throws IOException {
        if (FORMAT_ID != formatId) {
            throw new IOException(String.format("Xid formatId (%s) is not valid", formatId));
        }

        final DataInputDeserializer gid = new DataInputDeserializer(globalTransactionId);
        byte[] jobIdBytes = readJobId(gid);
        int subtaskId = gid.readInt();

        final DataInputDeserializer branch = new DataInputDeserializer(branchQualifier);
        int numberOfSubtasks = branch.readInt();
        long checkpoint = branch.readLong();

        return new TransactionId(jobIdBytes, subtaskId, numberOfSubtasks, checkpoint, 0, restored);
    }

    public static TransactionId deserialize(byte[] bytes) {
        try {
            final DataInputDeserializer in = new DataInputDeserializer(bytes);
            byte[] jobIdBytes = readJobId(in);
            int subtaskId = in.readInt();
            int numberOfSubtasks = in.readInt();
            long checkpoint = in.readLong();
            int attempts = in.readInt();
            return restore(jobIdBytes, subtaskId, numberOfSubtasks, checkpoint, attempts);
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    private static byte[] readJobId(DataInputDeserializer in) throws IOException {
        byte[] jobIdBytes = new byte[JobID.SIZE];
        in.read(jobIdBytes);
        return jobIdBytes;
    }

    public TransactionId copy() {
        return new TransactionId(
                jobId, subtaskId, numberOfSubtasks, checkpointId, attempts, restored);
    }

    public TransactionId withBranch(long checkpointId) {
        return new TransactionId(
                jobId, subtaskId, numberOfSubtasks, checkpointId, attempts, restored);
    }

    public TransactionId withAttemptsIncremented() {
        return new TransactionId(
                jobId, subtaskId, numberOfSubtasks, checkpointId, attempts + 1, restored);
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public boolean getRestored() {
        return restored;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getXidValue() {
        return String.format(
                "%s:%s:%s",
                getFormatId(),
                byteToHexString(getGlobalTransactionId()),
                byteToHexString(getBranchQualifier()));
    }

    @Override
    public int getFormatId() {
        return FORMAT_ID;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        try {
            // globalTransactionId = job id + task index
            final DataOutputSerializer out = new DataOutputSerializer(1);
            out.write(jobId, 0, JobID.SIZE);
            out.writeInt(subtaskId);
            return out.getSharedBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public byte[] getBranchQualifier() {
        Preconditions.checkArgument(checkpointId >= 0, "No branch was initialized");
        try {
            // branchQualifier = numberOfSubtasks + checkpoint id
            final DataOutputSerializer out = new DataOutputSerializer(1);
            out.writeInt(numberOfSubtasks);
            out.writeLong(checkpointId);
            return out.getSharedBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public byte[] serialize() {
        try {
            final DataOutputSerializer out = new DataOutputSerializer(1);
            out.write(jobId, 0, JobID.SIZE);
            out.writeInt(subtaskId);
            out.writeInt(numberOfSubtasks);
            out.writeLong(checkpointId);
            out.writeInt(attempts);
            return out.getSharedBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public boolean belongsTo(Xid xid) {
        if (!(xid instanceof TransactionId)) {
            return false;
        }

        TransactionId tXid = (TransactionId) xid;

        if (FORMAT_ID != tXid.getFormatId() || !Arrays.equals(jobId, tXid.jobId)) {
            return false;
        }

        if (subtaskId == tXid.subtaskId && numberOfSubtasks == tXid.numberOfSubtasks) {
            return true;
        }

        // Check if was a job downgrade
        return numberOfSubtasks < tXid.subtaskId;
    }

    @Override
    public boolean equals(Object other) {
        return equals(other, false);
    }

    public boolean equals(Object other, boolean withInternal) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TransactionId that = (TransactionId) other;
        EqualsBuilder builder =
                new EqualsBuilder()
                        .append(jobId, that.jobId)
                        .append(subtaskId, that.subtaskId)
                        .append(numberOfSubtasks, that.numberOfSubtasks)
                        .append(checkpointId, that.checkpointId);

        if (withInternal) {
            builder.append(attempts, that.attempts).append(restored, that.restored);
        }

        return builder.isEquals();
    }

    @Override
    public int hashCode() {
        return hashCode(false);
    }

    public int hashCode(boolean withInternal) {
        HashCodeBuilder builder =
                new HashCodeBuilder(17, 37)
                        .append(jobId)
                        .append(subtaskId)
                        .append(numberOfSubtasks)
                        .append(checkpointId);

        if (withInternal) {
            builder.append(attempts).append(restored);
        }

        return builder.toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("jobId", jobId)
                .append("subtaskId", subtaskId)
                .append("numberOfSubtasks", numberOfSubtasks)
                .append("checkpointId", checkpointId)
                .append("attempts", attempts)
                .append("restored", restored)
                .toString();
    }
}
