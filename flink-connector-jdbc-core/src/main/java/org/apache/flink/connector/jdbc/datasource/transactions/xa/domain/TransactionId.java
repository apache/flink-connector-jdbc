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

import javax.annotation.Nullable;
import javax.transaction.xa.Xid;

import java.io.IOException;
import java.io.Serializable;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.util.StringUtils.byteToHexString;

/** A simple {@link Xid} implementation. */
@Internal
public class TransactionId implements Xid, Serializable {

    private static final long serialVersionUID = 1L;

    private static final int FORMAT_ID = 202;

    // the global transaction id starts with the prefix
    private static final int PREFIXED_FORMAT_ID = 203;

    /** The bytes left for the prefix in a global transaction id, after its length, job and task. */
    public static final int MAX_PREFIX_BYTES = Xid.MAXGTRIDSIZE - 1 - JobID.SIZE - Integer.BYTES;

    @Nullable private final byte[] prefix;
    private final byte[] jobId;
    private final int subtaskId;
    private final int numberOfSubtasks;
    private final long checkpointId;
    private final int attempts;
    private final boolean restored;

    private TransactionId(
            @Nullable byte[] prefix,
            byte[] jobId,
            int subtaskId,
            int numberOfSubtasks,
            long checkpointId,
            int attempts,
            boolean restored) {
        this.prefix = prefix;
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
        return create(null, jobId, subtaskId, numberOfSubtasks);
    }

    public static TransactionId create(
            @Nullable String prefix, byte[] jobId, int subtaskId, int numberOfSubtasks) {
        byte[] prefixBytes = prefix == null ? null : prefix.getBytes(StandardCharsets.UTF_8);
        Preconditions.checkArgument(
                prefixBytes == null || prefixBytes.length <= MAX_PREFIX_BYTES,
                "transactionIdPrefix must be at most %s bytes in UTF-8",
                MAX_PREFIX_BYTES);
        return new TransactionId(prefixBytes, jobId, subtaskId, numberOfSubtasks, -1, 0, false);
    }

    public static TransactionId restore(
            byte[] jobId, int subtaskId, int numberOfSubtasks, long checkpointId, int attempts) {
        return new TransactionId(
                null, jobId, subtaskId, numberOfSubtasks, checkpointId, attempts, true);
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
        if (PREFIXED_FORMAT_ID == formatId) {
            return fromPrefixedXid(globalTransactionId, branchQualifier, restored);
        }
        if (FORMAT_ID != formatId) {
            throw new IOException(String.format("Xid formatId (%s) is not valid", formatId));
        }

        final DataInputDeserializer gid = new DataInputDeserializer(globalTransactionId);
        byte[] jobIdBytes = readJobId(gid);
        int subtaskId = gid.readInt();

        final DataInputDeserializer branch = new DataInputDeserializer(branchQualifier);
        int numberOfSubtasks = branch.readInt();
        long checkpoint = branch.readLong();

        return new TransactionId(
                null, jobIdBytes, subtaskId, numberOfSubtasks, checkpoint, 0, restored);
    }

    private static TransactionId fromPrefixedXid(
            byte[] globalTransactionId, byte[] branchQualifier, boolean restored)
            throws IOException {
        try {
            ByteBuffer gid = ByteBuffer.wrap(globalTransactionId);
            byte[] prefix = new byte[Byte.toUnsignedInt(gid.get())];
            gid.get(prefix);
            byte[] jobIdBytes = new byte[JobID.SIZE];
            gid.get(jobIdBytes);
            int subtaskId = gid.getInt();

            ByteBuffer branch = ByteBuffer.wrap(branchQualifier);
            int numberOfSubtasks = branch.getInt();
            long checkpoint = branch.getLong();
            if (gid.hasRemaining() || branch.hasRemaining()) {
                throw new IOException("Xid is too long");
            }

            return new TransactionId(
                    prefix, jobIdBytes, subtaskId, numberOfSubtasks, checkpoint, 0, restored);
        } catch (BufferUnderflowException e) {
            throw new IOException("Xid is too short", e);
        }
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
                prefix, jobId, subtaskId, numberOfSubtasks, checkpointId, attempts, restored);
    }

    public TransactionId withBranch(long checkpointId) {
        return new TransactionId(
                prefix, jobId, subtaskId, numberOfSubtasks, checkpointId, attempts, restored);
    }

    public TransactionId withAttemptsIncremented() {
        return new TransactionId(
                prefix, jobId, subtaskId, numberOfSubtasks, checkpointId, attempts + 1, restored);
    }

    /** For the writer state, which stores the prefix next to what {@link #serialize()} writes. */
    public TransactionId withPrefix(@Nullable byte[] prefix) {
        return new TransactionId(
                prefix, jobId, subtaskId, numberOfSubtasks, checkpointId, attempts, restored);
    }

    @Nullable
    public byte[] getPrefixBytes() {
        return prefix;
    }

    public Optional<String> getPrefix() {
        return Optional.ofNullable(prefix).map(p -> new String(p, StandardCharsets.UTF_8));
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
        return prefix == null ? FORMAT_ID : PREFIXED_FORMAT_ID;
    }

    /**
     * Without a prefix the ids keep the padding of the shared buffer, as transactions prepared
     * before the prefix was added must still be found by their bytes.
     */
    @Override
    public byte[] getGlobalTransactionId() {
        if (prefix != null) {
            // globalTransactionId = prefix length + prefix + job id + task index
            return ByteBuffer.allocate(1 + prefix.length + JobID.SIZE + Integer.BYTES)
                    .put((byte) prefix.length)
                    .put(prefix)
                    .put(jobId, 0, JobID.SIZE)
                    .putInt(subtaskId)
                    .array();
        }
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
        if (prefix != null) {
            return ByteBuffer.allocate(Integer.BYTES + Long.BYTES)
                    .putInt(numberOfSubtasks)
                    .putLong(checkpointId)
                    .array();
        }
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

    /** Writes all but the prefix. */
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
                        .append(prefix, that.prefix)
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
                        .append(prefix)
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
                .append("prefix", getPrefix().orElse(null))
                .append("jobId", jobId)
                .append("subtaskId", subtaskId)
                .append("numberOfSubtasks", numberOfSubtasks)
                .append("checkpointId", checkpointId)
                .append("attempts", attempts)
                .append("restored", restored)
                .toString();
    }
}
