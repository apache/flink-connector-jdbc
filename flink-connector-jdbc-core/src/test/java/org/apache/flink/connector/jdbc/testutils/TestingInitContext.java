/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.OptionalLong;

/**
 * A {@link WriterInitContext} for sink writers under test. The job, the task, the restored
 * checkpoint id, the input serializer, the object reuse flag and the metric group are real; the
 * mailbox executor, the processing time service and the serialization schema context are null, the
 * JDBC writer uses none of them.
 */
public class TestingInitContext implements WriterInitContext {

    private final JobInfo jobInfo;
    private final TaskInfo taskInfo;
    private final TypeSerializer<?> inputSerializer;
    private final OptionalLong restoredCheckpointId;
    private final boolean objectReuseEnabled;
    // not UnregisteredMetricsGroup.createSinkWriterMetricGroup(): that one returns a new counter
    // on every call, so what the writer increments could never be read back
    private final SinkWriterMetricGroup metricGroup =
            InternalSinkWriterMetricGroup.wrap(
                    UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup());

    /** The context of a fresh writer in a job with a single subtask. */
    public TestingInitContext(TypeSerializer<?> inputSerializer) {
        this(
                JobID.generate(),
                "test_job",
                new TaskInfoImpl("test_task", 1, 0, 1, 0),
                inputSerializer);
    }

    public TestingInitContext(
            JobID jobId, String jobName, TaskInfo taskInfo, TypeSerializer<?> inputSerializer) {
        this(
                new JobInfoImpl(jobId, jobName),
                taskInfo,
                inputSerializer,
                OptionalLong.empty(),
                false);
    }

    private TestingInitContext(
            JobInfo jobInfo,
            TaskInfo taskInfo,
            TypeSerializer<?> inputSerializer,
            OptionalLong restoredCheckpointId,
            boolean objectReuseEnabled) {
        this.jobInfo = jobInfo;
        this.taskInfo = taskInfo;
        this.inputSerializer = inputSerializer;
        this.restoredCheckpointId = restoredCheckpointId;
        this.objectReuseEnabled = objectReuseEnabled;
    }

    /** The same job and task, for a writer restored from checkpoint {@code checkpointId}. */
    public TestingInitContext withRestoredCheckpointId(long checkpointId) {
        return new TestingInitContext(
                jobInfo,
                taskInfo,
                inputSerializer,
                OptionalLong.of(checkpointId),
                objectReuseEnabled);
    }

    /** The same job and task, with object reuse enabled so the writer copies every record. */
    public TestingInitContext withObjectReuse() {
        return new TestingInitContext(
                jobInfo, taskInfo, inputSerializer, restoredCheckpointId, true);
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return restoredCheckpointId;
    }

    @Override
    public JobInfo getJobInfo() {
        return jobInfo;
    }

    @Override
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return null;
    }

    @Override
    public ProcessingTimeService getProcessingTimeService() {
        return null;
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return metricGroup;
    }

    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return null;
    }

    @Override
    public boolean isObjectReuseEnabled() {
        return objectReuseEnabled;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <IN> TypeSerializer<IN> createInputSerializer() {
        return (TypeSerializer<IN>) inputSerializer;
    }
}
