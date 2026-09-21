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
import org.apache.flink.api.connector.sink2.InitContext;

import java.util.OptionalLong;

/** An {@link InitContext} carrying a fixed job and task, for sink writers under test. */
public class TestingInitContext implements InitContext {

    private final JobInfo jobInfo;
    private final TaskInfo taskInfo;

    public TestingInitContext(JobID jobId, String jobName, TaskInfo taskInfo) {
        this.jobInfo = new JobInfoImpl(jobId, jobName);
        this.taskInfo = taskInfo;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return OptionalLong.empty();
    }

    @Override
    public JobInfo getJobInfo() {
        return jobInfo;
    }

    @Override
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }
}
