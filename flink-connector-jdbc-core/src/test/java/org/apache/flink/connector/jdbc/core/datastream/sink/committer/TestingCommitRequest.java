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

package org.apache.flink.connector.jdbc.core.datastream.sink.committer;

import org.apache.flink.api.connector.sink2.Committer;

/** A {@link Committer.CommitRequest} that records how the committer answered it. */
public class TestingCommitRequest implements Committer.CommitRequest<JdbcCommitable> {

    private final JdbcCommitable committable;
    private int numberOfRetries;
    private int retryLaterCalls;
    private Throwable failure;

    public TestingCommitRequest(JdbcCommitable committable) {
        this.committable = committable;
    }

    @Override
    public JdbcCommitable getCommittable() {
        return committable;
    }

    @Override
    public int getNumberOfRetries() {
        return numberOfRetries;
    }

    @Override
    public void signalFailedWithKnownReason(Throwable t) {
        failure = t;
    }

    /** Flink's own request throws here and fails the job; this one records it. */
    @Override
    public void signalFailedWithUnknownReason(Throwable t) {
        failure = t;
    }

    @Override
    public void retryLater() {
        retryLaterCalls++;
        numberOfRetries++;
    }

    @Override
    public void updateAndRetryLater(JdbcCommitable committable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void signalAlreadyCommitted() {}

    public int getRetryLaterCalls() {
        return retryLaterCalls;
    }

    public Throwable getFailure() {
        return failure;
    }
}
