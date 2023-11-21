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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;

import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.connector.jdbc.xa.JdbcXaSinkTestBase.TEST_RUNTIME_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

/** Simple uniqueness tests for the {@link SemanticXidGenerator}. */
class SemanticXidGeneratorTest {
    private static final int COUNT = 100_000;

    @Test
    void testXidsUniqueAmongCheckpoints() {
        SemanticXidGenerator xidGenerator = new SemanticXidGenerator();
        xidGenerator.open();
        checkUniqueness(
                checkpoint ->
                        xidGenerator.generateXid(JobSubtask.of(TEST_RUNTIME_CONTEXT), checkpoint));
    }

    @Test
    void testXidsUniqueAmongJobs() {
        long checkpointId = 1L;
        SemanticXidGenerator generator = new SemanticXidGenerator();
        checkUniqueness(
                unused -> {
                    generator.open();
                    RuntimeContext context = JdbcXaSinkTestBase.getRuntimeContext(new JobID());
                    return generator.generateXid(JobSubtask.of(context), checkpointId);
                });
    }

    private void checkUniqueness(Function<Integer, Xid> generate) {
        Set<Xid> generated = new HashSet<>();
        for (int i = 0; i < COUNT; i++) {
            // We "drop" the branch id because uniqueness of gtrid is important
            generated.add(new XidImpl(0, generate.apply(i).getGlobalTransactionId(), new byte[0]));
        }
        assertThat(generated).hasSize(COUNT);
    }
}
