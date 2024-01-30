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

package org.apache.flink.connector.jdbc;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.databases.derby.DerbyTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import static org.mockito.Mockito.doReturn;

/**
 * Base class for JDBC test using DDL from {@link JdbcTestFixture}. It uses create tables before
 * each test and drops afterward.
 */
public abstract class JdbcTestBase implements DerbyTestBase {

    @BeforeEach
    public void before() throws Exception {
        JdbcTestFixture.initSchema(getMetadata());
    }

    @AfterEach
    public void after() throws Exception {
        JdbcTestFixture.cleanUpDatabasesStatic(getMetadata());
    }

    public static RuntimeContext getRuntimeContext(Boolean reused) {
        ExecutionConfig config = getExecutionConfig(reused);
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        doReturn(config).when(context).getExecutionConfig();
        return context;
    }

    public static RuntimeContext getRuntimeContext(JobID jobId) {
        RuntimeContext context = getRuntimeContext(false);
        doReturn(jobId).when(context).getJobId();
        return context;
    }

    public static ExecutionConfig getExecutionConfig(boolean reused) {
        ExecutionConfig config = new ExecutionConfig();
        if (reused) {
            config.enableObjectReuse();
        } else {
            config.disableObjectReuse();
        }
        return config;
    }

    public static <T> TypeSerializer<T> getSerializer(
            TypeInformation<T> type, Boolean objectReused) {
        return type.createSerializer(getExecutionConfig(objectReused));
    }
}
