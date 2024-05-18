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

package org.apache.flink.connector.jdbc.testutils.databases.oracle;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.util.FlinkRuntimeException;

import org.testcontainers.containers.OracleContainer;

/** A Oracle database for testing. */
public class OracleDatabase extends DatabaseExtension implements OracleImages {

    private static final OracleContainer CONTAINER =
            new OracleContainer(ORACLE_21)
                    .withStartupTimeoutSeconds(240)
                    .withConnectTimeoutSeconds(120)
                    .usingSid();

    private static OracleMetadata metadata;

    public static OracleMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new OracleMetadata(CONTAINER, true);
        }
        return metadata;
    }

    @Override
    protected DatabaseMetadata startDatabase() throws Exception {
        CONTAINER.start();
        return getMetadata();
    }

    @Override
    protected void stopDatabase() throws Exception {
        CONTAINER.stop();
        metadata = null;
    }
}
