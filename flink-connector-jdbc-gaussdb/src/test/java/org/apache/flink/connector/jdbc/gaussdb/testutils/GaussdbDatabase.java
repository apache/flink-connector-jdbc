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

package org.apache.flink.connector.jdbc.gaussdb.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseResource;
import org.apache.flink.connector.jdbc.testutils.resources.DockerResource;
import org.apache.flink.util.FlinkRuntimeException;

import org.testcontainers.utility.DockerImageName;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A Gaussdb database for testing.
 *
 * <p>Notes: The source code is based on PostgresDatabase.
 */
public class GaussdbDatabase extends DatabaseExtension implements GaussdbImages {

    private static final GaussDBContainer<?> CONTAINER =
            new GaussdbXaContainer(IMAGE).withMaxConnections(10).withMaxTransactions(50);

    private static final DockerResource RESOURCE = new DockerResource(CONTAINER);

    private static GaussdbMetadata metadata;

    public static GaussdbMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new GaussdbMetadata(CONTAINER, true);
        }
        return metadata;
    }

    protected DatabaseMetadata getMetadataDB() {
        return getMetadata();
    }

    @Override
    protected DatabaseResource getResource() {
        return RESOURCE;
    }

    /** {@link GaussDBContainer} with XA enabled (by setting max_prepared_transactions). */
    public static class GaussdbXaContainer extends GaussDBContainer<GaussdbXaContainer> {
        private static final int SUPERUSER_RESERVED_CONNECTIONS = 1;
        private volatile boolean started = false;

        public GaussdbXaContainer(String dockerImageName) {
            super(DockerImageName.parse(dockerImageName));
        }

        public GaussdbXaContainer withMaxConnections(int maxConnections) {
            checkArgument(
                    maxConnections > SUPERUSER_RESERVED_CONNECTIONS,
                    "maxConnections should be greater than superuser_reserved_connections");
            return this.self();
        }

        public GaussdbXaContainer withMaxTransactions(int maxTransactions) {
            checkArgument(maxTransactions > 1, "maxTransactions should be greater 1");
            return this.self();
        }

        @Override
        public void start() {
            synchronized (this) {
                if (!started) {
                    super.start();
                    started = true;
                }
            }
        }
    }
}
