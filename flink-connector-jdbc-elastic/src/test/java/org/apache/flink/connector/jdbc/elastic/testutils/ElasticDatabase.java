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

package org.apache.flink.connector.jdbc.elastic.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseExtension;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseResource;
import org.apache.flink.connector.jdbc.testutils.resources.DockerResource;
import org.apache.flink.util.FlinkRuntimeException;

import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.time.Duration;

import static org.apache.flink.connector.jdbc.elastic.testutils.ElasticMetadata.PASSWORD;

/** An Elastic database for testing. */
public class ElasticDatabase extends DatabaseExtension implements ElasticImages {

    private static final ElasticsearchContainer CONTAINER =
            new TestElasticsearchContainer(ELASTICSEARCH_8)
                    .waitingFor(
                            Wait.forLogMessage(
                                            ".*Node .* is selected as the current health node.*", 1)
                                    .withStartupTimeout(Duration.ofMinutes(5)));

    private static ElasticMetadata metadata;
    private static ElasticRestClient client;

    public static ElasticMetadata getMetadata() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (metadata == null) {
            metadata = new ElasticMetadata(CONTAINER);
        }
        return metadata;
    }

    @Override
    protected DatabaseMetadata getMetadataDB() {
        return getMetadata();
    }

    @Override
    protected DatabaseResource getResource() {
        return new DockerResource(CONTAINER);
    }

    private static ElasticRestClient getClient() {
        if (!CONTAINER.isRunning()) {
            throw new FlinkRuntimeException("Container is stopped.");
        }
        if (client == null) {
            client = new ElasticRestClient(getMetadata());
        }
        return client;
    }

    /** Custom wrapper for {@link ElasticsearchContainer}. */
    public static class TestElasticsearchContainer extends ElasticsearchContainer {

        public TestElasticsearchContainer(String dockerImageName) {
            super(dockerImageName);
        }

        @Override
        public void start() {
            CONTAINER.withEnv("xpack.security.enabled", "true");
            CONTAINER.withEnv("ELASTIC_PASSWORD", PASSWORD);
            CONTAINER.withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g");

            super.start();

            // JDBC plugin is available only in Platinum and Enterprise licenses or in trial.
            try {
                if (!getClient().trialEnabled()) {
                    getClient().enableTrial();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void stop() {
            super.stop();
            client = null;
        }
    }
}
