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

package org.apache.flink.connector.jdbc.testutils.resources;

import org.apache.flink.connector.jdbc.testutils.DatabaseResource;

import com.github.dockerjava.api.DockerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.Arrays;

/** Docker based database resource. */
public class DockerResource implements DatabaseResource {

    protected static final Logger LOG = LoggerFactory.getLogger(DockerResource.class);

    private final JdbcDatabaseContainer<?> container;

    public DockerResource(JdbcDatabaseContainer<?> container) {
        this.container = container;
    }

    @Override
    public void start() {
        this.container.start();
    }

    @Override
    public void stop() {
        this.container.stop();
    }

    @Override
    public void close() throws Throwable {
        stop();
        cleanContainers(container);
    }

    public static void cleanContainers(GenericContainer<?> container) {
        try {
            DockerClient client = DockerClientFactory.instance().client();
            //            client.removeImageCmd(container.getDockerImageName()).exec();
            client.listImagesCmd().exec().stream()
                    .filter(
                            image ->
                                    Arrays.stream(image.getRepoTags())
                                            .anyMatch(
                                                    tag ->
                                                            !tag.contains("testcontainers/ryuk")
                                                                    && !tag.contains(
                                                                            container
                                                                                    .getDockerImageName())))
                    .forEach(image -> client.removeImageCmd(image.getId()).exec());

        } catch (Exception ignore) {
            LOG.warn("Error deleting image.");
        }
    }
}
