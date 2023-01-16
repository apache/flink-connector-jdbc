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

package org.apache.flink.connector.jdbc.dialect.elastic;

import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/** {@link ElasticsearchTestContainer}. */
public class ElasticsearchTestContainer extends ElasticsearchContainer {
    private static final String IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";
    private static final String DEFAULT_TAG = "8.6.0";
    private static final DockerImageName ELASTIC_IMAGE =
            DockerImageName.parse(IMAGE).withTag(DEFAULT_TAG);

    private static final int ELASTIC_PORT = 9200;

    public ElasticsearchTestContainer() {
        super(ELASTIC_IMAGE);
        preconfigure();
    }

    private void preconfigure() {
        addExposedPorts(ELASTIC_PORT);
    }

    public String getDriverClassName() {
        return "org.elasticsearch.xpack.sql.jdbc.EsDriver";
    }

    public String getJdbcUrl() {
        return "jdbc:elasticsearch://" + getHost() + ":" + getElasticPort();
    }

    public Integer getElasticPort() {
        return getMappedPort(ELASTIC_PORT);
    }
}
