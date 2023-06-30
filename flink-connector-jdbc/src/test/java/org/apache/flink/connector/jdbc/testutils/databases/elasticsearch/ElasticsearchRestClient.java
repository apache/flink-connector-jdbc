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

package org.apache.flink.connector.jdbc.testutils.databases.elasticsearch;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

import static java.lang.String.format;

/** Elasticsearch REST API client. */
public class ElasticsearchRestClient {

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final RestClient restClient;

    public ElasticsearchRestClient(ElasticsearchMetadata metadata) {
        this(
                metadata.getContainerHost(),
                metadata.getContainerPort(),
                metadata.getUsername(),
                metadata.getPassword());
    }

    public ElasticsearchRestClient(String host, int port, String username, String password) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        this.restClient =
                RestClient.builder(new HttpHost(host, port, "http"))
                        .setHttpClientConfigCallback(
                                builder ->
                                        builder.setDefaultCredentialsProvider(credentialsProvider))
                        .build();
    }

    public boolean trialEnabled() throws Exception {
        Request request = new Request("GET", "/_license");
        ElasticLicenseResponse response = executeRequest(request, ElasticLicenseResponse.class);
        return response != null
                && response.license.status.equals("active")
                && response.license.type.equals("trial");
    }

    public void enableTrial() throws Exception {
        executeRequest(new Request("POST", "/_license/start_trial?acknowledge=true"));
    }

    public void createIndex(String indexName, String indexDefinition) throws Exception {
        Request request = new Request("PUT", format("/%s/", indexName));
        request.setJsonEntity(indexDefinition);
        executeRequest(request);
    }

    public void deleteIndex(String indexName) throws Exception {
        executeRequest(new Request("DELETE", format("/%s/", indexName)));
    }

    public void addDataBulk(String indexName, String content) throws Exception {
        Request request = new Request("PUT", format("/%s/_bulk?refresh=true", indexName));
        request.setJsonEntity(content);
        executeRequest(request);
    }

    private <T> T executeRequest(Request request, Class<T> outputClass) throws IOException {
        org.elasticsearch.client.Response response = restClient.performRequest(request);
        Assertions.assertEquals(200, response.getStatusLine().getStatusCode());
        return OBJECT_MAPPER.readValue(EntityUtils.toString(response.getEntity()), outputClass);
    }

    private void executeRequest(Request request) throws IOException {
        org.elasticsearch.client.Response response = restClient.performRequest(request);
        Assertions.assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private static class ElasticLicenseResponse {
        private static class License {
            private String status;
            private String type;

            public String getStatus() {
                return status;
            }

            public void setStatus(String status) {
                this.status = status;
            }

            public String getType() {
                return type;
            }

            public void setType(String type) {
                this.type = type;
            }
        }

        private License license;

        public License getLicense() {
            return license;
        }

        public void setLicense(License license) {
            this.license = license;
        }
    }
}
