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
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

import static java.lang.String.format;

/**
 * Elasticsearch REST API client.
 */
public class ElasticsearchRestClient {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final String host;
    private final int port;
    private final String username;
    private final String password;

    public ElasticsearchRestClient(ElasticsearchMetadata metadata) {
        this(metadata.getContainerHost(),
                metadata.getContainerPort(),
                metadata.getUsername(),
                metadata.getPassword());
    }

    public ElasticsearchRestClient(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public boolean trialEnabled() throws Exception {
        Request request = requestWithAuthorization()
                .url(format("http://%s:%d/_license", host, port))
                .get()
                .build();
        ElasticLicenseResponse response = executeRequest(request, ElasticLicenseResponse.class);
        return response != null && response.license.status.equals("active") && response.license.type.equals("trial");
    }

    public void enableTrial() throws Exception {
        Request request = requestWithAuthorization()
                .url(format("http://%s:%d/_license/start_trial?acknowledge=true", host, port))
                .post(RequestBody.create(new byte[]{}))
                .build();
        executeRequest(request);
    }

    public void createIndex(String indexName, String indexDefinition) throws Exception {
        Request request = requestWithAuthorization()
                .url(format("http://%s:%d/%s/", host, port, indexName))
                .put(RequestBody.create(indexDefinition, MediaType.get("application/json")))
                .build();
        executeRequest(request);
    }

    public void deleteIndex(String indexName) throws Exception {
        Request request = requestWithAuthorization()
                .url(format("http://%s:%d/%s/", host, port, indexName))
                .delete()
                .build();
        executeRequest(request);
    }

    public void addDataBulk(String indexName, String content) throws Exception {
        Request request = requestWithAuthorization()
                .url(format("http://%s:%d/%s/_bulk?refresh=true", host, port, indexName))
                .post(RequestBody.create(content, MediaType.get("application/json")))
                .build();
        executeRequest(request);
    }

    private <T> T executeRequest(Request request, Class<T> outputClass) throws IOException {
        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            ResponseBody body = response.body();
            Assertions.assertTrue(response.isSuccessful());
            Assertions.assertNotNull(body);
            return OBJECT_MAPPER.readValue(body.string(), outputClass);
        }
    }

    private void executeRequest(Request request) throws IOException {
        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            Assertions.assertTrue(response.isSuccessful());
        }
    }

    private Request.Builder requestWithAuthorization() {
        return new Request.Builder().addHeader("Authorization", Credentials.basic(username, password));
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
