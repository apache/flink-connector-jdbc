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

package org.apache.flink.connector.jdbc.spanner.database.catalog;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.connection.ConnectionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link SpannerCatalog} that do not require a Spanner instance. */
class SpannerCatalogTest {

    private static final String BASE_URL =
            "jdbc:cloudspanner://localhost:9010/projects/p/instances/i/databases/"
                    + ";autoConfigEmulator=true";

    @Test
    void testDefaultDatabase() {
        SpannerCatalog catalog = createCatalog("db");

        assertThat(catalog.getDefaultDatabase()).isEqualTo("db");
    }

    @Test
    void testMissingDefaultDatabase() {
        assertThatThrownBy(() -> createCatalog(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Please set 'default-database' for the Spanner catalog.");
    }

    @Test
    void testSpannerOptionsUseCredentialsFileFromBaseUrl(@TempDir Path tempDir) throws Exception {
        Path credentialsFile = tempDir.resolve("service-account.json");
        Files.write(credentialsFile, createServiceAccountJson().getBytes(StandardCharsets.UTF_8));

        ConnectionOptions options = createConnectionOptions(";credentials=" + credentialsFile);
        SpannerOptions spannerOptions = SpannerCatalog.getSpannerOptions(options);

        assertThat(options.getCredentials()).isInstanceOf(ServiceAccountCredentials.class);
        assertThat(spannerOptions.getCredentials()).isSameAs(options.getCredentials());
    }

    @Test
    void testSpannerOptionsUseOAuthTokenFromBaseUrl() {
        ConnectionOptions options = createConnectionOptions(";oauthToken=test-token");
        SpannerOptions spannerOptions = SpannerCatalog.getSpannerOptions(options);

        assertThat(spannerOptions.getCredentials()).isSameAs(options.getCredentials());
    }

    private static ConnectionOptions createConnectionOptions(String params) {
        return ConnectionOptions.newBuilder()
                .setUri(
                        "cloudspanner://spanner.googleapis.com/projects/p/instances/i/databases/db"
                                + params)
                .build();
    }

    /** Creates a dummy service account key file content with a freshly generated RSA key. */
    private static String createServiceAccountJson() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        String privateKey =
                "-----BEGIN PRIVATE KEY-----\\n"
                        + Base64.getEncoder()
                                .encodeToString(
                                        generator.generateKeyPair().getPrivate().getEncoded())
                        + "\\n-----END PRIVATE KEY-----\\n";
        return "{"
                + "\"type\": \"service_account\","
                + "\"project_id\": \"p\","
                + "\"private_key_id\": \"test-key-id\","
                + "\"private_key\": \""
                + privateKey
                + "\","
                + "\"client_email\": \"test@p.iam.gserviceaccount.com\","
                + "\"client_id\": \"1234567890\","
                + "\"token_uri\": \"https://oauth2.googleapis.com/token\""
                + "}";
    }

    private static SpannerCatalog createCatalog(String defaultDatabase) {
        return new SpannerCatalog(
                Thread.currentThread().getContextClassLoader(),
                "catalog",
                defaultDatabase,
                BASE_URL,
                new Properties());
    }
}
