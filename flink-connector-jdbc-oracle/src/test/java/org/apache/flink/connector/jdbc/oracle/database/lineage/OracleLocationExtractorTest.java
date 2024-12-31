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

package org.apache.flink.connector.jdbc.oracle.database.lineage;

import org.apache.flink.connector.jdbc.lineage.JdbcLocation;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** The Oracle params for {@link OracleLocationExtractor}. */
public class OracleLocationExtractorTest {
    private static final String THIN_URL = "oracle:thin:HR/hr@//10.1.0.0:5221/orcl";
    private static final String OCI_URL = "oracle:oci:HR/hr@//10.1.0.0:5221/orcl";
    private static final String KPRB_URL = "oracle:kprb:HR/hr@//10.1.0.0:5221/orcl";

    private final OracleLocationExtractor extractor = new OracleLocationExtractor();

    public static Collection<String> parameters() {
        return Arrays.asList(THIN_URL, OCI_URL, KPRB_URL);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testURL(String url) throws Exception {
        assertThat(extractor.isDefinedAt(url)).isTrue();
        verify(extractor.extract(url, new Properties()));
    }

    private void verify(JdbcLocation jdbcLocation) {
        assertThat(jdbcLocation.getScheme()).isEqualTo("oracle");
        assertThat(jdbcLocation.getAuthority()).isEqualTo(Optional.of("10.1.0.0:5221"));
        assertThat(jdbcLocation.getInstance()).isEqualTo(Optional.empty());
        assertThat(jdbcLocation.getDatabase()).isEqualTo(Optional.of("orcl"));
    }
}
