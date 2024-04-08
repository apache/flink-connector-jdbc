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

package org.apache.flink.connector.jdbc.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link JdbcSourceReader}. */
class JdbcSourceReaderTest extends JdbcDataTestBase {

    @Test
    void testRequestSplitWhenNoSplitRestored() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final JdbcSourceReader<String> reader = createReader(context);
        reader.start();
        reader.close();
        assertThat(context.getNumSplitRequests()).isEqualTo(1);
    }

    @Test
    void testNoSplitRequestWhenSplitRestored() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final JdbcSourceReader<String> reader = createReader(context);
        reader.addSplits(
                Collections.singletonList(new JdbcSourceSplit("1", "select 1", null, 0, null)));
        reader.start();
        reader.close();
        assertThat(context.getNumSplitRequests()).isEqualTo(0);
    }

    private JdbcSourceReader<String> createReader(TestingReaderContext context) {
        Configuration configuration = new Configuration();
        JdbcConnectionOptions connectionOptions =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(getMetadata().getJdbcUrl())
                        .withDriverName(getMetadata().getDriverClass())
                        .build();
        return new JdbcSourceReader<>(
                () ->
                        new JdbcSourceSplitReader<>(
                                context,
                                configuration,
                                TypeInformation.of(String.class),
                                new SimpleJdbcConnectionProvider(connectionOptions),
                                DeliveryGuarantee.NONE,
                                (ResultExtractor<String>) resultSet -> resultSet.getString(0)),
                configuration,
                context);
    }
}
