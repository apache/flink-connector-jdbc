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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.TestingSplitsChange;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link JdbcSourceSplitReader}. */
class JdbcSourceSplitReaderTest extends JdbcDataTestBase {

    private final JdbcSourceSplit split =
            new JdbcSourceSplit(
                    "1", "select id, title, author, price, qty from " + INPUT_TABLE, null, 0, null);
    private final JdbcConnectionProvider connectionProvider =
            new SimpleJdbcConnectionProvider(
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl(getMetadata().getJdbcUrl())
                            .withDriverName(getMetadata().getDriverClass())
                            .build());

    @Test
    void testGetProducedType() {
        final TestingReaderContext context = new TestingReaderContext();
        TypeInformation<String> stringTypeInformation = TypeInformation.of(String.class);
        JdbcSourceSplitReader<String> splitReader =
                new JdbcSourceSplitReader<>(
                        context,
                        new Configuration(),
                        stringTypeInformation,
                        connectionProvider,
                        DeliveryGuarantee.NONE,
                        (ResultExtractor<String>) resultSet -> resultSet.getString(0));
        assertThat(splitReader.getProducedType()).isSameAs(stringTypeInformation);
    }

    @Test
    void testClose() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        TypeInformation<TestEntry> typeInformation = TypeInformation.of(TestEntry.class);
        JdbcSourceSplitReader<TestEntry> splitReader =
                new JdbcSourceSplitReader<>(
                        context,
                        new Configuration(),
                        typeInformation,
                        connectionProvider,
                        DeliveryGuarantee.NONE,
                        extractor);
        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
        splitReader.fetch();
        splitReader.close();
        assertThat(splitReader.getConnection()).isNull();
        assertThat(splitReader.getStatement()).isNull();
        assertThat(splitReader.getResultSet()).isNull();
    }

    @Test
    void testHandleSplitsChanges() {
        final TestingReaderContext context = new TestingReaderContext();
        TypeInformation<String> stringTypeInformation = TypeInformation.of(String.class);
        JdbcSourceSplitReader<String> splitReader =
                new JdbcSourceSplitReader<>(
                        context,
                        new Configuration(),
                        stringTypeInformation,
                        connectionProvider,
                        DeliveryGuarantee.NONE,
                        (ResultExtractor<String>) resultSet -> resultSet.getString(0));
        assertThatThrownBy(
                        () ->
                                splitReader.handleSplitsChanges(
                                        new TestingSplitsChange(Collections.emptyList())))
                .isInstanceOf(UnsupportedOperationException.class);

        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
        assertThat(splitReader.getSplits()).isEqualTo(Collections.singletonList(split));
    }

    @Test
    void testFetch() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        TypeInformation<TestEntry> typeInformation = TypeInformation.of(TestEntry.class);
        JdbcSourceSplitReader<TestEntry> splitReader =
                new JdbcSourceSplitReader<>(
                        context,
                        new Configuration(),
                        typeInformation,
                        connectionProvider,
                        DeliveryGuarantee.NONE,
                        extractor);
        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
        RecordsWithSplitIds<RecordAndOffset<TestEntry>> fetchedRecordsWithSplitIds =
                splitReader.fetch();
        assertThat(fetchedRecordsWithSplitIds.nextSplit()).isEqualTo("1");
        RecordAndOffset<TestEntry> testEntryRecordAndOffset =
                fetchedRecordsWithSplitIds.nextRecordFromSplit();
        List<TestEntry> records = new ArrayList<>();
        while (testEntryRecordAndOffset != null) {
            records.add(testEntryRecordAndOffset.record);
            testEntryRecordAndOffset = fetchedRecordsWithSplitIds.nextRecordFromSplit();
        }
        assertThat(records).hasSize(TEST_DATA.length);
        assertThat(fetchedRecordsWithSplitIds.nextSplit()).isNull();
        splitReader.close();
    }
}
