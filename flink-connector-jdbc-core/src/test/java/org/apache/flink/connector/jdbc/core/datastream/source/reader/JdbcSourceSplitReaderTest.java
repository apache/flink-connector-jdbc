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

package org.apache.flink.connector.jdbc.core.datastream.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.TestingSplitsChange;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.core.datastream.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link
 * org.apache.flink.connector.jdbc.core.datastream.source.reader.JdbcSourceSplitReader}.
 */
class JdbcSourceSplitReaderTest extends JdbcDataTestBase {

    private final JdbcSourceSplit split =
            new JdbcSourceSplit(
                    "1", "select id, title, author, price, qty from " + INPUT_TABLE, null, null);
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

    @Test
    void testFetchReconnectsWhenConnectionClosedWhileOpeningSplit() throws Exception {
        // The provider hands back an already-closed connection the first time, so the reader's
        // first use of it fails (mimicking the connection being torn down, e.g. on source
        // cancellation, before the reader finishes opening the split). The reader must
        // re-establish the connection and read the whole split.
        CountingConnectionProvider provider = new CountingConnectionProvider(connectionProvider, 1);
        try (JdbcSourceSplitReader<TestEntry> splitReader = newReader(provider, split)) {
            RecordsWithSplitIds<RecordAndOffset<TestEntry>> fetched = splitReader.fetch();
            assertThat(fetched.nextSplit()).isEqualTo("1");
            List<TestEntry> records = new ArrayList<>();
            RecordAndOffset<TestEntry> recordAndOffset = fetched.nextRecordFromSplit();
            while (recordAndOffset != null) {
                records.add(recordAndOffset.record);
                recordAndOffset = fetched.nextRecordFromSplit();
            }
            assertThat(records).hasSize(TEST_DATA.length);
            // The first (closed) connection plus exactly one re-established one: a reconnect ran.
            assertThat(provider.establishCount).isEqualTo(2);
        }
    }

    @Test
    void testFetchRethrowsImmediatelyWhenConnectionStaysOpen() throws Exception {
        // A query error on a healthy (open) connection must be rethrown immediately, with no
        // reconnect attempt.
        CountingConnectionProvider provider = new CountingConnectionProvider(connectionProvider, 0);
        JdbcSourceSplit invalidSplit =
                new JdbcSourceSplit("1", "select * from NON_EXISTENT_TABLE", null, null);
        try (JdbcSourceSplitReader<TestEntry> splitReader = newReader(provider, invalidSplit)) {
            assertThatThrownBy(splitReader::fetch).isInstanceOf(RuntimeException.class);
            assertThat(provider.establishCount).isEqualTo(1);
        }
    }

    @Test
    void testFetchFailsAfterExhaustingReconnectRetries() throws Exception {
        // If the connection keeps being closed, the reader gives up after the retry budget
        // instead of looping forever.
        CountingConnectionProvider provider =
                new CountingConnectionProvider(connectionProvider, Integer.MAX_VALUE);
        try (JdbcSourceSplitReader<TestEntry> splitReader = newReader(provider, split)) {
            assertThatThrownBy(splitReader::fetch).isInstanceOf(RuntimeException.class);
            // The initial attempt plus MAX_CONNECTION_RETRIES (3) reconnect attempts.
            assertThat(provider.establishCount).isEqualTo(4);
        }
    }

    private JdbcSourceSplitReader<TestEntry> newReader(
            JdbcConnectionProvider provider, JdbcSourceSplit sourceSplit) {
        JdbcSourceSplitReader<TestEntry> reader =
                new JdbcSourceSplitReader<>(
                        new TestingReaderContext(),
                        new Configuration(),
                        TypeInformation.of(TestEntry.class),
                        provider,
                        DeliveryGuarantee.NONE,
                        extractor);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(sourceSplit)));
        return reader;
    }

    /**
     * A {@link JdbcConnectionProvider} that closes the first {@code closeFirstN} connections it
     * hands out (returning them already closed) to deterministically reproduce a connection torn
     * down underneath the reader, while counting how many connections were established.
     */
    private static class CountingConnectionProvider implements JdbcConnectionProvider {
        private final JdbcConnectionProvider delegate;
        private final int closeFirstN;
        private int establishCount = 0;

        CountingConnectionProvider(JdbcConnectionProvider delegate, int closeFirstN) {
            this.delegate = delegate;
            this.closeFirstN = closeFirstN;
        }

        @Override
        public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
            Connection connection = delegate.getOrEstablishConnection();
            if (++establishCount <= closeFirstN) {
                connection.close();
            }
            return connection;
        }

        @Override
        public Connection getConnection() {
            return delegate.getConnection();
        }

        @Override
        public boolean isConnectionValid() throws SQLException {
            return delegate.isConnectionValid();
        }

        @Override
        public void closeConnection() {
            delegate.closeConnection();
        }

        @Override
        public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
            return delegate.reestablishConnection();
        }
    }
}
