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

package org.apache.flink.connector.jdbc.core.datastream.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.core.datastream.source.reader.JdbcSourceSplitReader;
import org.apache.flink.connector.jdbc.core.datastream.source.reader.RecordAndOffset;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link JdbcSource}. */
class JdbcSourceTest extends JdbcDataTestBase {

    private static final String SQL = "SELECT id, title, author, price, qty FROM " + INPUT_TABLE;

    @Test
    void testSplitReadersDoNotShareAConnection() throws Exception {
        // flink-connector-base builds a split reader per split fetcher and closes the reader of a
        // finished split while the reader of the next split is already reading. A connection
        // provider holds a single connection and is not thread safe, so every split reader needs
        // its own: sharing one lets the finished reader close the running reader's result set.
        JdbcSource<TestEntry> source =
                JdbcSource.<TestEntry>builder()
                        .setTypeInformation(TypeInformation.of(TestEntry.class))
                        .setSql(SQL)
                        .setDBUrl(getMetadata().getJdbcUrl())
                        .setDriverName(getMetadata().getDriverClass())
                        .setResultExtractor(extractor)
                        // One record per fetch, so the running reader still has an open result set
                        // when the finished one is closed.
                        .setSplitReaderFetchBatchSize(1)
                        .build();

        Supplier<SplitReader<RecordAndOffset<TestEntry>, JdbcSourceSplit>> supplier =
                source.splitReaderSupplier(new TestingReaderContext());

        JdbcSourceSplitReader<TestEntry> finishedSplitReader = newSplitReader(supplier, "1");
        JdbcSourceSplitReader<TestEntry> runningSplitReader = newSplitReader(supplier, "2");

        // The first fetch on each opens its result set, which is what establishes its connection.
        drain(finishedSplitReader.fetch());
        List<TestEntry> records = new ArrayList<>(drain(runningSplitReader.fetch()));

        finishedSplitReader.close();

        // The finished reader is gone; the running one must still read its split to the end.
        // One record per fetch, so the remaining count is exactly the number of fetches left.
        for (int i = records.size(); i < TEST_DATA.length; i++) {
            records.addAll(drain(runningSplitReader.fetch()));
        }
        runningSplitReader.close();
        assertThat(records).containsExactlyInAnyOrder(TEST_DATA);
    }

    @SuppressWarnings("unchecked")
    private static JdbcSourceSplitReader<TestEntry> newSplitReader(
            Supplier<SplitReader<RecordAndOffset<TestEntry>, JdbcSourceSplit>> supplier,
            String splitId) {
        JdbcSourceSplitReader<TestEntry> splitReader =
                (JdbcSourceSplitReader<TestEntry>) supplier.get();
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(new JdbcSourceSplit(splitId, SQL, null, null))));
        return splitReader;
    }

    private static List<TestEntry> drain(
            RecordsWithSplitIds<RecordAndOffset<TestEntry>> fetchedRecords) {
        List<TestEntry> records = new ArrayList<>();
        while (fetchedRecords.nextSplit() != null) {
            RecordAndOffset<TestEntry> recordAndOffset = fetchedRecords.nextRecordFromSplit();
            while (recordAndOffset != null) {
                records.add(recordAndOffset.getRecord());
                recordAndOffset = fetchedRecords.nextRecordFromSplit();
            }
        }
        return records;
    }
}
