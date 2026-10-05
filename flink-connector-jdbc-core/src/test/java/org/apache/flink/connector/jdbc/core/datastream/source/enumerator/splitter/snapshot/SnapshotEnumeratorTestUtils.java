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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot;

import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.SplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Shared helpers for the snapshot splitter enumerator tests. */
final class SnapshotEnumeratorTestUtils {

    private SnapshotEnumeratorTestUtils() {}

    /**
     * Drains all splits, mirroring {@code JdbcSourceEnumerator}'s production contract of
     * enumerate-then-confirm.
     */
    static List<JdbcSourceSplit> drainAllSplits(SplitterEnumerator enumerator) {
        List<JdbcSourceSplit> allSplits = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<JdbcSourceSplit> batch;
        do {
            batch = enumerator.enumerateSplits();
            allSplits.addAll(batch);
            enumerator.confirmSplitsDelivered(batch);
        } while (!enumerator.isAllSplitsFinished() && System.nanoTime() < deadline);
        if (!enumerator.isAllSplitsFinished()) {
            // Fail loudly instead of returning partial results, which would surface as a
            // confusing downstream assertion (or worse, pass a "drained empty" check).
            throw new AssertionError(
                    "Splitter did not finish draining within 10s (got "
                            + allSplits.size()
                            + " splits so far)");
        }
        return allSplits;
    }

    /**
     * A {@link JdbcConnectionProvider} that is not a {@link
     * org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionProvider}.
     */
    static final class NotAConnectionProvider implements JdbcConnectionProvider {
        @Override
        public Connection getConnection() {
            return null;
        }

        @Override
        public boolean isConnectionValid() {
            return false;
        }

        @Override
        public Connection getOrEstablishConnection() {
            return null;
        }

        @Override
        public void closeConnection() {}

        @Override
        public Connection reestablishConnection() {
            return null;
        }
    }
}
