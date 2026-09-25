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

package org.apache.flink.connector.jdbc.core.datastream.connection;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.Table;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableBounds;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link JdbcConnectionProvider} extended with the table/column metadata discovery and
 * bound-query building needed by the snapshot splitters.
 */
@PublicEvolving
public interface ConnectionProvider extends JdbcConnectionProvider {

    Set<Table> getTables(String catalog, String schema);

    Set<TableColumn> getTableColumns(TableId tableId);

    String createQueryWithBounds(
            TableId tableId, Set<String> tableColumns, TableColumn pkColumn, TableBounds bounds);

    TableBounds queryMinMax(TableId tableId, TableColumn column);

    Optional<Object> queryNextChunkMax(
            TableId tableId, TableColumn column, Object lowerBound, long chunkSize);

    /**
     * Creates a new independent instance of this provider backed by its own connection. When a
     * connection pool is configured, the new instance borrows from the shared pool. The caller is
     * responsible for closing the returned instance to release its connection.
     */
    ConnectionProvider newInstance();

    /**
     * Creates a transaction-scoped snapshot shared by every connection derived from this provider,
     * so bound discovery and data reads observe the same point-in-time view. Providers without
     * snapshot support treat this as a no-op; a snapshot that already exists is kept.
     *
     * <p>The exporting transaction is held open on this provider's connection for as long as the
     * connection lives. A snapshot does not survive a cluster restart: after restore a new snapshot
     * must be created (this method is called again from {@code start}).
     */
    default void createGlobalSnapshot() throws SQLException, ClassNotFoundException {}

    /**
     * Returns the id of the global snapshot this provider participates in, or {@code null} if none.
     * Split enumerators stamp this id onto the splits they emit so readers can join the same
     * snapshot.
     */
    @Nullable
    default String getGlobalSnapshotId() {
        return null;
    }

    /**
     * Joins this provider's connection to a previously exported global snapshot (e.g. one carried
     * by a {@link org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit}).
     * Providers without snapshot support treat this as a no-op. Implementations must configure the
     * connection (REPEATABLE READ, transaction open) such that subsequent queries observe exactly
     * the exported snapshot; the connection must not have its auto-commit flag flipped afterwards.
     */
    default void joinGlobalSnapshot(String snapshotId)
            throws SQLException, ClassNotFoundException {}

    /**
     * Whether this provider can actually export and join global snapshots. Readers refuse to open a
     * snapshot-bearing split when this returns {@code false} (a provider that merely inherits the
     * no-op {@link #joinGlobalSnapshot(String)} would otherwise silently read outside the snapshot
     * the split's id promises). Defaults to {@code false}; only snapshot-capable dialects override.
     */
    default boolean supportsGlobalSnapshot() {
        return false;
    }
}
