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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionException;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.SplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableBounds;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.connector.jdbc.core.datastream.source.split.CheckpointedOffset;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation of {@link SplitterEnumerator} that splits a table into multiple splits based on
 * the primary key column and a specified chunk size.
 */
@PublicEvolving
public class TableSplitterEnumerator extends AsyncSnapshotSplitterEnumerator<TableBounds> {

    private static final Logger LOG = LoggerFactory.getLogger(TableSplitterEnumerator.class);

    private final TableId tableId;
    private final Set<String> columnNames;
    private final int chunkSize;

    /** Column metadata resolved by the parent (database) splitter, or {@code null} standalone. */
    @Nullable private final Set<TableColumn> preResolvedColumns;

    private volatile TableColumn tablePrimaryKey;
    private final Set<String> lineageQueries;

    /** Upper bound of still-unemitted chunk bounds before pacing kicks in. */
    private static final int MAX_QUEUED_BOUNDS = 1000;

    /**
     * Set once the terminal (or only) split for this table has been offered: the single split of an
     * empty/single-row table, or the trailing {@code (max, null)} chunk. Until then, a captured
     * progress must never claim the table is {@code finished} with nothing pending, or a restore
     * would silently drop the whole table.
     */
    private boolean terminalSplitOffered;

    private TableBounds tableMinMax;
    private Object currentLowerBound;
    private boolean boundsInitialized;

    protected TableSplitterEnumerator(
            TableId tableId, Set<String> columnNames, int chunkSize, Boundedness boundedness) {
        this(tableId, columnNames, null, chunkSize, boundedness);
    }

    TableSplitterEnumerator(
            TableId tableId,
            Set<String> columnNames,
            @Nullable Set<TableColumn> preResolvedColumns,
            int chunkSize,
            Boundedness boundedness) {
        super(tableId.toString(), boundedness);
        this.tableId = tableId;
        this.columnNames = columnNames;
        this.preResolvedColumns = preResolvedColumns;
        this.chunkSize = chunkSize;
        this.lineageQueries = new LinkedHashSet<>();
        this.boundsInitialized = false;
    }

    TableId tableId() {
        return tableId;
    }

    public static TableSplitterEnumeratorBuilder builder() {
        return new TableSplitterEnumeratorBuilder();
    }

    @Override
    public void start(JdbcConnectionProvider connectionProvider) {
        initConnection(connectionProvider);
        try {
            // Export a snapshot shared by every connection derived from this provider (pooled
            // table splitters via newInstance(), reader connections via the id stamped onto
            // splits) so bound discovery and data reads see one consistent point in time.
            // No-op for providers without snapshot support and when one already exists (this
            // instance may be a pool clone that inherited the owner's snapshot).
            connection.createGlobalSnapshot();
        } catch (SQLException | ClassNotFoundException e) {
            throw new ConnectionException(
                    "Failed to create shared snapshot for table " + tableId, e);
        }
        startBackgroundWork();
    }

    @Override
    public synchronized List<String> lineageQueries() {
        return new ArrayList<>(lineageQueries);
    }

    @Override
    protected void runBackgroundWork() throws InterruptedException {
        validateTableAndColumns();
        if (!boundsInitialized) {
            return;
        }
        while (computeNextBound()) {
            // Pace bound discovery so the pending-bounds queue stays bounded even when readers
            // consume splits slower than the cursor advances (a 1T-row table would otherwise
            // queue every chunk bound in the JobManager heap).
            while (queuedItemCount() >= MAX_QUEUED_BOUNDS
                    && !Thread.currentThread().isInterrupted()) {
                Thread.sleep(10);
            }
        }
    }

    @Override
    protected void closeResources() {
        // Close this splitter's own pooled connection, returning it to the pool.
        if (this.connection != null) {
            this.connection.closeConnection();
        }
    }

    @Override
    protected Serializable snapshotProgress(List<TableBounds> pendingItems) {
        // Called by the base class while holding this monitor; computeNextBound()/validate() mutate
        // the same fields under that lock, so this capture cannot tear against the compute thread.
        if (tableMinMax == null || (!boundsInitialized && !terminalSplitOffered)) {
            // Computation never produced anything capturable (not started, or failed/interleaved
            // between min/max discovery and the first offer): treat as "no progress" so a restore
            // re-computes the table from scratch rather than trusting a half-initialized state.
            return null;
        }
        return new TableSplitProgress(
                tableId,
                boundsInitialized,
                terminalSplitOffered,
                tableMinMax,
                currentLowerBound,
                pendingItems);
    }

    @Override
    protected void restoreProgress(Serializable state) {
        TableSplitProgress progress = (TableSplitProgress) state;
        if (!progress.tableId().equals(tableId)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Restored progress is for table %s but this splitter is for table %s",
                            progress.tableId(), tableId));
        }
        synchronized (this) {
            this.tableMinMax = progress.tableMinMax();
            this.boundsInitialized = progress.boundsInitialized();
            this.terminalSplitOffered = progress.finished();
            this.currentLowerBound = progress.currentLowerBound();
        }
        restorePendingItems(progress.pendingBounds());
    }

    private void validateTableAndColumns() {
        Set<TableColumn> tableColumns;
        if (preResolvedColumns != null) {
            // Column metadata was already fetched by the parent (database) splitter; re-querying
            // it here would double the per-table metadata round trips at database scale.
            tableColumns = preResolvedColumns;
        } else {
            Set<TableColumn> discoveredColumns = connection.getTableColumns(tableId);
            if (this.columnNames.isEmpty()) {
                tableColumns = discoveredColumns;
            } else {
                tableColumns =
                        discoveredColumns.stream()
                                .filter(col -> columnNames.contains(col.columnName()))
                                .collect(Collectors.toSet());
            }
        }

        // When no explicit column names were requested, the discovered columns define the set.
        Set<String> resolvedNames;
        if (this.columnNames.isEmpty() && preResolvedColumns == null) {
            resolvedNames =
                    tableColumns.stream().map(TableColumn::columnName).collect(Collectors.toSet());
        } else {
            resolvedNames = this.columnNames;
        }

        if (tableColumns.size() != resolvedNames.size()) {
            Set<String> missingColumns =
                    resolvedNames.stream()
                            .filter(
                                    colName ->
                                            tableColumns.stream()
                                                    .noneMatch(
                                                            tableCol ->
                                                                    tableCol.columnName()
                                                                            .equals(colName)))
                            .collect(Collectors.toSet());
            throw new IllegalArgumentException(
                    String.format(
                            "These column names %s do not exist in table %s.",
                            missingColumns, tableId));
        }

        Set<TableColumn> primaryKeys =
                tableColumns.stream()
                        .filter(TableColumn::columnPrimaryKey)
                        .sorted(Comparator.comparingInt(TableColumn::columnPosition))
                        .collect(Collectors.toCollection(LinkedHashSet::new));

        if (primaryKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Table %s does not have a primary key or is not inside columns fields provided."
                                    + " Snapshot reading requires a primary key to chunk the table.",
                            tableId));
        }
        if (primaryKeys.size() > 1) {
            LOG.warn("Table {} has a composite primary key, using only the first field.", tableId);
        }

        synchronized (this) {
            if (preResolvedColumns == null && this.columnNames.isEmpty()) {
                this.columnNames.addAll(
                        tableColumns.stream()
                                .map(TableColumn::columnName)
                                .collect(Collectors.toSet()));
            }
            this.tablePrimaryKey = primaryKeys.iterator().next();

            if (tableMinMax != null) {
                // Progress was restored from a checkpoint: bounds, cursor and any single-table
                // split
                // were already computed (and possibly emitted) before the failover. Re-querying or
                // re-offering them here would emit duplicates.
                return;
            }
        }

        TableBounds minMax = connection.queryMinMax(tableId, tablePrimaryKey);

        synchronized (this) {
            this.tableMinMax = minMax;

            if (minMax.isEmpty()) {
                LOG.info("Table {} is empty, single unbounded split generated.", tableId);
                offer(minMax);
                terminalSplitOffered = true;
                return;
            }

            if (Objects.equals(minMax.lowerBound(), minMax.upperBound())) {
                LOG.info("Table {} has only one row, single split generated.", tableId);
                offer(TableBounds.empty());
                terminalSplitOffered = true;
                return;
            }

            this.currentLowerBound = minMax.lowerBound();
            this.boundsInitialized = true;
        }
    }

    private boolean computeNextBound() {
        Object lowerBound;
        synchronized (this) {
            if (!boundsInitialized || Objects.equals(currentLowerBound, tableMinMax.upperBound())) {
                return false;
            }
            lowerBound = currentLowerBound;
        }
        final Object upperMax = tableMinMax.upperBound();

        Optional<Object> nextChunk =
                connection.queryNextChunkMax(tableId, tablePrimaryKey, lowerBound, chunkSize);
        Object nextUpperBound = nextChunk.orElse(upperMax);
        TableBounds splitBounds = TableBounds.of(lowerBound, nextUpperBound);
        boolean isLastChunk = splitBounds.upperBound().equals(upperMax);

        synchronized (this) {
            if (splitBounds.lowerBound().equals(tableMinMax.lowerBound())) {
                offer(TableBounds.of(null, tableMinMax.lowerBound()));
            }
            offer(splitBounds);
            if (isLastChunk) {
                offer(TableBounds.of(upperMax, null));
                terminalSplitOffered = true;
            }
            currentLowerBound = nextUpperBound;
        }
        return !Objects.equals(nextUpperBound, upperMax);
    }

    @Override
    protected boolean readyToConvertQueuedItems() {
        // toSplit() dereferences the primary key (via createQueryWithBounds) and the resolved
        // column names; both are only populated by the background thread in
        // validateTableAndColumns(). Until then a restored bound cannot be converted.
        return tablePrimaryKey != null;
    }

    @Override
    protected JdbcSourceSplit toSplit(TableBounds bound) {
        // The split id embeds a hash of the chunk bounds rather than the raw primary-key boundary
        // values: split ids are surfaced in the Flink Web UI, REST and logs, so leaking actual
        // column values there would disclose user data to anyone with job read access. SHA-256
        // keeps
        // the id deterministic (stable across restarts) and collision-free for practical purposes.
        String splitId =
                String.format(
                        "%s:%s:%s:%s",
                        tableId.catalogName(),
                        tableId.schemaName(),
                        tableId.tableName(),
                        fingerprint(bound.toString()));
        String splitQuery =
                this.connection.createQueryWithBounds(tableId, columnNames, tablePrimaryKey, bound);
        Serializable[] splitParams = bound.getBoundsAsParams();
        JdbcSourceSplit split =
                new JdbcSourceSplit(
                        splitId,
                        splitQuery,
                        splitParams,
                        new CheckpointedOffset(),
                        connection.getGlobalSnapshotId());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generated split {}: bounds={}", splitId, bound);
        }
        // One representative query per table is sufficient to derive the lineage dataset name;
        // retaining every split's full SQL text would grow the JobManager heap with split count.
        if (lineageQueries.isEmpty()) {
            lineageQueries.add(splitQuery);
        }
        return split;
    }

    private static String fingerprint(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%02x", hash[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required but unavailable", e);
        }
    }
}
