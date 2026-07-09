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
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.Table;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/** Splitter enumerator that fans out over every table in a database/schema. */
@PublicEvolving
public class DatabaseSplitterEnumerator extends AsyncSnapshotSplitterEnumerator<JdbcSourceSplit> {

    private final String catalog;
    private final String schema;
    private final Set<String> tables;
    private final Set<String> lineageQueries;
    private final int chunkSize;

    // Bound concurrent table splitters so we don't exhaust the connection pool.
    // Must be <= pool max size in ConnectionProvider.
    private static final int MAX_CONCURRENT_TABLE_SPLITTERS = 4;

    // Upper bound of emitted-but-not-yet-consumed splits before the fan-out loop stops pulling
    // more from the table splitters (back-pressure for the JobManager heap).
    private static final int MAX_QUEUED_SPLITS = 10_000;

    private transient Queue<TableSplitterEnumerator> pendingTableSplitters;
    private transient List<TableSplitterEnumerator> activeTableSplitters;

    /**
     * Per-table progress captured at checkpoint time, applied to freshly prepared table splitters
     * when the background work starts after a restore. Consumed entry by entry while applying;
     * entries still present are captured by {@link #snapshotProgress} so a checkpoint landing in
     * the restore window cannot lose them.
     */
    private transient Map<TableId, TableSplitProgress> restoredTableProgresses;

    /**
     * Terminal progress of table splitters that completed, kept after the splitter itself is closed
     * and dropped from the active list so later checkpoints still know the table is done — without
     * it, a restore would re-enumerate finished tables and duplicate every split they emitted.
     */
    private transient Map<TableId, TableSplitProgress> completedTableProgresses;

    /**
     * True from {@link #restoreProgress} until the restored per-table progress has been applied to
     * the freshly prepared table splitters. While set, {@link #snapshotProgress} re-persists the
     * restored progress verbatim instead of the not-yet-materialized live state, so a checkpoint
     * landing in the restore window cannot silently drop per-table progress.
     */
    private transient boolean restorePending;

    public DatabaseSplitterEnumerator(
            String catalog,
            String schema,
            Set<String> tables,
            int chunkSize,
            Boundedness boundedness) {
        super(schema, boundedness);
        this.catalog = catalog;
        this.schema = schema;
        this.tables = tables;
        this.lineageQueries = new LinkedHashSet<>();
        this.chunkSize = chunkSize;
    }

    public static DatabaseSplitterEnumeratorBuilder builder() {
        return new DatabaseSplitterEnumeratorBuilder();
    }

    @Override
    public void start(JdbcConnectionProvider connectionProvider) {
        initConnection(connectionProvider);
        try {
            // Export the shared snapshot synchronously, before start() returns: every split the
            // enumerator hands out afterwards (including restored splits re-stamped at hand-over)
            // must observe a non-null currentSnapshotId(), and table splitters created via
            // connection.newInstance() inherit it for bound discovery.
            connection.createGlobalSnapshot();
        } catch (SQLException | ClassNotFoundException e) {
            throw new ConnectionException("Failed to create shared database snapshot", e);
        }
        this.pendingTableSplitters = new ConcurrentLinkedQueue<>();
        this.activeTableSplitters = new CopyOnWriteArrayList<>();
        this.completedTableProgresses = new HashMap<>();
        startBackgroundWork();
    }

    @Override
    public synchronized List<String> lineageQueries() {
        return new ArrayList<>(this.lineageQueries);
    }

    @Override
    protected void runBackgroundWork() throws InterruptedException {
        // The shared snapshot was exported synchronously in start(); table splitters derived from
        // connection.newInstance() inherit it for bound discovery, and each split carries the id so
        // reader connections join the same snapshot.
        prepareTableSplitters();
        applyRestoredProgress();

        // Start an initial batch of table splitters — each one borrows its
        // own pooled connection. We refill below as splitters finish so we
        // never exceed the pool capacity.
        fillActiveTableSplitters();

        // Loop while work remains. Once the pending queue has been drained, the active set only
        // shrinks — a momentarily-empty active set means every table splitter has finished and
        // been closed, even if none of them produced splits on the previous iteration.
        while (!Thread.currentThread().isInterrupted() && !activeTableSplitters.isEmpty()) {
            boolean producedSplits = false;
            List<TableSplitterEnumerator> finished = new ArrayList<>();

            // Back-pressure: stop pulling from table splitters while the output backlog is
            // deep, so unassigned splits cannot exhaust the JobManager heap. The table splitters
            // pace themselves once their own queues fill up.
            boolean backlogFull = queuedItemCount() >= MAX_QUEUED_SPLITS;

            for (TableSplitterEnumerator tableSplitter : activeTableSplitters) {
                if (tableSplitter.isAllSplitsFinished()) {
                    addLineageQueries(tableSplitter.lineageQueries());
                    // Record the terminal progress BEFORE closing/dropping: a checkpoint taken
                    // after this point still needs to know the table finished, otherwise a restore
                    // re-enumerates it and duplicates every split it already emitted.
                    captureCompletedProgress(tableSplitter);
                    tableSplitter.close();
                    finished.add(tableSplitter);
                    continue;
                }

                if (backlogFull) {
                    continue;
                }

                // Not holding the monitor while polling: any failure between this call and the
                // offer below leaves the splits accounted for in the child splitter's staged
                // (handed-off) state, which the checkpoint captures.
                List<JdbcSourceSplit> splits = tableSplitter.enumerateSplits();
                if (!splits.isEmpty()) {
                    synchronized (this) {
                        offerAll(splits);
                        tableSplitter.confirmSplitsDelivered(splits);
                    }
                    producedSplits = true;
                }
            }

            // CopyOnWriteArrayList's iterator doesn't support remove(); batch-remove instead.
            if (!finished.isEmpty()) {
                activeTableSplitters.removeAll(finished);
            }

            // Refill active set with pending splitters now that finished
            // ones have returned their connections to the pool.
            fillActiveTableSplitters();

            if (!producedSplits) {
                Thread.sleep(100);
            }
        }
    }

    @Override
    protected JdbcSourceSplit toSplit(JdbcSourceSplit item) {
        return item;
    }

    @Override
    protected Serializable snapshotProgress(List<JdbcSourceSplit> pendingItems) {
        // Called by the base class while holding this monitor.
        if (restorePending) {
            // Restore window: the per-table progress from the checkpoint has not been applied to
            // live table splitters yet. Re-persist it verbatim (with the current pending splits)
            // so a checkpoint completing in this window restores the same state, instead of
            // capturing empty live progress and dropping every table's cursor.
            return new DatabaseSplitProgress(
                    new ArrayList<>(restoredTableProgresses.values()), pendingItems);
        }
        if (pendingTableSplitters == null) {
            // Enumeration never started (e.g. background thread failed during table discovery).
            return null;
        }
        Map<TableId, TableSplitProgress> byTable = new LinkedHashMap<>();
        for (TableSplitterEnumerator tableSplitter : pendingTableSplitters) {
            collectProgress(tableSplitter, byTable);
        }
        for (TableSplitterEnumerator tableSplitter : activeTableSplitters) {
            collectProgress(tableSplitter, byTable);
        }
        // Finished splitters have been closed and dropped from the live lists above; their terminal
        // progress lives in completedTableProgresses. putIfAbsent: a splitter still present in the
        // active list during its final loop iteration reports the same terminal progress itself.
        completedTableProgresses.forEach(byTable::putIfAbsent);
        return new DatabaseSplitProgress(new ArrayList<>(byTable.values()), pendingItems);
    }

    private void captureCompletedProgress(TableSplitterEnumerator tableSplitter) {
        Serializable tableState = tableSplitter.serializableState();
        if (tableState != null) {
            synchronized (this) {
                completedTableProgresses.put(
                        tableSplitter.tableId(), (TableSplitProgress) tableState);
            }
        }
    }

    private static void collectProgress(
            TableSplitterEnumerator tableSplitter, Map<TableId, TableSplitProgress> byTable) {
        Serializable tableState = tableSplitter.serializableState();
        if (tableState != null) {
            byTable.put(tableSplitter.tableId(), (TableSplitProgress) tableState);
        }
    }

    @Override
    protected void restoreProgress(Serializable state) {
        DatabaseSplitProgress progress = (DatabaseSplitProgress) state;
        Map<TableId, TableSplitProgress> restored = new HashMap<>();
        for (TableSplitProgress tableProgress : progress.tableProgresses()) {
            restored.put(tableProgress.tableId(), tableProgress);
        }
        synchronized (this) {
            this.restoredTableProgresses = restored;
            this.restorePending = true;
        }
        restorePendingItems(progress.pendingSplits());
    }

    /**
     * Replays checkpointed per-table progress onto the freshly prepared table splitters. Tables
     * that were fully emitted (with nothing left pending) are dropped (their terminal progress is
     * retained for later checkpoints); tables that disappeared from the database since the
     * checkpoint are ignored.
     */
    private void applyRestoredProgress() {
        Map<TableId, TableSplitProgress> restored;
        synchronized (this) {
            restored = restoredTableProgresses;
        }
        if (restored == null || restored.isEmpty()) {
            synchronized (this) {
                restorePending = false;
            }
            return;
        }
        List<TableSplitterEnumerator> prepared = new ArrayList<>();
        TableSplitterEnumerator preparedSplitter;
        while ((preparedSplitter = pendingTableSplitters.poll()) != null) {
            prepared.add(preparedSplitter);
        }
        for (TableSplitterEnumerator tableSplitter : prepared) {
            // The restored map is never mutated while restorePending is set (snapshotProgress
            // reads it concurrently on the coordinator thread), so lookups use get(), not remove().
            TableSplitProgress progress = restored.get(tableSplitter.tableId());
            if (progress == null) {
                pendingTableSplitters.add(tableSplitter);
            } else if (progress.finished() && progress.pendingBounds().isEmpty()) {
                // Keep the terminal progress so later checkpoints still mark the table finished.
                captureCompletedProgress(tableSplitter, progress);
                tableSplitter.close();
            } else {
                tableSplitter.restoreProgress(progress);
                pendingTableSplitters.add(tableSplitter);
            }
        }
        synchronized (this) {
            restoredTableProgresses = null;
            restorePending = false;
        }
    }

    private void captureCompletedProgress(
            TableSplitterEnumerator tableSplitter, TableSplitProgress terminalProgress) {
        synchronized (this) {
            completedTableProgresses.put(tableSplitter.tableId(), terminalProgress);
        }
    }

    @Override
    protected void closeResources() {
        if (activeTableSplitters == null) {
            return;
        }
        for (TableSplitterEnumerator tableSplitter : activeTableSplitters) {
            tableSplitter.close();
        }
        activeTableSplitters.clear();
        // Do NOT close the main connection — it is managed by the caller (JdbcSourceEnumerator).
        // Table splitters close their own pooled connections in their close() method.
    }

    private synchronized void addLineageQueries(Collection<String> queries) {
        lineageQueries.addAll(queries);
    }

    private void fillActiveTableSplitters() {
        while (activeTableSplitters.size() < MAX_CONCURRENT_TABLE_SPLITTERS) {
            TableSplitterEnumerator tableSplitter;
            // Move from pending to active atomically with respect to snapshotProgress (which runs
            // under this same monitor): otherwise a checkpoint could observe the splitter in
            // neither
            // list and drop its (possibly just-restored) progress. start() stays outside the lock —
            // it does connection setup and spawns the background thread; a not-yet-started splitter
            // reports null progress, so the add-before-start order stays safe.
            synchronized (this) {
                tableSplitter = pendingTableSplitters.poll();
                if (tableSplitter == null) {
                    return;
                }
                activeTableSplitters.add(tableSplitter);
            }
            // Each table splitter gets its own pooled connection so they can
            // run concurrently without JDBC thread-safety issues.
            tableSplitter.start(connection.newInstance());
        }
    }

    private void prepareTableSplitters() {
        Set<Table> dbTablesWithPartition = this.connection.getTables(this.catalog, this.schema);
        Set<Table> tablesToProcess = new HashSet<>();

        for (Table table : dbTablesWithPartition) {
            if (tables == null
                    || tables.isEmpty()
                    || tables.contains(table.tableId().tableName())) {
                tablesToProcess.add(table);
            } else {
                Set<String> partitions =
                        table.partitions().stream()
                                .filter(tables::contains)
                                .collect(Collectors.toSet());

                if (!partitions.isEmpty()) {
                    tablesToProcess.add(new Table(table.tableId(), partitions));
                }
            }
        }

        if (tablesToProcess.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "No tables found in the database for catalog: %s, schema: %s, with specified tables: %s",
                            this.catalog, this.schema, this.tables));
        }

        for (Table table : tablesToProcess) {
            Set<TableColumn> resolvedColumns = this.connection.getTableColumns(table.tableId());
            Set<String> tableColumns =
                    resolvedColumns.stream()
                            .map(TableColumn::columnName)
                            .collect(Collectors.toSet());

            Set<TableId> partitions =
                    table.partitions().stream()
                            .map(
                                    p ->
                                            TableId.builder()
                                                    .withCatalogName(table.tableId().catalogName())
                                                    .withSchemaName(table.tableId().schemaName())
                                                    .withTableName(p)
                                                    .build())
                            .collect(Collectors.toSet());

            boolean partitioned = !partitions.isEmpty();
            if (partitions.isEmpty()) {
                partitions = Collections.singleton(table.tableId());
            }

            final Set<TableColumn> rootColumns = resolvedColumns;
            partitions.stream()
                    .sorted(Comparator.comparing(TableId::toString))
                    .forEach(
                            tableId -> {
                                // Reuse the metadata already fetched for the root table to avoid a
                                // second round trip per table; a partition child is handed null so
                                // it resolves its own (possibly divergent) partition-key metadata.
                                Set<TableColumn> preResolved = partitioned ? null : rootColumns;
                                TableSplitterEnumerator tableSplitter =
                                        new TableSplitterEnumerator(
                                                tableId,
                                                tableColumns,
                                                preResolved,
                                                chunkSize,
                                                getBoundedness());
                                pendingTableSplitters.add(tableSplitter);
                            });
        }
    }
}
