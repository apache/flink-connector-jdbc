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
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.Table;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
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

    private transient Queue<TableSplitterEnumerator> pendingTableSplitters;
    private transient List<TableSplitterEnumerator> activeTableSplitters;

    public DatabaseSplitterEnumerator(
            String catalog, String schema, Set<String> tables, int chunkSize) {
        super(schema);
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
        this.pendingTableSplitters = new ConcurrentLinkedQueue<>();
        this.activeTableSplitters = new CopyOnWriteArrayList<>();
        prepareTableSplitters();
        startBackgroundWork();
    }

    @Override
    public List<String> lineageQueries() {
        return new ArrayList<>(this.lineageQueries);
    }

    @Override
    protected void runBackgroundWork() throws InterruptedException {
        // Start an initial batch of table splitters — each one borrows its
        // own pooled connection. We refill below as splitters finish so we
        // never exceed the pool capacity.
        fillActiveTableSplitters();

        while (!Thread.currentThread().isInterrupted()
                && (!activeTableSplitters.isEmpty() || !pendingTableSplitters.isEmpty())) {
            boolean producedSplits = false;
            List<TableSplitterEnumerator> finished = new ArrayList<>();

            for (TableSplitterEnumerator tableSplitter : activeTableSplitters) {
                if (tableSplitter.isAllSplitsFinished()) {
                    lineageQueries.addAll(tableSplitter.lineageQueries());
                    tableSplitter.close();
                    finished.add(tableSplitter);
                    continue;
                }

                List<JdbcSourceSplit> splits = tableSplitter.enumerateSplits();
                if (!splits.isEmpty()) {
                    offerAll(splits);
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

    private void fillActiveTableSplitters() {
        while (activeTableSplitters.size() < MAX_CONCURRENT_TABLE_SPLITTERS) {
            TableSplitterEnumerator tableSplitter = pendingTableSplitters.poll();
            if (tableSplitter == null) {
                return;
            }
            // Each table splitter gets its own pooled connection so they can
            // run concurrently without JDBC thread-safety issues.
            tableSplitter.start(connection.newInstance());
            activeTableSplitters.add(tableSplitter);
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
            Set<String> tableColumns =
                    this.connection.getTableColumns(table.tableId()).stream()
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

            if (partitions.isEmpty()) {
                partitions = Collections.singleton(table.tableId());
            }

            partitions.stream()
                    .sorted(Comparator.comparing(TableId::toString))
                    .forEach(
                            tableId -> {
                                TableSplitterEnumerator tableSplitter =
                                        new TableSplitterEnumerator(
                                                tableId, tableColumns, chunkSize);
                                pendingTableSplitters.add(tableSplitter);
                            });
        }
    }
}
