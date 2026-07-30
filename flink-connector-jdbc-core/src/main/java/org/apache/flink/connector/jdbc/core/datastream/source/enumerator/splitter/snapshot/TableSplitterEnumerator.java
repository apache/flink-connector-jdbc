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
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.SplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableBounds;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.connector.jdbc.core.datastream.source.split.CheckpointedOffset;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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
    private final Logger log = LoggerFactory.getLogger(TableSplitterEnumerator.class);

    private final TableId tableId;
    private final Set<String> columnNames;
    private final int chunkSize;

    private TableColumn tablePrimaryKey;
    private final Set<String> lineageQueries;

    private TableBounds tableMinMax;
    private Object currentLowerBound;
    private boolean boundsInitialized;

    protected TableSplitterEnumerator(TableId tableId, Set<String> columnNames, int chunkSize) {
        super(tableId.toString());
        this.tableId = tableId;
        this.columnNames = columnNames;
        this.chunkSize = chunkSize;
        this.lineageQueries = new LinkedHashSet<>();
        this.boundsInitialized = false;
    }

    public static TableSplitterEnumeratorBuilder builder() {
        return new TableSplitterEnumeratorBuilder();
    }

    @Override
    public void start(JdbcConnectionProvider connectionProvider) {
        initConnection(connectionProvider);
        startBackgroundWork();
    }

    @Override
    public List<String> lineageQueries() {
        return new ArrayList<>(lineageQueries);
    }

    @Override
    protected void runBackgroundWork() {
        validateTableAndColumns();
        if (!boundsInitialized) {
            return;
        }
        while (computeNextBound()) {
            // offer() inside computeNextBound() already signals readiness per item
        }
    }

    @Override
    protected void closeResources() {
        // Close this splitter's own pooled connection, returning it to the pool.
        if (this.connection != null) {
            this.connection.closeConnection();
        }
    }

    private void validateTableAndColumns() {
        Set<TableColumn> discoveredColumns = connection.getTableColumns(tableId);

        Set<TableColumn> tableColumns;
        if (this.columnNames.isEmpty()) {
            tableColumns = discoveredColumns;
            this.columnNames.addAll(
                    tableColumns.stream().map(TableColumn::columnName).collect(Collectors.toSet()));
        } else {
            tableColumns =
                    discoveredColumns.stream()
                            .filter(col -> columnNames.contains(col.columnName()))
                            .collect(Collectors.toSet());
        }

        if (tableColumns.size() != columnNames.size()) {
            Set<String> missingColumns =
                    columnNames.stream()
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
            log.warn("Table {} has a composite primary key, using only the first field.", tableId);
        }

        this.tablePrimaryKey = primaryKeys.iterator().next();
        this.tableMinMax = connection.queryMinMax(tableId, tablePrimaryKey);

        if (tableMinMax.isEmpty()) {
            log.info("Table {} is empty, single unbounded split generated.", tableId);
            offer(tableMinMax);
            return;
        }

        if (Objects.equals(tableMinMax.lowerBound(), tableMinMax.upperBound())) {
            log.info("Table {} has only one row, single split generated.", tableId);
            offer(TableBounds.empty());
            return;
        }

        this.currentLowerBound = tableMinMax.lowerBound();
        this.boundsInitialized = true;
    }

    private boolean computeNextBound() {
        if (!boundsInitialized || Objects.equals(currentLowerBound, tableMinMax.upperBound())) {
            return false;
        }

        Optional<Object> nextChunk =
                connection.queryNextChunkMax(
                        tableId, tablePrimaryKey, currentLowerBound, chunkSize);
        Object nextUpperBound = nextChunk.orElse(tableMinMax.upperBound());
        TableBounds splitBounds = TableBounds.of(currentLowerBound, nextUpperBound);

        if (splitBounds.lowerBound().equals(tableMinMax.lowerBound())) {
            offer(TableBounds.of(null, tableMinMax.lowerBound()));
        }
        offer(splitBounds);
        if (splitBounds.upperBound().equals(tableMinMax.upperBound())) {
            offer(TableBounds.of(tableMinMax.upperBound(), null));
        }
        currentLowerBound = nextUpperBound;
        return !Objects.equals(currentLowerBound, tableMinMax.upperBound());
    }

    @Override
    protected JdbcSourceSplit toSplit(TableBounds bound) {
        String splitId =
                String.format(
                        "%s:%s:%s:%s",
                        tableId.catalogName(),
                        tableId.schemaName(),
                        tableId.tableName(),
                        bound.toString());
        String splitQuery =
                this.connection.createQueryWithBounds(tableId, columnNames, tablePrimaryKey, bound);
        Serializable[] splitParams = bound.getBoundsAsParams();
        JdbcSourceSplit split =
                new JdbcSourceSplit(splitId, splitQuery, splitParams, new CheckpointedOffset());
        log.info("Generated split: {}", split);
        lineageQueries.add(splitQuery);
        return split;
    }
}
