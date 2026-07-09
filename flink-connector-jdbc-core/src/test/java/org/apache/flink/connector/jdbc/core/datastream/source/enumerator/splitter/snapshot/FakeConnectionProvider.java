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

import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionProvider;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.Table;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableBounds;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;

import java.sql.Connection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory {@link ConnectionProvider} test double. Simulates table/column metadata and primary key
 * value ranges purely in memory — no real JDBC connection is ever involved.
 */
class FakeConnectionProvider implements ConnectionProvider {

    private final Set<Table> tables;
    private final Map<TableId, Set<TableColumn>> columnsByTable;
    private final Map<TableId, LinkedHashSet<Long>> primaryKeyValuesByTable;

    final AtomicInteger closeCount = new AtomicInteger();

    FakeConnectionProvider(
            Set<Table> tables,
            Map<TableId, Set<TableColumn>> columnsByTable,
            Map<TableId, LinkedHashSet<Long>> primaryKeyValuesByTable) {
        this.tables = tables;
        this.columnsByTable = columnsByTable;
        this.primaryKeyValuesByTable = primaryKeyValuesByTable;
    }

    @Override
    public Set<Table> getTables(String catalog, String schema) {
        return tables;
    }

    @Override
    public Set<TableColumn> getTableColumns(TableId tableId) {
        return columnsByTable.get(tableId);
    }

    @Override
    public String createQueryWithBounds(
            TableId tableId, Set<String> tableColumns, TableColumn pkColumn, TableBounds bounds) {
        return "SELECT * FROM " + tableId + " WHERE bounds=" + bounds;
    }

    @Override
    public TableBounds queryMinMax(TableId tableId, TableColumn column) {
        LinkedHashSet<Long> values = primaryKeyValuesByTable.get(tableId);
        if (values == null || values.isEmpty()) {
            return TableBounds.empty();
        }
        long first = values.iterator().next();
        long last = first;
        for (long v : values) {
            last = v;
        }
        return TableBounds.of(first, last);
    }

    @Override
    public Optional<Object> queryNextChunkMax(
            TableId tableId, TableColumn column, Object lowerBound, long chunkSize) {
        LinkedHashSet<Long> values = primaryKeyValuesByTable.get(tableId);
        long lower = (Long) lowerBound;
        long seen = 0;
        for (long v : values) {
            if (v > lower) {
                seen++;
                if (seen == chunkSize) {
                    return Optional.of(v);
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public ConnectionProvider newInstance() {
        return this;
    }

    @Override
    public Connection getConnection() {
        return null;
    }

    @Override
    public boolean isConnectionValid() {
        return true;
    }

    @Override
    public Connection getOrEstablishConnection() {
        return null;
    }

    @Override
    public void closeConnection() {
        closeCount.incrementAndGet();
    }

    @Override
    public Connection reestablishConnection() {
        return null;
    }
}
