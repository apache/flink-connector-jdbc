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

package org.apache.flink.connector.jdbc.postgres.datastream.connection;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.core.datastream.connection.AbstractConnectionProvider;
import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionException;
import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionOptions;
import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionProvider;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.Table;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableBounds;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Postgres {@link ConnectionProvider} implementation backed by a pooled JDBC connection. Only the
 * Postgres-specific table/partition discovery and bound-query building live here; the connection
 * lifecycle, pooling, and statement plumbing are inherited from {@link AbstractConnectionProvider}.
 */
@PublicEvolving
public class PostgresConnectionProvider extends AbstractConnectionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresConnectionProvider.class);

    private static final String TABLE_PARTITIONED_TYPE = "PARTITIONED TABLE";
    private static final Set<String> TABLE_TYPES =
            new HashSet<>(Arrays.asList("TABLE", TABLE_PARTITIONED_TYPE));
    private static final Set<String> VIEW_TYPES =
            new HashSet<>(Arrays.asList("VIEW", "MATERIALIZED VIEW"));
    private static final String[] SUPPORTED_TYPES =
            Stream.concat(TABLE_TYPES.stream(), VIEW_TYPES.stream()).toArray(String[]::new);

    private static final int POOL_SIZE = 4;

    private String snapshotId;
    private String snapshotTxId;

    public PostgresConnectionProvider(ConnectionOptions jdbcOptions) {
        super(jdbcOptions);
    }

    private PostgresConnectionProvider(ConnectionOptions jdbcOptions, HikariDataSource pool) {
        super(jdbcOptions, pool);
    }

    @Override
    public ConnectionProvider newInstance() {
        return new PostgresConnectionProvider(jdbcOptions, getOrCreatePool());
    }

    @Override
    protected int maxPoolSize() {
        return POOL_SIZE;
    }

    @Override
    protected String poolName() {
        return "postgres-splitter-pool";
    }

    @Override
    protected void onConnectionEstablished() {
        checkTransactionSnapshotTxId();
    }

    public void createGlobalSnapshotId() throws SQLException, ClassNotFoundException {
        Connection currentConn = getOrEstablishConnection();
        currentConn.setAutoCommit(false);
        currentConn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        try (Statement statement = currentConn.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT pg_export_snapshot()")) {
            if (resultSet.next()) {
                snapshotId = resultSet.getString(1);
                LOG.info("Created global snapshot id: {}", snapshotId);
            }
            snapshotTxId = currentSnapshotTxId();
        }
    }

    private void checkTransactionSnapshotTxId() {
        try {
            if (snapshotId != null
                    && isConnectionValid()
                    && !snapshotTxId.equalsIgnoreCase(currentSnapshotTxId())) {
                Connection currentConn = getConnection();
                assert currentConn != null;
                if (!currentConn.getAutoCommit()) {
                    currentConn.rollback();
                }
                currentConn.setAutoCommit(false);
                currentConn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                LOG.info("Setting connection with snapshot id: {}", snapshotId);
                try (PreparedStatement statement =
                        currentConn.prepareStatement("SET TRANSACTION SNAPSHOT ?")) {
                    statement.setString(1, snapshotId);
                    statement.executeQuery();
                }
            }
        } catch (SQLException e) {
            throw new ConnectionException("Failed to set current snapshot txId to connection", e);
        }
    }

    public String currentSnapshotTxId() throws SQLException {
        Connection currentConn = getConnection();
        if (currentConn == null) {
            throw new SQLException("Connection is not established");
        }
        try (Statement statement = currentConn.createStatement();
                ResultSet resultSet =
                        statement.executeQuery("SELECT pg_current_snapshot()::text")) {
            if (resultSet.next()) {
                String txId = resultSet.getString(1);
                LOG.info("Current global snapshot txId: {}", txId);
                return txId;
            }
        }
        return null;
    }

    @Override
    public Set<Table> getTables(String catalog, String schema) {
        return getTables(catalog, schema, TABLE_TYPES);
    }

    public Set<Table> getTables(String catalog, String schema, Set<String> supportedTypes) {
        Map<TableId, Set<String>> tables = new HashMap<>();
        try {
            Connection conn = getOrEstablishConnection();
            try (PreparedStatement parentPS =
                            conn.prepareStatement("SELECT pg_partition_root(to_regclass(?))");
                    ResultSet rs =
                            conn.getMetaData()
                                    .getTables(
                                            catalog,
                                            schema,
                                            null,
                                            supportedTypes.toArray(new String[0]))) {
                while (rs.next()) {
                    Optional<String> parent = Optional.empty();
                    String tableType = rs.getString(4);
                    String tableName = rs.getString(3);

                    parent = getParent(parentPS, tableName);

                    TableId tableId =
                            TableId.builder()
                                    .withCatalogName(
                                            Optional.ofNullable(rs.getString(1)).orElse(catalog))
                                    .withSchemaName(
                                            Optional.ofNullable(rs.getString(2)).orElse(schema))
                                    .withTableName(parent.orElse(tableName))
                                    .build();

                    Set<String> partitions = tables.getOrDefault(tableId, new HashSet<>());
                    if (parent.isPresent() && !parent.get().equals(tableName)) {
                        partitions.add(tableName);
                    }
                    tables.put(tableId, partitions);
                }
            }
        } catch (SQLException | ClassNotFoundException e) {
            throw new ConnectionException(
                    "Failed to get tables for catalog " + catalog + " and schema " + schema, e);
        }
        return tables.entrySet().stream()
                .map(kv -> new Table(kv.getKey(), kv.getValue()))
                .collect(Collectors.toSet());
    }

    private Optional<String> getParent(PreparedStatement parentPS, String partitionName)
            throws SQLException {
        parentPS.setString(1, partitionName);
        try (ResultSet rs = parentPS.executeQuery()) {
            if (rs.next()) {
                String parentTable = rs.getString(1);
                return Optional.ofNullable(parentTable);
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public TableBounds queryMinMax(TableId tableId, TableColumn column) {
        String columnName = quote(column.columnName());
        if (column.isUuidColumnType()) {
            columnName = castToText(columnName);
        }
        String query =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s.%s",
                        columnName, columnName, tableId.schemaName(), tableId.tableName());
        return queryAndMap(
                query,
                rs -> {
                    if (rs.next()) {
                        return TableBounds.of(rs.getObject(1), rs.getObject(2));
                    } else {
                        return TableBounds.empty();
                    }
                });
    }

    @Override
    public Optional<Object> queryNextChunkMax(
            TableId tableId, TableColumn column, Object lowerBound, long chunkSize) {
        String columnName = quote(column.columnName());
        String query =
                String.format(
                        "SELECT %s FROM %s.%s WHERE %s > %s ORDER BY %s ASC OFFSET %d LIMIT 1",
                        columnName,
                        tableId.schemaName(),
                        tableId.tableName(),
                        columnName,
                        (column.isUuidColumnType() ? castToUuid("?") : "?"),
                        columnName,
                        chunkSize - 1);
        return this.prepareQueryAndMap(
                query,
                ps -> ps.setObject(1, lowerBound),
                rs -> {
                    if (rs.next()) {
                        return Optional.ofNullable(rs.getObject(1));
                    } else {
                        return Optional.empty();
                    }
                });
    }

    @Override
    public String createQueryWithBounds(
            TableId tableId, Set<String> tableColumns, TableColumn pkColumn, TableBounds bounds) {
        String query =
                String.format(
                        "SELECT %s FROM %s.%s",
                        tableColumns.stream()
                                .map(PostgresConnectionProvider::quote)
                                .collect(Collectors.joining(",")),
                        quote(tableId.schemaName()),
                        quote(tableId.tableName()));
        if (bounds.isEmpty()) {
            return query;
        }
        query += " WHERE 1=1";
        if (bounds.lowerBound() != null) {
            query +=
                    String.format(
                            " AND %s >= %s",
                            quote(pkColumn.columnName()),
                            (pkColumn.isUuidColumnType() ? castToUuid("?") : "?"));
        }
        if (bounds.upperBound() != null) {
            query +=
                    String.format(
                            " AND %s < %s",
                            quote(pkColumn.columnName()),
                            (pkColumn.isUuidColumnType() ? castToUuid("?") : "?"));
        }
        return query;
    }

    private static String quote(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String castToText(String value) {
        return String.format("(%s)::text", value);
    }

    private static String castToUuid(String value) {
        return String.format("(%s)::uuid", value);
    }
}
