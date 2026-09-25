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

import javax.annotation.Nullable;

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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    private static final int POOL_SIZE = 4;

    // Resolves the partition root by (schema, name) instead of to_regclass(name), which resolves
    // through the search_path and would silently hit a same-named relation in another schema.
    private static final String PARTITION_ROOT_QUERY =
            "SELECT r.relname "
                    + "FROM pg_class c "
                    + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    + "JOIN pg_class r ON r.oid = pg_partition_root(c.oid) "
                    + "WHERE n.nspname = COALESCE(?, current_schema()) AND c.relname = ?";

    // Defense-in-depth allowlist for snapshot ids embedded as SQL literals (Postgres does not
    // accept them as bind parameters). pg_export_snapshot() yields hex segments separated by
    // dashes; anything else (quotes, whitespace, control chars) reaching the SET TRANSACTION
    // SNAPSHOT statement would signal forged/corrupted state, not a valid id.
    private static final Pattern SNAPSHOT_ID_PATTERN = Pattern.compile("[0-9A-Za-z_-]{1,64}");

    private String snapshotId;

    /**
     * Identity of the connection the exported snapshot was applied to (or that holds it). Used to
     * skip re-applying the snapshot on every {@link #getOrEstablishConnection()} call: it only
     * needs to be re-applied when a <em>different</em> (newly borrowed/established) connection is
     * handed out.
     */
    private transient Connection snapshotHoldingConnection;

    /**
     * Snapshot id actually in effect on {@link #snapshotHoldingConnection}. Must be tracked
     * separately from {@link #snapshotId}: joining a <em>different</em> snapshot id on the very
     * connection that still holds an old one must re-apply (rollback + SET), not be skipped just
     * because the connection object matches.
     */
    private transient @Nullable String appliedSnapshotId;

    public PostgresConnectionProvider(ConnectionOptions jdbcOptions) {
        super(jdbcOptions);
    }

    private PostgresConnectionProvider(
            ConnectionOptions jdbcOptions, HikariDataSource pool, @Nullable String snapshotId) {
        super(jdbcOptions, pool);
        this.snapshotId = snapshotId;
    }

    @Override
    public ConnectionProvider newInstance() {
        // Propagate the snapshot id: every connection derived from this provider (table splitters
        // fanning out over a pool, bound-discovery queries) must observe the same exported
        // snapshot, otherwise the chunk boundaries and the data reads disagree.
        return new PostgresConnectionProvider(jdbcOptions, getOrCreatePool(), snapshotId);
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
        applyTransactionSnapshot();
    }

    /**
     * Exports a snapshot on this provider's connection. The exporting transaction is kept open for
     * as long as the connection lives; connections derived from this provider (via {@link
     * #newInstance()} or {@link #joinGlobalSnapshot(String)}) read at exactly this point in time.
     */
    @Override
    public void createGlobalSnapshot() throws SQLException, ClassNotFoundException {
        if (snapshotId != null) {
            // Already exporting (or inherited the owner's snapshot): keep the existing one so all
            // derived connections stay on the same point in time.
            return;
        }
        createGlobalSnapshotId();
    }

    @Nullable
    @Override
    public String getGlobalSnapshotId() {
        return snapshotId;
    }

    @Override
    public boolean supportsGlobalSnapshot() {
        return true;
    }

    @Override
    public void closeConnection() {
        // The exported snapshot dies with its exporting transaction: if this provider instance is
        // ever reused after a close (e.g. the same deserialized object graph restarted in-JVM),
        // a stale snapshotId would make createGlobalSnapshot() skip the re-export and hand out an
        // id that no longer resolves. Forget the snapshot state alongside the connection.
        this.snapshotId = null;
        this.snapshotHoldingConnection = null;
        this.appliedSnapshotId = null;
        super.closeConnection();
    }

    /**
     * Joins this provider's connection to a snapshot exported elsewhere (typically the id carried
     * by a {@link org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit} on
     * a TaskManager, where the exporting connection on the JobManager is not reachable locally).
     */
    @Override
    public void joinGlobalSnapshot(String snapshotId) throws SQLException, ClassNotFoundException {
        if (holdingSnapshotOf(snapshotId)) {
            return;
        }
        this.snapshotId = snapshotId;
        // getOrEstablishConnection() invokes onConnectionEstablished() on both the reuse path and
        // the fresh-connection path, which (re-)applies the snapshot transaction whenever the
        // connection doesn't yet reflect this exact id.
        getOrEstablishConnection();
    }

    private boolean holdingSnapshotOf(String id) {
        Connection conn = getConnection();
        return conn != null && conn == snapshotHoldingConnection && id.equals(appliedSnapshotId);
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
        }
        // This connection holds the snapshot; no need to re-apply it to it.
        snapshotHoldingConnection = currentConn;
        appliedSnapshotId = snapshotId;
    }

    private void applyTransactionSnapshot() {
        if (snapshotId == null) {
            return;
        }
        Connection currentConn = getConnection();
        if (currentConn == null) {
            return;
        }
        if (currentConn == snapshotHoldingConnection && snapshotId.equals(appliedSnapshotId)) {
            return;
        }
        if (!SNAPSHOT_ID_PATTERN.matcher(snapshotId).matches()) {
            // Do not echo the rejected value: it originates from (possibly forged) checkpoint
            // state and could carry control characters into the JM/TM log.
            throw new ConnectionException(
                    "Illegal global snapshot id (rejected by allowlist, length "
                            + snapshotId.length()
                            + ")");
        }
        try {
            if (!currentConn.getAutoCommit()) {
                currentConn.rollback();
            }
            currentConn.setAutoCommit(false);
            currentConn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            LOG.debug("Setting connection with snapshot id: {}", snapshotId);
            try (Statement statement = currentConn.createStatement()) {
                // PostgreSQL accepts the snapshot id only as a literal, not as a bind parameter.
                statement.execute(
                        "SET TRANSACTION SNAPSHOT '" + snapshotId.replace("'", "''") + "'");
            }
            snapshotHoldingConnection = currentConn;
            appliedSnapshotId = snapshotId;
        } catch (SQLException e) {
            throw new ConnectionException("Failed to set transaction snapshot on connection", e);
        }
    }

    /** True for PostgreSQL's built-in namespaces (pg_catalog, pg_toast, pg_temp_*, ...). */
    private static boolean isSystemSchema(String schemaName) {
        return schemaName == null
                || schemaName.startsWith("pg_")
                || "information_schema".equals(schemaName);
    }

    @Override
    public Set<Table> getTables(String catalog, String schema) {
        Map<TableId, Set<String>> tables = new HashMap<>();
        // Empty string means "unset" in the builder idiom, but JDBC metadata treats it as "match
        // nothing" — pgjdbc even short-circuits to an empty ResultSet for a non-null non-matching
        // catalog. Translate to null.
        String catalogFilter = emptyToNull(catalog);
        String schemaFilter = emptyToNull(schema);
        try {
            Connection conn = getOrEstablishConnection();
            try (PreparedStatement parentPS = conn.prepareStatement(PARTITION_ROOT_QUERY);
                    ResultSet rs =
                            conn.getMetaData()
                                    .getTables(
                                            catalogFilter,
                                            schemaFilter,
                                            null,
                                            TABLE_TYPES.toArray(new String[0]))) {
                while (rs.next()) {
                    Optional<String> parent = Optional.empty();
                    String tableType = rs.getString(4);
                    String tableName = rs.getString(3);
                    String resolvedSchema = Optional.ofNullable(rs.getString(2)).orElse(schema);
                    if (schemaFilter == null && isSystemSchema(resolvedSchema)) {
                        // With no explicit schema, metadata enumerates the system catalogs too;
                        // snapshot splitting them is never intended (they lack usable PKs and the
                        // run would fail with a confusing error instead of a clear one).
                        continue;
                    }

                    parent = getParent(parentPS, resolvedSchema, tableName);

                    TableId tableId =
                            TableId.builder()
                                    .withCatalogName(
                                            Optional.ofNullable(rs.getString(1)).orElse(catalog))
                                    .withSchemaName(resolvedSchema)
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

    private Optional<String> getParent(
            PreparedStatement parentPS, @Nullable String schema, String partitionName)
            throws SQLException {
        parentPS.setString(1, schema);
        parentPS.setString(2, partitionName);
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
        // The table is always qualified and quoted (case-sensitive / injection-safe), matching
        // createQueryWithBounds.
        String fromClause = qualifiedTable(tableId);
        // Two single-aggregate queries instead of one combined MIN+MAX: PostgreSQL optimizes each
        // into an index boundary scan (O(log n)); the combined form forces a full scan.
        Object lower = queryAggregate(fromClause, "MIN", columnName);
        Object upper = queryAggregate(fromClause, "MAX", columnName);
        if (lower == null && upper == null) {
            return TableBounds.empty();
        }
        return TableBounds.of(lower, upper);
    }

    private Object queryAggregate(String fromClause, String aggregate, String columnName) {
        String query = String.format("SELECT %s(%s) FROM %s", aggregate, columnName, fromClause);
        return queryAndMap(
                query,
                rs -> {
                    if (rs.next()) {
                        return rs.getObject(1);
                    }
                    return null;
                });
    }

    @Override
    public Optional<Object> queryNextChunkMax(
            TableId tableId, TableColumn column, Object lowerBound, long chunkSize) {
        String columnName = quote(column.columnName());
        String query =
                String.format(
                        "SELECT %s FROM %s WHERE %s > %s ORDER BY %s ASC OFFSET %d LIMIT 1",
                        columnName,
                        qualifiedTable(tableId),
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

    // The SELECT..FROM prefix of a bounded query is constant for a given (table, columns) pair but
    // was rebuilt (stream + quote + join) once per split — significant CPU at chunk-count scale
    // since it happens on the coordinator under the enumerator monitor.
    private transient @Nullable String cachedSelectPrefix;
    private transient @Nullable TableId cachedSelectTableId;
    private transient @Nullable Set<String> cachedSelectColumns;

    @Override
    public String createQueryWithBounds(
            TableId tableId, Set<String> tableColumns, TableColumn pkColumn, TableBounds bounds) {
        String query;
        if (tableId.equals(cachedSelectTableId) && tableColumns.equals(cachedSelectColumns)) {
            query = cachedSelectPrefix;
        } else {
            query =
                    String.format(
                            "SELECT %s FROM %s.%s",
                            tableColumns.stream()
                                    .map(PostgresConnectionProvider::quote)
                                    .collect(Collectors.joining(",")),
                            quote(tableId.schemaName()),
                            quote(tableId.tableName()));
            cachedSelectPrefix = query;
            cachedSelectTableId = tableId;
            cachedSelectColumns = tableColumns;
        }
        if (bounds.isEmpty()) {
            return query;
        }
        StringBuilder builder = new StringBuilder(query).append(" WHERE 1=1");
        if (bounds.lowerBound() != null) {
            builder.append(
                    String.format(
                            " AND %s >= %s",
                            quote(pkColumn.columnName()),
                            (pkColumn.isUuidColumnType() ? castToUuid("?") : "?")));
        }
        if (bounds.upperBound() != null) {
            builder.append(
                    String.format(
                            " AND %s < %s",
                            quote(pkColumn.columnName()),
                            (pkColumn.isUuidColumnType() ? castToUuid("?") : "?")));
        }
        return builder.toString();
    }

    private static String quote(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String qualifiedTable(TableId tableId) {
        return quote(tableId.schemaName()) + "." + quote(tableId.tableName());
    }

    private static String castToText(String value) {
        return String.format("(%s)::text", value);
    }

    private static String castToUuid(String value) {
        return String.format("(%s)::uuid", value);
    }
}
