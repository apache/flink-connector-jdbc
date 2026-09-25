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

import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionException;
import org.apache.flink.connector.jdbc.core.datastream.connection.ConnectionOptions;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.Table;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableBounds;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableColumn;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;
import org.apache.flink.connector.jdbc.postgres.PostgresTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ITCase for {@link PostgresConnectionProvider}: verifies the exported-snapshot lifecycle against a
 * real PostgreSQL instance (including re-applying the snapshot to pooled connections), table and
 * partition discovery, and bound-query building.
 */
class PostgresConnectionProviderITCase implements PostgresTestBase {

    private PostgresConnectionProvider provider;

    @BeforeEach
    void openProvider() {
        provider = new PostgresConnectionProvider(connectionOptions());
    }

    @AfterEach
    void closeProvider() throws Exception {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    void testSnapshotIsReappliedToPooledInstances() throws Exception {
        execute(
                "CREATE TABLE snapshot_t (id INT PRIMARY KEY, val TEXT)",
                "INSERT INTO snapshot_t (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        provider.createGlobalSnapshotId();

        // A concurrent committed insert must not be visible to the exported snapshot.
        try (Connection other =
                        DriverManager.getConnection(getMetadata().getJdbcUrlWithCredentials());
                Statement statement = other.createStatement()) {
            statement.execute("INSERT INTO snapshot_t (id, val) VALUES (4, 'd')");
        }

        PostgresConnectionProvider pooled = (PostgresConnectionProvider) provider.newInstance();
        try {
            // Regression: SET TRANSACTION SNAPSHOT with a bind parameter throws
            // `syntax error at or near "$1"`; applying the snapshot must succeed.
            assertThat(pooled.getOrEstablishConnection()).isNotNull();

            TableBounds bounds = pooled.queryMinMax(tableId("snapshot_t"), intPkColumn());
            assertThat(bounds.lowerBound()).isEqualTo(1);
            assertThat(bounds.upperBound())
                    .as("snapshot read must not see the concurrent insert")
                    .isEqualTo(3);
        } finally {
            pooled.close();
        }

        // A brand-new connection outside the snapshot does see the insert.
        try (Connection fresh =
                        DriverManager.getConnection(getMetadata().getJdbcUrlWithCredentials());
                Statement statement = fresh.createStatement();
                ResultSet rs = statement.executeQuery("SELECT MAX(id) FROM public.snapshot_t")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(4);
        }
    }

    @Test
    void testGetTablesDiscoversTablesAndPartitionsButNotViews() throws Exception {
        execute(
                "CREATE TABLE plain_t (id INT PRIMARY KEY)",
                "CREATE TABLE part_parent (id INT, region TEXT, PRIMARY KEY (id, region))"
                        + " PARTITION BY LIST (region)",
                "CREATE TABLE part_east PARTITION OF part_parent FOR VALUES IN ('east')",
                "CREATE VIEW plain_v AS SELECT 1 AS id");

        Set<Table> tables = provider.getTables(null, "public");
        Set<String> names =
                tables.stream()
                        .map(t -> t.tableId().tableName())
                        .collect(Collectors.toCollection(LinkedHashSet::new));

        assertThat(names).contains("plain_t", "part_parent");
        assertThat(names).doesNotContain("plain_v", "part_east");

        Table partitioned =
                tables.stream()
                        .filter(t -> t.tableId().tableName().equals("part_parent"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        assertThat(partitioned.partitions()).containsExactly("part_east");
    }

    @Test
    void testQueryMinMaxAndChunkBounds() throws Exception {
        execute("CREATE TABLE chunk_t (id INT PRIMARY KEY)");
        StringBuilder inserts = new StringBuilder("INSERT INTO chunk_t (id) VALUES ");
        for (int i = 1; i <= 25; i++) {
            inserts.append("(").append(i).append(")").append(i == 25 ? "" : ",");
        }
        execute(inserts.toString());

        TableId tableId = tableId("chunk_t");
        TableColumn pk = intPkColumn();

        TableBounds bounds = provider.queryMinMax(tableId, pk);
        assertThat(bounds.lowerBound()).isEqualTo(1);
        assertThat(bounds.upperBound()).isEqualTo(25);

        Optional<Object> nextChunkMax = provider.queryNextChunkMax(tableId, pk, 1, 10);
        assertThat(nextChunkMax).contains(11);

        String query =
                provider.createQueryWithBounds(
                        tableId,
                        new LinkedHashSet<>(Arrays.asList("id")),
                        pk,
                        TableBounds.of(1, 11));
        assertThat(query)
                .contains("\"id\"")
                .contains("\"public\".\"chunk_t\"")
                .contains("\"id\" >= ?")
                .contains("\"id\" < ?");
    }

    @Test
    void testUuidColumnHandling() throws Exception {
        execute(
                "CREATE TABLE uuid_t (id UUID PRIMARY KEY, payload TEXT)",
                "INSERT INTO uuid_t (id, payload) VALUES"
                        + " ('00000000-0000-0000-0000-000000000001'::uuid, 'a'),"
                        + " ('00000000-0000-0000-0000-000000000002'::uuid, 'b'),"
                        + " ('00000000-0000-0000-0000-000000000003'::uuid, 'c')");

        TableId tableId = tableId("uuid_t");
        TableColumn uuidPk =
                TableColumn.builder()
                        .withColumnName("id")
                        .withColumnType("uuid")
                        .withColumnPosition(1)
                        .withColumnNullable(false)
                        .withColumnPk(true)
                        .build();
        assertThat(uuidPk.isUuidColumnType()).isTrue();

        TableBounds bounds = provider.queryMinMax(tableId, uuidPk);
        assertThat(String.valueOf(bounds.lowerBound()))
                .isEqualTo("00000000-0000-0000-0000-000000000001");
        assertThat(String.valueOf(bounds.upperBound()))
                .isEqualTo("00000000-0000-0000-0000-000000000003");

        Optional<Object> nextChunkMax =
                provider.queryNextChunkMax(
                        tableId, uuidPk, "00000000-0000-0000-0000-000000000001", 1);
        assertThat(String.valueOf(nextChunkMax.orElseThrow(AssertionError::new)))
                .isEqualTo("00000000-0000-0000-0000-000000000002");

        String query =
                provider.createQueryWithBounds(
                        tableId,
                        new LinkedHashSet<>(Arrays.asList("id", "payload")),
                        uuidPk,
                        TableBounds.of(
                                "00000000-0000-0000-0000-000000000001",
                                "00000000-0000-0000-0000-000000000003"));
        assertThat(query).contains("(?)::uuid");
    }

    @Test
    void testGetTablesWithEmptyIdentifiersDiscoversUserSchemasOnly() throws Exception {
        // Empty string is the builder idiom for "unset": it must behave like null (discover all),
        // but the system catalogs are never intended split targets.
        execute("CREATE TABLE empty_id_t (id INT PRIMARY KEY)");

        Set<Table> tables = provider.getTables("", "");

        assertThat(tables.stream().map(t -> t.tableId().tableName()).collect(Collectors.toSet()))
                .contains("empty_id_t");
        assertThat(tables)
                .allSatisfy(
                        t -> {
                            String schema = t.tableId().schemaName();
                            assertThat(schema).doesNotStartWith("pg_");
                            assertThat(schema).isNotEqualTo("information_schema");
                        });
    }

    @Test
    void testJoiningDifferentSnapshotOnSameConnectionReApplies() throws Exception {
        // The connection-object fast path must also compare the applied snapshot id: joining a
        // NEWER snapshot on the connection that still holds an older one must switch visibility.
        execute("CREATE TABLE flip_t (id INT PRIMARY KEY)");
        provider.createGlobalSnapshot();
        String firstSnapshotId = provider.getGlobalSnapshotId();
        assertThat(firstSnapshotId).isNotNull();

        try (Connection exporter =
                DriverManager.getConnection(getMetadata().getJdbcUrlWithCredentials())) {
            exporter.setAutoCommit(false);
            try (Statement statement = exporter.createStatement()) {
                statement.execute("INSERT INTO flip_t (id) VALUES (1)");
            }
            exporter.commit();

            String secondSnapshotId;
            try (Statement statement = exporter.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT pg_export_snapshot()")) {
                assertThat(rs.next()).isTrue();
                secondSnapshotId = rs.getString(1);
            }

            provider.joinGlobalSnapshot(secondSnapshotId);

            try (Statement statement = provider.getOrEstablishConnection().createStatement();
                    ResultSet rs = statement.executeQuery("SELECT id FROM flip_t")) {
                assertThat(rs.next())
                        .as(
                                "the re-joined snapshot must see the row committed after the first "
                                        + "snapshot — skipping the SET via the connection fast path would "
                                        + "keep the stale first snapshot in effect")
                        .isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
            exporter.rollback();
        }
    }

    @Test
    void testIllegalSnapshotIdIsRejectedWithoutEchoingIt() throws Exception {
        String forged = "evil'; DROP TABLE snapshot_t; --";

        assertThatThrownBy(() -> provider.joinGlobalSnapshot(forged))
                .isInstanceOf(ConnectionException.class)
                .hasMessageContaining("rejected by allowlist")
                .hasMessageNotContaining(forged);
        assertThat(provider.isConnectionValid()).isTrue();
    }

    @Test
    void testBoundQueryCacheIsValidatedAcrossTables() throws Exception {
        execute(
                "CREATE TABLE cache_a (id INT PRIMARY KEY)",
                "CREATE TABLE cache_b (id INT PRIMARY KEY)");
        Set<String> cols = new LinkedHashSet<>(Arrays.asList("id"));
        TableId a = tableId("cache_a");
        TableId b = tableId("cache_b");
        TableColumn pk = intPkColumn();

        String first = provider.createQueryWithBounds(a, cols, pk, TableBounds.of(1, 5));
        String second = provider.createQueryWithBounds(b, cols, pk, TableBounds.of(1, 5));
        String again = provider.createQueryWithBounds(a, cols, pk, TableBounds.of(5, 9));

        assertThat(second).contains("\"cache_b\"").doesNotContain("\"cache_a\"");
        assertThat(again).isEqualTo(first);
    }

    private ConnectionOptions connectionOptions() {
        return ConnectionOptions.builder()
                .withUrl(getMetadata().getJdbcUrl())
                .withDriverName(getMetadata().getDriverClass())
                .withUsername(getMetadata().getUsername())
                .withPassword(getMetadata().getPassword())
                .build();
    }

    private static TableId tableId(String tableName) {
        return TableId.builder()
                .withCatalogName("")
                .withSchemaName("public")
                .withTableName(tableName)
                .build();
    }

    private static TableColumn intPkColumn() {
        return TableColumn.builder()
                .withColumnName("id")
                .withColumnType("int4")
                .withColumnPosition(1)
                .withColumnNullable(false)
                .withColumnPk(true)
                .build();
    }

    private void execute(String... statements) throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(getMetadata().getJdbcUrlWithCredentials());
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
