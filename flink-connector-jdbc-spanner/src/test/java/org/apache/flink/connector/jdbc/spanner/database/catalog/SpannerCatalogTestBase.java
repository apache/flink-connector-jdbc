/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.spanner.database.catalog;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.spanner.SpannerTestBase;
import org.apache.flink.connector.jdbc.spanner.testutils.SpannerDatabase;
import org.apache.flink.connector.jdbc.spanner.testutils.SpannerMetadata;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.apache.flink.connector.jdbc.spanner.testutils.tables.SpannerTableRow.spannerTableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

/** Test base for {@link SpannerCatalog}. */
class SpannerCatalogTestBase implements JdbcITCaseBase, SpannerTestBase {

    protected static final String TEST_CATALOG_NAME = "spanner_catalog";
    protected static final String TEST_SCHEMA = "test_schema";

    protected static final TableRow TABLE_SIMPLE = createTableSimple("test_simple");
    protected static final TableRow TABLE_SIMPLE_WITH_SCHEMA =
            createTableWithSchema("test_simple_with_schema");
    protected static final TableRow TABLE_ALL_TYPES = createTableAllTypes("test_all_types");
    protected static final TableRow TABLE_ALL_TYPES_SINK =
            createTableAllTypesSink("test_all_types_sink");
    protected static final TableRow TABLE_ARRAY_TYPES = createTableArrayTypes("test_array_types");
    protected static final TableRow TABLE_GROUPED_BY_SINK =
            createTableGroupedBy("test_grouped_by_sink");

    protected static String baseUrl;
    protected static SpannerMetadata metadata;
    protected static SpannerCatalog catalog;
    protected TableEnvironment tEnv;

    protected static TableRow createTableSimple(String name) {
        return spannerTableRow(
                name, pkField("id", dbType("STRING(36) NOT NULL"), DataTypes.STRING().notNull()));
    }

    protected static TableRow createTableAllTypes(String name) {
        return spannerTableRow(
                name,
                pkField("id", dbType("STRING(36) NOT NULL"), DataTypes.STRING().notNull()),
                field("col_bool", dbType("BOOL"), DataTypes.BOOLEAN()),
                field("col_bytes", dbType("BYTES(MAX)"), DataTypes.BYTES()),
                field("col_date", dbType("DATE"), DataTypes.DATE()),
                field("col_json", dbType("JSON"), DataTypes.STRING()),
                field("col_int64", dbType("INT64"), DataTypes.BIGINT()),
                field("col_numeric", dbType("NUMERIC"), DataTypes.DECIMAL(38, 9)),
                field("col_float32", dbType("FLOAT32"), DataTypes.FLOAT()),
                field("col_float64", dbType("FLOAT64"), DataTypes.DOUBLE()),
                field("col_string", dbType("STRING(MAX)"), DataTypes.STRING()),
                field("col_timestamp", dbType("TIMESTAMP"), DataTypes.TIMESTAMP()));
    }

    protected static TableRow createTableAllTypesSink(String name) {
        // JSON type inserts are not supported, so it is excluded.
        return spannerTableRow(
                name,
                pkField("id", dbType("STRING(36) NOT NULL"), DataTypes.STRING().notNull()),
                field("col_bool", dbType("BOOL"), DataTypes.BOOLEAN()),
                field("col_bytes", dbType("BYTES(MAX)"), DataTypes.BYTES()),
                field("col_date", dbType("DATE"), DataTypes.DATE()),
                field("col_int64", dbType("INT64"), DataTypes.BIGINT()),
                field("col_numeric", dbType("NUMERIC"), DataTypes.DECIMAL(38, 9)),
                field("col_float32", dbType("FLOAT32"), DataTypes.FLOAT()),
                field("col_float64", dbType("FLOAT64"), DataTypes.DOUBLE()),
                field("col_string", dbType("STRING(MAX)"), DataTypes.STRING()),
                field("col_timestamp", dbType("TIMESTAMP"), DataTypes.TIMESTAMP()));
    }

    protected static TableRow createTableArrayTypes(String name) {
        return spannerTableRow(
                name,
                pkField("id", dbType("STRING(36) NOT NULL"), DataTypes.STRING().notNull()),
                field("col_arr_bool", dbType("ARRAY<BOOL>"), DataTypes.ARRAY(DataTypes.BOOLEAN())),
                field(
                        "col_arr_bytes",
                        dbType("ARRAY<BYTES(MAX)>"),
                        DataTypes.ARRAY(DataTypes.BYTES())),
                field("col_arr_date", dbType("ARRAY<DATE>"), DataTypes.ARRAY(DataTypes.DATE())),
                field("col_arr_int64", dbType("ARRAY<INT64>"), DataTypes.ARRAY(DataTypes.BIGINT())),
                field(
                        "col_arr_numeric",
                        dbType("ARRAY<NUMERIC>"),
                        DataTypes.ARRAY(DataTypes.DECIMAL(38, 9))),
                // INVALID_ARGUMENT: Value has type ARRAY<FLOAT64> which cannot be inserted into
                // column col_arr_float32, which has type ARRAY<FLOAT>
                // field(
                //         "col_arr_float32",
                //         dbType("ARRAY<FLOAT32>"),
                //         DataTypes.ARRAY(DataTypes.FLOAT())),
                field(
                        "col_arr_float64",
                        dbType("ARRAY<FLOAT64>"),
                        DataTypes.ARRAY(DataTypes.DOUBLE())),
                field(
                        "col_arr_string",
                        dbType("ARRAY<STRING(MAX)>"),
                        DataTypes.ARRAY(DataTypes.STRING())),
                field(
                        "col_arr_timestamp",
                        dbType("ARRAY<TIMESTAMP>"),
                        DataTypes.ARRAY(DataTypes.TIMESTAMP())));
    }

    protected static TableRow createTableWithSchema(String name) {
        return spannerTableRow(
                TEST_SCHEMA + "." + name,
                pkField("id", dbType("STRING(36) NOT NULL"), DataTypes.STRING().notNull()));
    }

    protected static TableRow createTableGroupedBy(String name) {
        return spannerTableRow(
                name,
                pkField("id", dbType("STRING(36) NOT NULL"), DataTypes.STRING().notNull()),
                field("col_int64", dbType("INT64"), DataTypes.BIGINT()));
    }

    @BeforeAll
    static void beforeAll() throws SQLException {
        String jdbcUrl = SpannerDatabase.getMetadata().getJdbcUrl();
        baseUrl = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"));
        metadata = SpannerDatabase.getMetadata();

        Properties props =
                JdbcConnectionOptions.getBriefAuthProperties(
                        metadata.getUsername(), metadata.getPassword());
        props.put("autoConfigEmulator", "true");
        catalog =
                new SpannerCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        metadata.getDatabaseName(),
                        baseUrl,
                        props);

        executeQuery(TABLE_SIMPLE.getCreateQuery());
        executeQuery(TABLE_ALL_TYPES.getCreateQuery());
        executeQuery(TABLE_ALL_TYPES_SINK.getCreateQuery());
        executeQuery(TABLE_ARRAY_TYPES.getCreateQuery());
        executeQuery(TABLE_GROUPED_BY_SINK.getCreateQuery());
        executeQuery("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);
        executeQuery(TABLE_SIMPLE_WITH_SCHEMA.getCreateQuery());
        try (Connection conn = SpannerDatabase.getMetadata().getConnection()) {
            TABLE_SIMPLE.insertIntoTableValues(conn, "'12345-abcde'");
            TABLE_SIMPLE_WITH_SCHEMA.insertIntoTableValues(conn, "'98765-fghij'");
            TABLE_ALL_TYPES.insertIntoTableValues(
                    conn,
                    "'a', true, B'abc', DATE'2025-01-01', JSON'{\"key\": \"value1\"}', 1, 3.14, 1.1, -1.1, 'foo', TIMESTAMP'2025-01-01 00:00:00Z'",
                    "'b', false, B'123', DATE'2025-12-31', JSON'{\"key\": \"value2\"}', -1, -3.14, -1.1, 1.1, 'foo', TIMESTAMP'2025-12-31 00:00:00Z'");
            TABLE_ARRAY_TYPES.insertIntoTableValues(
                    conn,
                    "'a', [true, false], [B'abc', B'123'], [DATE'2025-01-01', DATE'2025-12-31'], "
                            + "[1, -1], [NUMERIC'3.14', NUMERIC'-3.14'], [1.1, -1.1], ['foo', 'bar'], "
                            + "[TIMESTAMP'2025-01-01 00:00:00Z', TIMESTAMP'2025-12-31 00:00:00Z']");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    static void afterAll() throws SQLException {
        executeQuery("DROP TABLE IF EXISTS " + TABLE_SIMPLE.getTableName());
        executeQuery("DROP TABLE IF EXISTS " + TABLE_ALL_TYPES.getTableName());
        executeQuery("DROP TABLE IF EXISTS " + TABLE_ALL_TYPES_SINK.getTableName());
        executeQuery("DROP TABLE IF EXISTS " + TABLE_ARRAY_TYPES.getTableName());
        executeQuery("DROP TABLE IF EXISTS " + TABLE_GROUPED_BY_SINK.getTableName());
        executeQuery("DROP TABLE IF EXISTS " + TABLE_SIMPLE_WITH_SCHEMA.getTableName());
        executeQuery("DROP SCHEMA IF EXISTS " + TEST_SCHEMA);
    }

    @BeforeEach
    void beforeEach() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        // Use Spanner catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    protected static void executeQuery(String query) throws SQLException {
        try (Connection conn = SpannerDatabase.getMetadata().getConnection();
                Statement statement = conn.createStatement()) {
            statement.execute(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
