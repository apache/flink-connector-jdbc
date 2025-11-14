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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.database.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient.ListDatabasesPage;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient.ListDatabasesPagedResponse;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.spanner.admin.database.v1.Database;
import com.google.spanner.admin.database.v1.InstanceName;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;

/** Catalog for Spanner. */
@Internal
public class SpannerCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerCatalog.class);

    private static final Set<String> builtinSchemas =
            new HashSet<String>() {
                {
                    add("INFORMATION_SCHEMA");
                    add("SPANNER_SYS");
                }
            };

    private final JdbcCatalogTypeMapper dialectTypeMapper;

    @VisibleForTesting
    public SpannerCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        this(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                getBriefAuthProperties(username, pwd));
    }

    public SpannerCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            Properties connectProperties) {
        super(userClassLoader, catalogName, defaultDatabase, baseUrl, connectProperties);
        this.dialectTypeMapper = new SpannerTypeMapper();
    }

    private ConnectionOptions getConnectionOptions() {
        return ConnectionOptions.newBuilder().setUri(defaultUrl.replace("jdbc:", "")).build();
    }

    private SpannerOptions getSpannerOptions(ConnectionOptions options) {
        SpannerOptions.Builder builder =
                SpannerOptions.newBuilder().setProjectId(options.getProjectId());
        if (options.getHost().contains("localhost")) {
            builder.setEmulatorHost(options.getHost());
        } else {
            builder.setHost(options.getHost());
        }
        return builder.build();
    }

    private Map<String, Boolean> getColumnNullables(Connection conn, ObjectPath tablePath)
            throws SQLException {
        Map<String, Boolean> nullables = new HashMap<>();
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
                                + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                                + "ORDER BY ORDINAL_POSITION;")) {
            ps.setString(1, getSchemaName(tablePath));
            ps.setString(2, getTableName(tablePath));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String isNullable = rs.getString(2);
                    nullables.put(rs.getString(1), isNullable.equals("YES"));
                }
            }
        }
        return nullables;
    }

    @Override
    protected Optional<UniqueConstraint> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        // In the case of Spanner, the database cannot be specified
        // because it always uses the connected database.
        return super.getPrimaryKey(metaData, null, schema, table);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = Lists.newArrayList();
        ConnectionOptions connOptions = getConnectionOptions();
        SpannerOptions spannerOptions = getSpannerOptions(connOptions);
        try (Spanner spanner = spannerOptions.getService();
                DatabaseAdminClient databaseAdminClient = spanner.createDatabaseAdminClient()) {
            InstanceName instanceName =
                    InstanceName.of(connOptions.getProjectId(), connOptions.getInstanceId());
            ListDatabasesPagedResponse response = databaseAdminClient.listDatabases(instanceName);
            for (ListDatabasesPage page : response.iteratePages()) {
                for (Database database : page.iterateAll()) {
                    final String fullName = database.getName();
                    databases.add(fullName.substring(fullName.lastIndexOf('/') + 1));
                }
            }
        } catch (Exception e) {
            throw new CatalogException("Failed to list databases.", e);
        }
        return databases;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String databaseName = tablePath.getDatabaseName();

        try (Connection conn =
                DriverManager.getConnection(getDatabaseUrl(databaseName), connectionProperties)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<UniqueConstraint> primaryKey =
                    getPrimaryKey(
                            metaData,
                            databaseName,
                            getSchemaName(tablePath),
                            getTableName(tablePath));

            // ResultSetMetaData.isNullable always returns columnNullableUnknown=2.
            // https://github.com/googleapis/java-spanner-jdbc/blob/v2.26.1/src/main/java/com/google/cloud/spanner/jdbc/JdbcResultSetMetaData.java#L76
            // The INFORMATION_SCHEMA.COLUMNS table is used to retrieve nullability.
            Map<String, Boolean> nullables = getColumnNullables(conn, tablePath);

            PreparedStatement ps =
                    conn.prepareStatement(
                            String.format("SELECT * FROM %s;", getSchemaTableName(tablePath)));

            ResultSetMetaData resultSetMetaData = ps.getMetaData();

            String[] columnNames = new String[resultSetMetaData.getColumnCount()];
            DataType[] types = new DataType[resultSetMetaData.getColumnCount()];

            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                final String columnName = resultSetMetaData.getColumnName(i);
                columnNames[i - 1] = columnName;
                types[i - 1] = fromJDBCType(tablePath, resultSetMetaData, i);
                if (!nullables.getOrDefault(columnName, true)) {
                    types[i - 1] = types[i - 1].notNull();
                }
            }

            Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, types);
            primaryKey.ifPresent(
                    pk -> schemaBuilder.primaryKeyNamed(pk.getName(), pk.getColumns()));
            Schema tableSchema = schemaBuilder.build();

            return CatalogTable.newBuilder()
                    .schema(tableSchema)
                    .options(getOptions(tablePath))
                    .build();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private List<String> getTables(Connection conn, List<String> schemas) throws SQLException {
        List<String> tables = Lists.newArrayList();
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.`TABLES` "
                                + "WHERE TABLE_TYPE = 'BASE TABLE' "
                                + "AND TABLE_SCHEMA = ? "
                                + "ORDER BY TABLE_NAME;")) {
            for (String schema : schemas) {
                extractColumnValuesByStatement(ps, 1, null, schema).stream()
                        .map(
                                table ->
                                        StringUtils.isNullOrWhitespaceOnly(schema)
                                                ? table
                                                : String.format("%s.%s", schema, table))
                        .forEach(tables::add);
            }
            return tables;
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        final String url = getDatabaseUrl(databaseName);
        try (Connection conn = DriverManager.getConnection(url, connectionProperties)) {
            // get all schemas
            List<String> schemas;
            try (PreparedStatement ps =
                    conn.prepareStatement("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA;")) {
                schemas =
                        extractColumnValuesByStatement(
                                ps, 1, pgSchema -> !builtinSchemas.contains(pgSchema));
            }

            // get all tables
            return getTables(conn, schemas);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list tables for database %s", databaseName), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List<String> tables;
        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
        return tables.contains(getSchemaTableName(tablePath));
    }

    /** Converts Spanner type to Flink {@link DataType}. */
    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return SpannerTablePath.fromFlinkTableName(tablePath.getObjectName()).getSchemaName();
    }

    @Override
    protected String getTableName(ObjectPath tablePath) {
        return SpannerTablePath.fromFlinkTableName(tablePath.getObjectName()).getTableName();
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return SpannerTablePath.fromFlinkTableName(tablePath.getObjectName()).getFullPath();
    }
}
