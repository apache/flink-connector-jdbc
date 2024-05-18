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

package org.apache.flink.connector.jdbc.testutils.tables;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.functions.JdbcResultSetBuilder;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Base table operations. * */
public abstract class TableBase<T> implements TableManaged {

    private final String name;
    private final TableField[] fields;

    protected TableBase(String name, TableField[] fields) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "Table name must be defined");
        Preconditions.checkArgument(
                fields != null && fields.length != 0, "Table fields must be defined");
        this.name = name;
        this.fields = fields;
    }

    protected abstract JdbcResultSetBuilder<T> getResultSetBuilder();

    public String getTableName() {
        return name;
    }

    private Stream<TableField> getStreamFields() {
        return Arrays.stream(this.fields);
    }

    private Stream<String> getStreamFieldNames() {
        return getStreamFields().map(TableField::getName);
    }

    private Stream<DataType> getStreamDataTypes() {
        return getStreamFields().map(TableField::getDataType);
    }

    public String[] getTableFields() {
        return getStreamFieldNames().toArray(String[]::new);
    }

    public DataTypes.Field[] getTableDataFields() {
        return getStreamFields()
                .map(field -> DataTypes.FIELD(field.getName(), field.getDataType()))
                .toArray(DataTypes.Field[]::new);
    }

    public DataType[] getTableDataTypes() {
        return getStreamDataTypes().toArray(DataType[]::new);
    }

    public RowTypeInfo getTableRowTypeInfo() {
        TypeInformation<?>[] typesArray =
                getStreamDataTypes()
                        .map(TypeConversions::fromDataTypeToLegacyInfo)
                        .toArray(TypeInformation[]::new);
        String[] fieldsArray = getTableFields();
        return new RowTypeInfo(typesArray, fieldsArray);
    }

    public RowType getTableRowType() {
        LogicalType[] typesArray =
                getStreamDataTypes().map(DataType::getLogicalType).toArray(LogicalType[]::new);
        String[] fieldsArray = getTableFields();
        return RowType.of(typesArray, fieldsArray);
    }

    public int[] getTableTypes() {
        return getStreamDataTypes()
                .map(DataType::getLogicalType)
                .map(LogicalType::getTypeRoot)
                .map(JdbcTypeUtil::logicalTypeToSqlType)
                .mapToInt(x -> x)
                .toArray();
    }

    public Schema getTableSchema() {
        Schema.Builder schema = Schema.newBuilder();
        getStreamFields().forEach(field -> schema.column(field.getName(), field.getDataType()));

        String pkFields =
                getStreamFields()
                        .filter(TableField::isPkField)
                        .map(TableField::getName)
                        .collect(Collectors.joining(", "));
        schema.primaryKeyNamed("PRIMARY", pkFields);

        return schema.build();
    }

    public ResolvedSchema getTableResolvedSchema() {
        return ResolvedSchema.of(
                getStreamFields()
                        .map(field -> Column.physical(field.getName(), field.getDataType()))
                        .collect(Collectors.toList()));
    }

    public String getCreateQuery() {
        String pkFields =
                getStreamFields()
                        .filter(TableField::isPkField)
                        .map(TableField::getName)
                        .collect(Collectors.joining(", "));
        return String.format(
                "CREATE TABLE %s (%s%s)",
                name,
                getStreamFields().map(TableField::asString).collect(Collectors.joining(", ")),
                pkFields.isEmpty() ? "" : String.format(", PRIMARY KEY (%s)", pkFields));
    }

    public String getCreateQueryForFlink(DatabaseMetadata metadata, String newName) {
        return getCreateQueryForFlink(metadata, newName, Collections.emptyList());
    }

    public String getCreateQueryForFlink(
            DatabaseMetadata metadata, String newName, List<String> withParams) {
        return getCreateQueryForFlink(
                metadata, newName, Arrays.asList(getTableFields()), withParams);
    }

    public String getCreateQueryForFlink(
            DatabaseMetadata metadata,
            String newName,
            List<String> newFields,
            List<String> withParams) {

        Map<String, TableField> fieldsMap =
                getStreamFields().collect(Collectors.toMap(TableField::getName, f -> f));

        String fields =
                newFields.stream()
                        .map(fieldsMap::get)
                        .map(field -> String.format("%s %s", field.getName(), field.getDataType()))
                        .collect(Collectors.joining(", "));
        String pkFields =
                getStreamFields()
                        .filter(TableField::isPkField)
                        .map(TableField::getName)
                        .collect(Collectors.joining(", "));

        String primaryKey =
                (pkFields.isEmpty())
                        ? ""
                        : String.format(", PRIMARY KEY (%s) NOT ENFORCED", pkFields);

        List<String> params = new ArrayList<>();
        params.add("'connector'='jdbc'");
        params.add(String.format("'table-name'='%s'", getTableName()));
        params.add(String.format("'url'='%s'", metadata.getJdbcUrl()));
        params.add(String.format("'username'='%s'", metadata.getUsername()));
        params.add(String.format("'password'='%s'", metadata.getPassword()));
        params.addAll(withParams);

        return String.format(
                "CREATE TABLE %s (%s%s) WITH (%s)",
                newName, fields, primaryKey, String.join(", ", params));
    }

    private String getInsertIntoQuery(String... values) {
        return String.format(
                "INSERT INTO %s (%s) VALUES %s",
                name,
                getStreamFieldNames().collect(Collectors.joining(", ")),
                Arrays.stream(values)
                        .map(v -> String.format("(%s)", v))
                        .collect(Collectors.joining(",")));
    }

    public String getInsertIntoQuery() {
        return getInsertIntoQuery(
                getStreamFieldNames().map(x -> "?").collect(Collectors.joining(", ")));
    }

    public String getSelectAllQuery() {
        return String.format(
                "SELECT %s FROM %s", getStreamFieldNames().collect(Collectors.joining(", ")), name);
    }

    protected String getDeleteFromQuery() {
        return String.format("DELETE FROM %s", name);
    }

    public String getDropTableQuery() {
        return String.format("DROP TABLE %s", name);
    }

    public void createTable(Connection conn) throws SQLException {
        executeUpdate(conn, getCreateQuery());
    }

    public void insertIntoTableValues(Connection conn, String... values) throws SQLException {
        executeUpdate(conn, getInsertIntoQuery(values));
    }

    public List<T> selectAllTable(DatabaseMetadata metadata) throws SQLException {
        try (Connection conn = metadata.getConnection()) {
            return selectAllTable(conn);
        }
    }

    public List<T> selectAllTable(Connection conn) throws SQLException {
        return executeStatement(conn, getSelectAllQuery(), getResultSetBuilder());
    }

    public void deleteTable(Connection conn) throws SQLException {
        executeUpdate(conn, getDeleteFromQuery());
    }

    public void dropTable(Connection conn) throws SQLException {
        executeUpdate(conn, getDropTableQuery());
    }

    protected void executeUpdate(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(sql);
        }
    }

    protected <T> List<T> executeStatement(
            Connection conn, String sql, JdbcResultSetBuilder<T> rsGetter) throws SQLException {
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            return rsGetter.accept(rs);
        }
    }

    protected <T> int[] executeStatement(
            Connection conn, String sql, JdbcStatementBuilder<T> psSetter, List<T> values)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (T value : values) {
                psSetter.accept(ps, value);
                ps.addBatch();
            }
            return ps.executeBatch();
        }
    }

    protected <T> T getNullable(ResultSet rs, FunctionWithException<ResultSet, T, SQLException> get)
            throws SQLException {
        T value = get.apply(rs);
        return getNullable(rs, value);
    }

    protected <T> T getNullable(ResultSet rs, T value) throws SQLException {
        return rs.wasNull() ? null : value;
    }
}
