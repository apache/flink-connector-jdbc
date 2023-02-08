package org.apache.flink.connector.jdbc.testutils.tables;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.functions.JdbcResultSetBuilder;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
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
import java.util.List;
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

    private Stream<String> getStreamFieldNames() {
        return Arrays.stream(this.fields).map(TableField::getName);
    }

    private Stream<DataType> getStreamDataTypes() {
        return Arrays.stream(this.fields).map(TableField::getDataType);
    }

    public String[] getTableFields() {
        return getStreamFieldNames().toArray(String[]::new);
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

    protected String getCreateQuery() {
        String pkFields =
                Arrays.stream(this.fields)
                        .filter(TableField::isPkField)
                        .map(TableField::getName)
                        .collect(Collectors.joining(", "));
        return String.format(
                "CREATE TABLE %s (%s%s)",
                name,
                Arrays.stream(this.fields)
                        .map(TableField::asString)
                        .collect(Collectors.joining(", ")),
                pkFields.isEmpty() ? "" : String.format(", PRIMARY KEY (%s)", pkFields));
    }

    public String getCreateQueryForFlink(
            DatabaseMetadata metadata, String newName, String... withParams) {

        String fields =
                Arrays.stream(this.fields)
                        .map(field -> String.format("%s %s", field.getName(), field.getDataType()))
                        .collect(Collectors.joining(", "));
        List<String> params = new ArrayList<>();
        params.add("'connector'='jdbc'");
        params.add(String.format("'table-name'='%s'", getTableName()));
        params.add(String.format("'url'='%s'", metadata.getJdbcUrl()));
        params.add(String.format("'username'='%s'", metadata.getUsername()));
        params.add(String.format("'password'='%s'", metadata.getPassword()));
        params.addAll(Arrays.asList(withParams));

        return String.format(
                "CREATE TABLE %s (%s) WITH (%s)", newName, fields, String.join(", ", params));
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

    protected String getDropTableQuery() {
        return String.format("DROP TABLE %s", name);
    }

    public void createTable(Connection conn) throws SQLException {
        executeUpdate(conn, getCreateQuery());
    }

    public void insertIntoTableValues(Connection conn, String... values) throws SQLException {
        executeUpdate(conn, getInsertIntoQuery(values));
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
        return rs.wasNull() ? null : value;
    }
}
