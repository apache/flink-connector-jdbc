package org.apache.flink.connector.jdbc.spanner.testutils.tables;

import org.apache.flink.connector.jdbc.testutils.functions.JdbcResultSetBuilder;
import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** TableRow for Spanner. */
public class SpannerTableRow extends TableRow {

    public static SpannerTableRow spannerTableRow(String name, TableField... fields) {
        return new SpannerTableRow(name, fields);
    }

    protected SpannerTableRow(String name, TableField[] fields) {
        super(name, fields);
    }

    @Override
    protected JdbcResultSetBuilder<Row> getResultSetBuilder() {
        return (rs) -> {
            List<Row> result = new ArrayList<>();
            DataTypes.Field[] fields = getTableDataFields();
            while (rs.next()) {
                Row row = new Row(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    Object dbValue;
                    Class<?> conversionClass = fields[i].getDataType().getConversionClass();
                    // Spanner arrays need special handling
                    if (fields[i].getDataType().getLogicalType().getTypeRoot()
                            == LogicalTypeRoot.ARRAY) {
                        Array sqlArray = rs.getArray(i + 1);
                        dbValue = sqlArray != null ? sqlArray.getArray() : null;
                    } else if (conversionClass.equals(java.time.LocalTime.class)) {
                        dbValue = rs.getTime(i + 1);
                    } else if (conversionClass.equals(java.time.LocalDate.class)) {
                        dbValue = rs.getDate(i + 1);
                    } else if (conversionClass.equals(java.time.LocalDateTime.class)) {
                        dbValue = rs.getTimestamp(i + 1);
                    } else {
                        dbValue = rs.getObject(i + 1, conversionClass);
                    }
                    row.setField(i, getNullable(rs, dbValue));
                }
                result.add(row);
            }
            return result;
        };
    }

    @Override
    public String getCreateQuery() {
        String pkFields =
                getStreamFields()
                        .filter(TableField::isPkField)
                        .map(TableField::getName)
                        .collect(Collectors.joining(", "));
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s) %s",
                name,
                getStreamFields().map(TableField::asString).collect(Collectors.joining(", ")),
                pkFields.isEmpty() ? "" : String.format("PRIMARY KEY (%s)", pkFields));
    }

    @Override
    protected String getDeleteFromQuery() {
        return String.format("DELETE FROM %s WHERE true", name);
    }

    @Override
    public String getDropTableQuery() {
        return String.format("DROP TABLE IF EXISTS %s", name);
    }
}
