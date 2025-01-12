package org.apache.flink.connector.jdbc.spanner.testutils.tables;

import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;

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
