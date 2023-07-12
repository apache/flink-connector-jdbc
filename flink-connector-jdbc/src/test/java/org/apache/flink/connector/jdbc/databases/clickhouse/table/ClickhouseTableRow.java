package org.apache.flink.connector.jdbc.databases.clickhouse.table;

import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;

import java.util.stream.Collectors;

/** ClickhouseTableRow . */
public class ClickhouseTableRow extends TableRow {

    public ClickhouseTableRow(String name, TableField[] fields) {
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
                "CREATE TABLE %s (%s) %s PRIMARY KEY (%s)",
                getTableName(),
                getStreamFields().map(TableField::asString).collect(Collectors.joining(", ")),
                "ENGINE = MergeTree",
                pkFields);
    }

    @Override
    protected String getDeleteFromQuery() {
        return String.format("truncate table %s", getTableName());
    }
}
