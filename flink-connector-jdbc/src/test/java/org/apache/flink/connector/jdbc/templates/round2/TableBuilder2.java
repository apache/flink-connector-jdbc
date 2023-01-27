package org.apache.flink.connector.jdbc.templates.round2;

import org.apache.flink.table.types.DataType;

/** Table builder. * */
public final class TableBuilder2 {

    public static TableRow tableRow(String name, TableField... fields) {
        return new TableRow(name, fields);
    }

    public static TableField field(String name, DataType dataType) {
        return field(name, null, dataType);
    }

    public static TableField field(String name, String dbType, DataType dataType) {
        return createField(name, dbType, dataType, false);
    }

    public static TableField pkField(String name, DataType dataType) {
        return pkField(name, null, dataType);
    }

    public static TableField pkField(String name, String dbType, DataType dataType) {
        return createField(name, dbType, dataType, true);
    }

    private static TableField createField(
            String name, String dbType, DataType dataType, boolean pkField) {
        return new TableField(name, dataType, dbType, pkField);
    }
}
