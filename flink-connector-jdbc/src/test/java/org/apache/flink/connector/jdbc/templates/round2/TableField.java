package org.apache.flink.connector.jdbc.templates.round2;

import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

/** Table field. * */
public class TableField {
    private final String name;
    private final String dbType;
    private final DataType dataType;
    private final boolean pkField;

    protected TableField(String name, DataType dataType, String dbType, boolean pkField) {
        Preconditions.checkNotNull(name, "Column name can not be null.");
        Preconditions.checkNotNull(dataType, "Column data type can not be null.");
        this.name = name;
        this.dataType = dataType;
        this.dbType = dbType;
        this.pkField = pkField;
    }

    public String getName() {
        return this.name;
    }

    public DataType getDataType() {
        return this.dataType;
    }

    public boolean isPkField() {
        return pkField;
    }

    public String asString() {
        String fieldType = this.dataType.toString();
        if (dbType != null) {
            String nullable = this.dataType.getLogicalType().isNullable() ? "" : " NOT NULL";
            fieldType = String.format("%s%s", dbType, nullable);
        }

        return String.format("%s %s", this.name, fieldType);
    }

    @Override
    public String toString() {
        return asString();
    }
}
