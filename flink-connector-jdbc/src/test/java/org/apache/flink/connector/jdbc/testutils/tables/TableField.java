package org.apache.flink.connector.jdbc.testutils.tables;

import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

/** Table field. * */
public class TableField {
    private final String name;
    private final DbType dbType;
    private final DataType dataType;
    private final boolean pkField;

    protected TableField(String name, DataType dataType, DbType dbType, boolean pkField) {
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
        String fieldType = (this.dbType != null) ? this.dbType.toString() : this.dataType.toString();
        return String.format("%s %s", this.name, fieldType);
    }

    @Override
    public String toString() {
        return asString();
    }

    public static class DbType {
        private final String type;
        private Boolean nullable = true;

        public DbType(String type) {
            this.type = type;
        }

        public DbType notNull() {
            this.nullable = false;
            return this;
        }

        @Override
        public String toString() {
            return String.format("%s%s", this.type, this.nullable ? "" : " NOT NULL");
        }
    }
}
