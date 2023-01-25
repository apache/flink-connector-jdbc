package org.apache.flink.connector.jdbc.templates;

/** Table for manual creation with query. * */
public class TableManual implements TableManaged {

    private final String name;
    private final String queryCreate;

    private TableManual(String name, String queryCreate) {
        this.name = name;
        this.queryCreate = queryCreate;
    }

    public static TableManual of(String name, String queryCreate) {
        return new TableManual(name, queryCreate);
    }

    public String getTableName() {
        return this.name;
    }

    @Override
    public String getCreateQuery() {
        return this.queryCreate;
    }
}
