package org.apache.flink.connector.jdbc.templates;

import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Table for manual creation with query. * */
public class TableManual implements TableManaged, TableChecker {

    private final String name;
    private final String queryCreate;

    private final TableManaged.JdbcResultSetBuilder<Row> resultSetBuilder =
            (rs) -> {
                List<Row> result = new ArrayList<>();
                while (rs.next()) {
                    int fieldsNumber = rs.getMetaData().getColumnCount();
                    Row row = new Row(fieldsNumber);
                    for (int i = 0; i < fieldsNumber; i++) {
                        row.setField(i, rs.getObject(i + 1));
                    }
                    result.add(row);
                }
                return result;
            };

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

    public String getSelectAllQuery() {
        return String.format("select * from %s", getTableName());
    }

    public List<Row> selectAllTable(Connection conn) throws SQLException {
        return executeStatement(conn, getSelectAllQuery(), resultSetBuilder);
    }
}
