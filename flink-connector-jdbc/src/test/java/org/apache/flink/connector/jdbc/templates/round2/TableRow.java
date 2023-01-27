package org.apache.flink.connector.jdbc.templates.round2;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Row table. * */
public class TableRow extends TableBase<Row> {

    protected TableRow(String name, TableField[] fields) {
        super(name, fields);
    }

    protected JdbcResultSetBuilder<Row> getResultSetBuilder() {
        return (rs) -> {
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
    }

    public void checkContent(DatabaseMetadata metadata, Row... content) throws SQLException {
        try (Connection dbConn = metadata.getConnection()) {
            String[] results =
                    selectAllTable(dbConn).stream()
                            .map(Row::toString)
                            .sorted()
                            .toArray(String[]::new);

            assertThat(results)
                    .isEqualTo(
                            Arrays.stream(content)
                                    .map(Row::toString)
                                    .sorted()
                                    .toArray(String[]::new));
        }
    }
}
