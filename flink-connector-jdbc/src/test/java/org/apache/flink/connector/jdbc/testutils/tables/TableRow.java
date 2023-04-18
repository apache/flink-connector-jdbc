package org.apache.flink.connector.jdbc.testutils.tables;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.functions.JdbcResultSetBuilder;
import org.apache.flink.table.api.DataTypes;
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
            DataTypes.Field[] fields = getTableDataFields();
            while (rs.next()) {
                Row row = new Row(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    row.setField(
                            i, rs.getObject(i + 1, fields[i].getDataType().getConversionClass()));
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
