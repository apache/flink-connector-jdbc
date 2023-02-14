/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.testutils.tables;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.functions.JdbcResultSetBuilder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
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
                    Object dbValue;
                    if (fields[i].getDataType().getConversionClass() == LocalDateTime.class) {
                        dbValue = rs.getTimestamp(i + 1);
                    } else {
                        dbValue = rs.getObject(i + 1, fields[i].getDataType().getConversionClass());
                    }
                    row.setField(i, dbValue);
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
