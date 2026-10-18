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

package org.apache.flink.connector.jdbc.clickhouse.testutils;

import org.apache.flink.connector.jdbc.testutils.functions.JdbcResultSetBuilder;
import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** ClickHouseTableRow. */
public class ClickHouseTableRow extends TableRow {
    public ClickHouseTableRow(String name, TableField[] fields) {
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
                "CREATE TABLE %s (%s) %s PRIMARY KEY (%s);",
                getTableName(),
                getStreamFields().map(TableField::asString).collect(Collectors.joining(", ")),
                "ENGINE = MergeTree",
                pkFields);
    }

    @Override
    protected String getDeleteFromQuery() {
        return String.format("TRUNCATE TABLE %s;", getTableName());
    }

    @Override
    protected String getInsertIntoQuery(String... values) {
        return String.format(
                "INSERT INTO %s (%s) VALUES %s;",
                getTableName(),
                getStreamFieldNames().collect(Collectors.joining(", ")),
                Arrays.stream(values)
                        .map(v -> String.format("(%s)", v))
                        .collect(Collectors.joining(",")));
    }

    @Override
    public String getSelectAllQuery() {
        return String.format(
                "SELECT %s FROM %s;",
                getStreamFieldNames().collect(Collectors.joining(", ")), getTableName());
    }

    @Override
    public String getDropTableQuery() {
        return String.format("DROP TABLE IF EXISTS %s;", getTableName());
    }

    @Override
    protected JdbcResultSetBuilder<Row> getResultSetBuilder() {
        return (rs) -> {
            List<Row> result = new ArrayList<>();
            DataTypes.Field[] fields = getTableDataFields();
            while (rs.next()) {
                Row row = new Row(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    Class<?> conversionClass = fields[i].getDataType().getConversionClass();
                    Object dbValue;
                    if (conversionClass.equals(LocalTime.class)) {
                        dbValue = rs.getTime(i + 1);
                    } else if (conversionClass.equals(LocalDate.class)) {
                        dbValue = rs.getDate(i + 1);
                    } else if (conversionClass.equals(LocalDateTime.class)) {
                        dbValue = rs.getTimestamp(i + 1);
                    } else if (conversionClass.isArray()) {
                        dbValue = readArrayColumn(rs, i + 1);
                    } else if (Map.class.isAssignableFrom(conversionClass)) {
                        // read untyped; ClickHouse's driver already returns a
                        // java.util.Map for MAP columns when not forced into a
                        // specific conversion class.
                        dbValue = rs.getObject(i + 1);
                    } else {
                        dbValue = rs.getObject(i + 1, conversionClass);
                    }
                    row.setField(i, getNullable(rs, dbValue));
                }
                result.add(row);
            }
            return result;
        };
    }

    private static Object[] readArrayColumn(ResultSet rs, int columnIndex) throws SQLException {
        Object raw = rs.getObject(columnIndex);
        if (raw == null) {
            return null;
        }
        if (raw instanceof Array) {
            Object arr = ((Array) raw).getArray();
            return (Object[]) arr;
        }
        if (raw instanceof java.util.List) {
            return ((java.util.List<?>) raw).toArray();
        }
        if (raw.getClass().isArray()) {
            return (Object[]) raw;
        }
        throw new IllegalStateException(
                "Unexpected representation for array column: " + raw.getClass());
    }
}
