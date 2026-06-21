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

import java.lang.reflect.Array;
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
    protected JdbcResultSetBuilder<Row> getResultSetBuilder() {
        return (rs) -> {
            List<Row> result = new ArrayList<>();
            DataTypes.Field[] fields = getTableDataFields();
            while (rs.next()) {
                Row row = new Row(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    Object dbValue;
                    Class<?> conversionClass = fields[i].getDataType().getConversionClass();
                    if (conversionClass.equals(LocalTime.class)) {
                        dbValue = rs.getTime(i + 1);
                    } else if (conversionClass.equals(LocalDate.class)) {
                        dbValue = rs.getDate(i + 1);
                    } else if (conversionClass.equals(LocalDateTime.class)) {
                        java.sql.Timestamp ts = rs.getTimestamp(i + 1);
                        dbValue = ts == null ? null : ts.toLocalDateTime();
                    } else if (conversionClass.isArray()) {
                        dbValue = readArray(rs, i + 1, conversionClass);
                    } else if (Map.class.isAssignableFrom(conversionClass)) {
                        dbValue = readMap(rs, i + 1);
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

    private Object readArray(ResultSet rs, int columnIndex, Class<?> conversionClass)
            throws SQLException {
        java.sql.Array sqlArray = rs.getArray(columnIndex);
        if (sqlArray == null) {
            return null;
        }
        Object raw = sqlArray.getArray();
        Class<?> componentType = conversionClass.getComponentType();
        int length = Array.getLength(raw);
        Object typedArray = Array.newInstance(componentType, length);
        for (int j = 0; j < length; j++) {
            Object element = Array.get(raw, j);
            Array.set(typedArray, j, coerce(element, componentType));
        }
        return typedArray;
    }

    private Object readMap(ResultSet rs, int columnIndex) throws SQLException {
        Object raw = rs.getObject(columnIndex);
        if (raw == null) {
            return null;
        }
        if (raw instanceof Map) {
            return raw;
        }
        throw new SQLException(
                "Unexpected map representation returned by clickhouse driver: " + raw.getClass());
    }

    @SuppressWarnings("unchecked")
    private <T> T coerce(Object value, Class<T> targetType) {
        if (value == null) {
            return null;
        }
        if (targetType.isInstance(value)) {
            return (T) value;
        }
        if (targetType.equals(String.class)) {
            return (T) value.toString();
        }
        if (Number.class.isAssignableFrom(targetType) && value instanceof Number) {
            Number n = (Number) value;
            if (targetType.equals(Integer.class)) {
                return (T) Integer.valueOf(n.intValue());
            } else if (targetType.equals(Long.class)) {
                return (T) Long.valueOf(n.longValue());
            } else if (targetType.equals(Short.class)) {
                return (T) Short.valueOf(n.shortValue());
            } else if (targetType.equals(Double.class)) {
                return (T) Double.valueOf(n.doubleValue());
            } else if (targetType.equals(Float.class)) {
                return (T) Float.valueOf(n.floatValue());
            }
        }
        throw new ClassCastException(
                "Cannot coerce value of type "
                        + value.getClass()
                        + " to "
                        + targetType
                        + " (value: "
                        + value
                        + ")");
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
}
