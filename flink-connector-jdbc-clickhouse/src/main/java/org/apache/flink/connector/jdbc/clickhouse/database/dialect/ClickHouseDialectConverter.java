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

package org.apache.flink.connector.jdbc.clickhouse.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * ClickHouse.
 */
@Internal
public class ClickHouseDialectConverter extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    public ClickHouseDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();

        if (root == LogicalTypeRoot.ARRAY) {
            ArrayType arrayType = (ArrayType) type;
            return createClickHouseArrayConverter(arrayType);
        } else if (root == LogicalTypeRoot.MAP) {
            MapType mapType = (MapType) type;
            return createClickHouseMapConverter(mapType);
        } else {
            return createPrimitiveConverter(type);
        }
    }

    @Override
    public JdbcSerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case MAP:
                return (val, index, statement) ->
                        statement.setObject(index, toExternalSerializer(val.getMap(index), type));
            case ARRAY:
                return (val, index, statement) ->
                        statement.setObject(index, toExternalSerializer(val.getArray(index), type));
            default:
                return super.createExternalConverter(type);
        }
    }

    // adding support to MAP and ARRAY types
    private static Object toExternalSerializer(Object value, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return value;
            case TINYINT:
                return value instanceof Number ? ((Number) value).byteValue() : value;
            case SMALLINT:
                return value instanceof Number ? ((Number) value).shortValue() : value;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return value instanceof Number ? ((Number) value).intValue() : value;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return value instanceof Number ? ((Number) value).longValue() : value;
            case FLOAT:
                return value instanceof Number ? ((Number) value).floatValue() : value;
            case DOUBLE:
                return value instanceof Number ? ((Number) value).doubleValue() : value;
            case BINARY:
            case VARBINARY:
                throw new UnsupportedOperationException(
                        "BINARY/VARBINARY types are not supported by ClickHouse dialect. "
                                + "Use STRING instead.");
            case CHAR:
            case VARCHAR:
                return value.toString();
            case DATE:
                return Date.valueOf(LocalDate.ofEpochDay((int) value));
            case TIME_WITHOUT_TIME_ZONE:
                return Time.valueOf(LocalTime.ofNanoOfDay((int) value * 1_000_000L));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampData) value).toTimestamp();
            case DECIMAL:
                return ((DecimalData) value).toBigDecimal();
            case ARRAY:
                LogicalType elementType =
                        ((ArrayType) type)
                                .getChildren().stream()
                                        .findFirst()
                                        .orElseThrow(
                                                () ->
                                                        new RuntimeException(
                                                                "Unknown array element type"));
                ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
                ArrayData arrayData = ((ArrayData) value);
                Object[] objectArray = new Object[arrayData.size()];
                for (int i = 0; i < arrayData.size(); i++) {
                    objectArray[i] =
                            toExternalSerializer(
                                    elementGetter.getElementOrNull(arrayData, i), elementType);
                }
                return objectArray;
            case MAP:
                LogicalType keyType = ((MapType) type).getKeyType();
                LogicalType valueType = ((MapType) type).getValueType();
                ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
                ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
                MapData mapData = (MapData) value;
                ArrayData keyArrayData = mapData.keyArray();
                ArrayData valueArrayData = mapData.valueArray();
                Map<Object, Object> objectMap = new HashMap<>(keyArrayData.size());
                for (int i = 0; i < keyArrayData.size(); i++) {
                    objectMap.put(
                            toExternalSerializer(
                                    keyGetter.getElementOrNull(keyArrayData, i), keyType),
                            toExternalSerializer(
                                    valueGetter.getElementOrNull(valueArrayData, i), valueType));
                }
                return objectMap;
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private JdbcDeserializationConverter createClickHouseArrayConverter(ArrayType arrayType) {
        final LogicalType elementType =
                arrayType.getChildren().stream()
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("Unknown array element type"));
        final JdbcDeserializationConverter elementConverter =
                createNullableInternalConverter(elementType);
        return val -> {
            Object[] raw;
            if (val instanceof java.sql.Array) {
                raw = (Object[]) ((java.sql.Array) val).getArray();
            } else if (val instanceof Object[]) {
                raw = (Object[]) val;
            } else {
                throw new RuntimeException("Unsupported array type: " + val.getClass());
            }
            Object[] converted = new Object[raw.length];
            for (int i = 0; i < raw.length; i++) {
                converted[i] = elementConverter.deserialize(raw[i]);
            }
            return new GenericArrayData(converted);
        };
    }

    private JdbcDeserializationConverter createClickHouseMapConverter(MapType mapType) {
        final LogicalType keyType = mapType.getKeyType();
        final LogicalType valueType = mapType.getValueType();
        final JdbcDeserializationConverter keyConverter = createNullableInternalConverter(keyType);
        final JdbcDeserializationConverter valueConverter =
                createNullableInternalConverter(valueType);
        return val -> {
            Map<?, ?> rawMap = (Map<?, ?>) val;
            Map<Object, Object> result = new HashMap<>(rawMap.size());
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                Object k = entry.getKey();
                Object v = entry.getValue();
                result.put(k == keyConverter.deserialize(k), v == valueConverter.deserialize(v));
            }
            return new GenericMapData(result);
        };
    }

    private JdbcDeserializationConverter createPrimitiveConverter(LogicalType type) {
        return super.createInternalConverter(type);
    }

    @Override
    public String converterName() {
        return "ClickHouse";
    }
}
