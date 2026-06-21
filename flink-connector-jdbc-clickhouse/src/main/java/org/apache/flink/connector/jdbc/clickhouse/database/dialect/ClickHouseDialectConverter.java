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
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
        LogicalTypeRoot root = type.getTypeRoot();

        if (root == LogicalTypeRoot.ARRAY) {
            return (val, index, statement) ->
                    statement.setObject(index, toExternalSerializer(val.getArray(index), type));
        } else if (root == LogicalTypeRoot.MAP) {
            return (val, index, statement) ->
                    statement.setObject(index, toExternalSerializer(val.getMap(index), type));
        } else {
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
                ArrayType arrayType = (ArrayType) type;
                final LogicalType elementType = arrayType.getElementType();
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

    // creating separate array converter
    private JdbcDeserializationConverter createClickHouseArrayConverter(ArrayType arrayType) {
        final LogicalType elementType = arrayType.getElementType();
        final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(elementType);
        final JdbcDeserializationConverter elementConverter =
                createNullableInternalConverter(elementType);
        return val -> {
            java.sql.Array chArray = (java.sql.Array) val;
            Object[] in = (Object[]) chArray.getArray();
            final Object[] converted = (Object[]) Array.newInstance(elementClass, in.length);
            for (int i = 0; i < in.length; i++) {
                converted[i] = elementConverter.deserialize(in[i]);
            }
            return new GenericArrayData(converted);
        };
    }

    // creating separate map converter
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

    // some types should be properly handled for clickhouse
    private JdbcDeserializationConverter createPrimitiveConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
                return val -> val instanceof Number ? ((Number) val).byteValue() : val;
            case SMALLINT:
                return val -> val instanceof Number ? ((Number) val).shortValue() : val;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return val -> val instanceof Number ? ((Number) val).intValue() : val;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return val -> val instanceof Number ? ((Number) val).longValue() : val;
            case FLOAT:
                return val -> val instanceof Number ? ((Number) val).floatValue() : val;
            case DOUBLE:
                return val -> val instanceof Number ? ((Number) val).doubleValue() : val;
            case DATE:
                return val ->
                        val instanceof Date
                                ? (int) (((Date) val).toLocalDate().toEpochDay())
                                : val instanceof LocalDate
                                        ? (int) ((LocalDate) val).toEpochDay()
                                        : val;
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        val instanceof Time
                                ? (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L)
                                : val instanceof LocalTime
                                        ? (int) (((LocalTime) val).toNanoOfDay() / 1_000_000L)
                                        : val;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val ->
                        val instanceof Timestamp
                                ? TimestampData.fromTimestamp((Timestamp) val)
                                : val instanceof LocalDateTime
                                        ? TimestampData.fromLocalDateTime((LocalDateTime) val)
                                        : val;
            default:
                return super.createInternalConverter(type);
        }
    }

    @Override
    public String converterName() {
        return "ClickHouse";
    }
}
