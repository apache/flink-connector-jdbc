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

package org.apache.flink.connector.jdbc.spanner.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * Spanner.
 */
@Internal
public class SpannerDialectConverter extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "Spanner";
    }

    public SpannerDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();

        if (root == LogicalTypeRoot.TINYINT
                || root == LogicalTypeRoot.SMALLINT
                || root == LogicalTypeRoot.INTEGER) {
            // Spanner only has INT64, so the driver always returns Long values. Narrow them to the
            // requested type and fail on overflow instead of silently truncating.
            return val -> narrowInt64(((Number) val).longValue(), root);
        } else if (root == LogicalTypeRoot.ARRAY) {
            ArrayType arrayType = (ArrayType) type;
            return createSpannerArrayConverter(arrayType);
        } else if (root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            // Spanner TIMESTAMP is an absolute instant, so convert it via Instant to keep the
            // value independent of the JVM default time zone.
            return val ->
                    val instanceof LocalDateTime
                            ? TimestampData.fromLocalDateTime((LocalDateTime) val)
                            : TimestampData.fromInstant(((Timestamp) val).toInstant());
        } else {
            return super.createInternalConverter(type);
        }
    }

    @Override
    protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
        if (type.getTypeRoot() == LogicalTypeRoot.ARRAY) {
            ArrayType arrayType = (ArrayType) type;
            LogicalType elementType = arrayType.getElementType();
            String typeName = getSpannerArrayTypeName(elementType);

            return (val, index, statement) -> {
                ArrayData arrayData = val.getArray(index);
                int size = arrayData.size();
                Object[] elements = new Object[size];

                // Convert each element
                for (int i = 0; i < size; i++) {
                    elements[i] = extractArrayElement(arrayData, i, elementType);
                }

                // Create JDBC Array and set it
                java.sql.Array jdbcArray = statement.createArrayOf(typeName, elements);
                statement.setArray(index, jdbcArray);
            };
        } else {
            return super.createExternalConverter(type);
        }
    }

    @Override
    protected JdbcSerializationConverter createNullableExternalConverter(LogicalType type) {
        if (type.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            // Spanner stores TIMESTAMP as an absolute instant, so write TIMESTAMP_LTZ via Instant.
            // JdbcTypeUtil has no mapping for TIMESTAMP_LTZ, so the null branch is handled here
            // with Types.TIMESTAMP instead of the parent's nullable wrapper.
            final int precision = LogicalTypeChecks.getPrecision(type);
            return (val, index, statement) -> {
                if (val == null || val.isNullAt(index)) {
                    statement.setNull(index, Types.TIMESTAMP);
                } else {
                    statement.setTimestamp(
                            index, Timestamp.from(val.getTimestamp(index, precision).toInstant()));
                }
            };
        } else {
            return super.createNullableExternalConverter(type);
        }
    }

    private static Object narrowInt64(long value, LogicalTypeRoot root) {
        switch (root) {
            case TINYINT:
                if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                    throw new ArithmeticException("TINYINT overflow: " + value);
                }
                return (byte) value;
            case SMALLINT:
                if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                    throw new ArithmeticException("SMALLINT overflow: " + value);
                }
                return (short) value;
            case INTEGER:
                return Math.toIntExact(value);
            default:
                throw new IllegalArgumentException("Unsupported integer type: " + root);
        }
    }

    private JdbcDeserializationConverter createSpannerArrayConverter(ArrayType arrayType) {
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        final JdbcDeserializationConverter elementConverter =
                createNullableInternalConverter(arrayType.getElementType());
        return val -> {
            java.sql.Array spannerArray = (java.sql.Array) val;
            Object[] in = (Object[]) spannerArray.getArray();
            final Object[] array = (Object[]) Array.newInstance(elementClass, in.length);
            for (int i = 0; i < in.length; i++) {
                array[i] = elementConverter.deserialize(in[i]);
            }
            return new GenericArrayData(array);
        };
    }

    /**
     * Get Spanner array type name from Flink LogicalType.
     *
     * @param elementType The element type of the array
     * @return Spanner type name for the array elements
     */
    private String getSpannerArrayTypeName(LogicalType elementType) {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                return "BOOL";
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return "INT64";
            case FLOAT:
                return "FLOAT32";
            case DOUBLE:
                return "FLOAT64";
            case DECIMAL:
                return "NUMERIC";
            case CHAR:
            case VARCHAR:
                return "STRING";
            case BINARY:
            case VARBINARY:
                return "BYTES";
            case DATE:
                return "DATE";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "TIMESTAMP";
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported array element type for Spanner: %s", elementType));
        }
    }

    /**
     * Extract an element from ArrayData based on the element type.
     *
     * @param arrayData The array data
     * @param index The index of the element
     * @param elementType The type of the element
     * @return The extracted element value
     */
    private Object extractArrayElement(ArrayData arrayData, int index, LogicalType elementType) {
        if (arrayData.isNullAt(index)) {
            return null;
        }

        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                return arrayData.getBoolean(index);
            case TINYINT:
                return (long) arrayData.getByte(index);
            case SMALLINT:
                return (long) arrayData.getShort(index);
            case INTEGER:
                return (long) arrayData.getInt(index);
            case BIGINT:
                return arrayData.getLong(index);
            case FLOAT:
                return arrayData.getFloat(index);
            case DOUBLE:
                return arrayData.getDouble(index);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) elementType;
                return arrayData
                        .getDecimal(index, decimalType.getPrecision(), decimalType.getScale())
                        .toBigDecimal();
            case CHAR:
            case VARCHAR:
                return arrayData.getString(index).toString();
            case BINARY:
            case VARBINARY:
                return arrayData.getBinary(index);
            case DATE:
                return java.sql.Date.valueOf(
                        java.time.LocalDate.ofEpochDay(arrayData.getInt(index)));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return arrayData
                        .getTimestamp(index, LogicalTypeChecks.getPrecision(elementType))
                        .toTimestamp();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.from(
                        arrayData
                                .getTimestamp(index, LogicalTypeChecks.getPrecision(elementType))
                                .toInstant());
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported array element type for Spanner: %s", elementType));
        }
    }
}
