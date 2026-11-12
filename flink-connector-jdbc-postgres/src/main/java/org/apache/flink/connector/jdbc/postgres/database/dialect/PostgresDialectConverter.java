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

package org.apache.flink.connector.jdbc.postgres.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * PostgreSQL.
 */
@Internal
public class PostgresDialectConverter extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    protected PostgresDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();

        if (root == LogicalTypeRoot.ARRAY) {
            ArrayType arrayType = (ArrayType) type;
            return createPostgresArrayConverter(arrayType);
        } else {
            return createPrimitiveConverter(type);
        }
    }

    @Override
    protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
        if (type.getTypeRoot() == LogicalTypeRoot.ARRAY) {
            // NULL arrays are handled by the parent's nullable converter.
            return createPostgresArrayExternalConverter((ArrayType) type);
        } else {
            return super.createExternalConverter(type);
        }
    }

    private JdbcSerializationConverter createPostgresArrayExternalConverter(ArrayType arrayType) {
        final LogicalType elementType = arrayType.getElementType();
        final ArrayElementWriter elementWriter = createArrayElementWriter(elementType);
        if (elementWriter == null) {
            // Fail on write only, since converters are also created for read-only tables.
            final String converterName = converterName();
            return (val, index, statement) -> {
                throw new UnsupportedOperationException(
                        String.format(
                                "Writing ARRAY with element type %s is not supported in JDBC:%s.",
                                elementType, converterName));
            };
        }
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        return (val, index, statement) -> {
            final ArrayData arrayData = val.getArray(index);
            // PgJDBC rejects byte[] elements in an Object[], so always create a typed array.
            final Object[] elements =
                    (Object[]) Array.newInstance(elementWriter.jdbcClass, arrayData.size());
            for (int i = 0; i < elements.length; i++) {
                final Object element = elementGetter.getElementOrNull(arrayData, i);
                elements[i] = element == null ? null : elementWriter.converter.convert(element);
            }
            statement.setArray(index, statement.createArrayOf(elementWriter.typeName, elements));
        };
    }

    /**
     * Returns how to write the elements of an array, mirroring the conversions of {@link
     * #createExternalConverter} for single values, or null if the element type is not supported.
     */
    private ArrayElementWriter createArrayElementWriter(LogicalType elementType) {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                return new ArrayElementWriter("bool", Boolean.class, element -> element);
            case TINYINT:
                // PostgreSQL has no single-byte integer type.
                return new ArrayElementWriter(
                        "int2", Short.class, element -> ((Byte) element).shortValue());
            case SMALLINT:
                return new ArrayElementWriter("int2", Short.class, element -> element);
            case INTEGER:
                return new ArrayElementWriter("int4", Integer.class, element -> element);
            case BIGINT:
                return new ArrayElementWriter("int8", Long.class, element -> element);
            case FLOAT:
                return new ArrayElementWriter("float4", Float.class, element -> element);
            case DOUBLE:
                return new ArrayElementWriter("float8", Double.class, element -> element);
            case DECIMAL:
                return new ArrayElementWriter(
                        "numeric",
                        BigDecimal.class,
                        element -> ((DecimalData) element).toBigDecimal());
            case CHAR:
            case VARCHAR:
                return new ArrayElementWriter("text", String.class, Object::toString);
            case BINARY:
            case VARBINARY:
                return new ArrayElementWriter("bytea", byte[].class, element -> element);
            case DATE:
                return new ArrayElementWriter(
                        "date",
                        Date.class,
                        element -> Date.valueOf(LocalDate.ofEpochDay((Integer) element)));
            case TIME_WITHOUT_TIME_ZONE:
                // java.sql.Time drops fractional seconds, so pass the time as a string.
                return new ArrayElementWriter(
                        "time",
                        String.class,
                        element ->
                                LocalTime.ofNanoOfDay((Integer) element * 1_000_000L).toString());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new ArrayElementWriter(
                        "timestamp",
                        Timestamp.class,
                        element -> ((TimestampData) element).toTimestamp());
            default:
                return null;
        }
    }

    /** Converts a non-null Flink internal array element to a JDBC object. */
    @FunctionalInterface
    private interface ElementConverter extends Serializable {
        Object convert(Object element);
    }

    /** PostgreSQL type name, JDBC class and converter for the elements of an array to write. */
    private static final class ArrayElementWriter implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String typeName;
        private final Class<?> jdbcClass;
        private final ElementConverter converter;

        private ArrayElementWriter(
                String typeName, Class<?> jdbcClass, ElementConverter converter) {
            this.typeName = typeName;
            this.jdbcClass = jdbcClass;
            this.converter = converter;
        }
    }

    private JdbcDeserializationConverter createPostgresArrayConverter(ArrayType arrayType) {
        // Since PGJDBC 42.2.15 (https://github.com/pgjdbc/pgjdbc/pull/1194) bytea[] is wrapped in
        // primitive byte arrays
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        final JdbcDeserializationConverter elementConverter =
                createNullableInternalConverter(arrayType.getElementType());
        return val -> {
            java.sql.Array pgArray = (java.sql.Array) val;
            Object[] in = (Object[]) pgArray.getArray();
            final Object[] array = (Object[]) Array.newInstance(elementClass, in.length);
            for (int i = 0; i < in.length; i++) {
                array[i] = elementConverter.deserialize(in[i]);
            }
            return new GenericArrayData(array);
        };
    }

    // Have its own method so that Postgres can support primitives that super class doesn't support
    // in the future
    private JdbcDeserializationConverter createPrimitiveConverter(LogicalType type) {
        return super.createInternalConverter(type);
    }

    @Override
    public String converterName() {
        return "PostgreSQL";
    }
}
