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

package org.apache.flink.connector.jdbc.gaussdb.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.lang.reflect.Array;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * GaussDB.
 */
@Internal
public class GaussdbDialectConverter extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    protected GaussdbDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();

        if (root == LogicalTypeRoot.ARRAY) {
            ArrayType arrayType = (ArrayType) type;
            return createGaussdbArrayConverter(arrayType);
        } else {
            return createPrimitiveConverter(type);
        }
    }

    @Override
    protected JdbcSerializationConverter createNullableExternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        if (root == LogicalTypeRoot.ARRAY) {
            // note: Writing ARRAY type is not yet supported by GaussdbQL dialect now.
            return (val, index, statement) -> {
                throw new IllegalStateException(
                        String.format(
                                "Writing ARRAY type is not yet supported in JDBC:%s.",
                                converterName()));
            };
        } else {
            return super.createNullableExternalConverter(type);
        }
    }

    private JdbcDeserializationConverter createGaussdbArrayConverter(ArrayType arrayType) {

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

    // Have its own method so that Gaussdb can support primitives that super class doesn't support
    // in the future
    private JdbcDeserializationConverter createPrimitiveConverter(LogicalType type) {
        return super.createInternalConverter(type);
    }

    @Override
    public String converterName() {
        return "Gaussdb";
    }
}
