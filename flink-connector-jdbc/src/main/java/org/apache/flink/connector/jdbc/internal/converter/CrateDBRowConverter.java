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

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import io.crate.shade.org.postgresql.jdbc.PgArray;

import java.lang.reflect.Array;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * CrateDB.
 */
public class CrateDBRowConverter extends PostgresRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "CrateDB";
    }

    public CrateDBRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    protected JdbcDeserializationConverter createPostgresArrayConverter(ArrayType arrayType) {
        // Since PGJDBC 42.2.15 (https://github.com/pgjdbc/pgjdbc/pull/1194) bytea[] is wrapped in
        // primitive byte arrays
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        final JdbcDeserializationConverter elementConverter =
                createNullableInternalConverter(arrayType.getElementType());
        return val -> {
            PgArray pgArray = (PgArray) val;
            Object[] in = (Object[]) pgArray.getArray();
            final Object[] array = (Object[]) Array.newInstance(elementClass, in.length);
            for (int i = 0; i < in.length; i++) {
                array[i] = elementConverter.deserialize(in[i]);
            }
            return new GenericArrayData(array);
        };
    }
}
