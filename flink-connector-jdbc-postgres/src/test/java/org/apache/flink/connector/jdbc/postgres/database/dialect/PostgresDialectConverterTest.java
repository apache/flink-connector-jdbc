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

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PostgresDialectConverter}. */
class PostgresDialectConverterTest {

    @Test
    void testUnsupportedArrayElementTypeFailsOnWriteOnly() {
        // Nested arrays can be read but not written, so creating the converter must not fail.
        PostgresDialectConverter converter =
                new PostgresDialectConverter(
                        RowType.of(new ArrayType(new ArrayType(new IntType()))));

        GenericRowData row =
                GenericRowData.of(
                        new GenericArrayData(
                                new Object[] {new GenericArrayData(new Integer[] {1})}));
        assertThatThrownBy(() -> converter.toExternal(row, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Writing ARRAY with element type ARRAY<INT> is not supported in JDBC:PostgreSQL.");
    }
}
