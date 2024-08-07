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

package org.apache.flink.connector.jdbc.core.database.dialect;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AbstractDialectConverter}. */
class JdbcDialectConverterTest {

    @Test
    void testExternalLocalDateTimeToTimestamp() throws Exception {
        RowType rowType = RowType.of(new IntType(), new TimestampType(3));
        JdbcDialectConverter rowConverter =
                new AbstractDialectConverter(rowType) {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public String converterName() {
                        return "test";
                    }
                };

        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getObject(1)).thenReturn(123);
        Mockito.when(resultSet.getObject(2))
                .thenReturn(LocalDateTime.parse("2021-04-07T00:00:05.999"));
        RowData res = rowConverter.toInternal(resultSet);

        assertThat(res.getInt(0)).isEqualTo(123);
        assertThat(res.getTimestamp(1, 3).toLocalDateTime())
                .isEqualTo(LocalDateTime.parse("2021-04-07T00:00:05.999"));
    }
}
