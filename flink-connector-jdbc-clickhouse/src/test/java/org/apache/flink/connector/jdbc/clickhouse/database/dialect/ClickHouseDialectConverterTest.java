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

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ClickHouseDialectConverterTest {
    @Test
    protected void testUInt8AboveTinyIntMaxFails() {
        ClickHouseDialectConverter converter =
                new ClickHouseDialectConverter(
                        RowType.of(new LogicalType[] {new TinyIntType()}, new String[] {"id"}));

        assertThatThrownBy(
                        () -> converter.createInternalConverter(new TinyIntType()).deserialize(200))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range")
                .hasMessageContaining("TINYINT")
                .hasMessageContaining("200");
    }

    @Test
    protected void testTinyIntAtMaxReads() throws SQLException {
        ClickHouseDialectConverter converter =
                new ClickHouseDialectConverter(
                        RowType.of(new LogicalType[] {new TinyIntType()}, new String[] {"id"}));

        Object result =
                converter.createInternalConverter(new TinyIntType()).deserialize((byte) 127);

        assertThat(result).isEqualTo((byte) 127);
    }

    @Test
    protected void testUInt16AboveSmallIntMaxFails() {
        ClickHouseDialectConverter converter =
                new ClickHouseDialectConverter(
                        RowType.of(new LogicalType[] {new SmallIntType()}, new String[] {"id"}));

        assertThatThrownBy(
                        () ->
                                converter
                                        .createInternalConverter(new SmallIntType())
                                        .deserialize(40000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range")
                .hasMessageContaining("SMALLINT")
                .hasMessageContaining("40000");
    }

    @Test
    protected void testSmallIntAtMaxReads() throws SQLException {
        ClickHouseDialectConverter converter =
                new ClickHouseDialectConverter(
                        RowType.of(new LogicalType[] {new SmallIntType()}, new String[] {"id"}));

        Object result =
                converter.createInternalConverter(new SmallIntType()).deserialize((short) 32767);

        assertThat(result).isEqualTo((short) 32767);
    }

    @Test
    protected void testUInt32AboveIntMaxFails() {
        ClickHouseDialectConverter converter =
                new ClickHouseDialectConverter(
                        RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"}));

        assertThatThrownBy(
                        () ->
                                converter
                                        .createInternalConverter(new IntType())
                                        .deserialize(4294967295L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range")
                .hasMessageContaining("INTEGER")
                .hasMessageContaining("4294967295");
    }

    @Test
    protected void testIntAtMaxReads() throws SQLException {
        ClickHouseDialectConverter converter =
                new ClickHouseDialectConverter(
                        RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"}));

        Object result =
                converter.createInternalConverter(new IntType()).deserialize(Integer.MAX_VALUE);

        assertThat(result).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    protected void testUInt64AboveLongMaxFailsInsteadOfWrapping() {
        ClickHouseDialectConverter converter =
                new ClickHouseDialectConverter(
                        RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"}));

        assertThatThrownBy(
                        () ->
                                converter
                                        .createInternalConverter(new BigIntType())
                                        .deserialize(new BigInteger("18446744073709551615")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range")
                .hasMessageContaining("18446744073709551615");
    }

    @Test
    protected void testUInt64AtLongMaxReads() throws SQLException {
        ClickHouseDialectConverter converter =
                new ClickHouseDialectConverter(
                        RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"}));

        Object result =
                converter
                        .createInternalConverter(new BigIntType())
                        .deserialize(new BigInteger("9223372036854775807"));

        assertThat(result).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    protected void testTimestampReadsWithoutZoneShift() throws SQLException {
        ClickHouseDialectConverter converter =
                new ClickHouseDialectConverter(
                        RowType.of(new LogicalType[] {new TimestampType(6)}, new String[] {"ts"}));

        Timestamp driverValue =
                Timestamp.from(
                        LocalDateTime.parse("2020-01-01T15:36:01.123456")
                                .toInstant(ZoneOffset.UTC));

        Object result =
                converter.createInternalConverter(new TimestampType(6)).deserialize(driverValue);

        assertThat(result)
                .isInstanceOf(TimestampData.class)
                .isEqualTo(TimestampData.fromTimestamp(driverValue));
    }

    @Test
    protected void testLocalDateTimeReadsAsIs() throws SQLException {
        ClickHouseDialectConverter converter =
                new ClickHouseDialectConverter(
                        RowType.of(new LogicalType[] {new TimestampType(6)}, new String[] {"ts"}));

        LocalDateTime local = LocalDateTime.parse("2020-01-01T15:36:01.123456");

        Object result = converter.createInternalConverter(new TimestampType(6)).deserialize(local);

        assertThat(result).isEqualTo(TimestampData.fromLocalDateTime(local));
    }
}
