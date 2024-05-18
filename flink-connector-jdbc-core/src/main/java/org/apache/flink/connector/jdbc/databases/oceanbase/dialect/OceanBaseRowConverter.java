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

package org.apache.flink.connector.jdbc.databases.oceanbase.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * OceanBase.
 */
@Internal
public class OceanBaseRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "OceanBase";
    }

    public OceanBaseRowConverter(RowType rowType) {
        super(rowType);
    }

    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val ->
                        val instanceof Number
                                ? ((Number) val).intValue() == 1
                                : Boolean.parseBoolean(val.toString());
            case FLOAT:
                return val -> val instanceof Number ? ((Number) val).floatValue() : val;
            case DOUBLE:
                return val -> val instanceof Number ? ((Number) val).doubleValue() : val;
            case TINYINT:
                return val -> val instanceof Number ? ((Number) val).byteValue() : val;
            case SMALLINT:
                return val -> val instanceof Number ? ((Number) val).shortValue() : val;
            case INTEGER:
                return val -> val instanceof Number ? ((Number) val).intValue() : val;
            case BIGINT:
                return val -> val instanceof Number ? ((Number) val).longValue() : val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : val instanceof BigDecimal
                                        ? DecimalData.fromBigDecimal(
                                                (BigDecimal) val, precision, scale)
                                        : DecimalData.fromBigDecimal(
                                                new BigDecimal(val.toString()), precision, scale);
            case DATE:
                return val ->
                        val instanceof Date
                                ? (int) (((Date) val).toLocalDate().toEpochDay())
                                : val instanceof Timestamp
                                        ? (int)
                                                (((Timestamp) val)
                                                        .toLocalDateTime()
                                                        .toLocalDate()
                                                        .toEpochDay())
                                        : val;
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        val instanceof Time
                                ? (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L)
                                : val instanceof Timestamp
                                        ? (int)
                                                (((Timestamp) val)
                                                                .toLocalDateTime()
                                                                .toLocalTime()
                                                                .toNanoOfDay()
                                                        / 1_000_000L)
                                        : val;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val ->
                        val instanceof LocalDateTime
                                ? TimestampData.fromLocalDateTime((LocalDateTime) val)
                                : val instanceof Timestamp
                                        ? TimestampData.fromTimestamp((Timestamp) val)
                                        : val;
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
            case RAW:
                return val ->
                        val instanceof Blob
                                ? ((Blob) val).getBytes(1, (int) ((Blob) val).length())
                                : val instanceof byte[] ? val : val.toString().getBytes();
            default:
                return super.createInternalConverter(type);
        }
    }
}
