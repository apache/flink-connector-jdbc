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

package org.apache.flink.connector.jdbc.testutils.databases.elasticsearch;

import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

/** Creates content for Elastic Index API call. */
public class ElasticsearchIndexSchemaBuilder {

    private static final ElasticsearchDataTypeMapper MAPPER = new ElasticsearchDataTypeMapper();

    public static String buildIndexSchema(TableRow tableRow) {
        String fields =
                stream(tableRow.getTableDataFields())
                        .map(
                                field ->
                                        format(
                                                "\"%s\": %s",
                                                field.getName(),
                                                field.getDataType().accept(MAPPER)))
                        .collect(joining(", "));
        return "{\"settings\": {\"number_of_shards\": 1}, \"mappings\": {\"properties\": {"
                + fields
                + "}}}";
    }

    /** Maps Flink types to Elasticsearch types. */
    private static class ElasticsearchDataTypeMapper
            implements DataTypeVisitor<String>, LogicalTypeVisitor<String> {

        @Override
        public String visit(AtomicDataType atomicDataType) {
            return atomicDataType.getLogicalType().accept(this);
        }

        @Override
        public String visit(CollectionDataType collectionDataType) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public String visit(FieldsDataType fieldsDataType) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public String visit(KeyValueDataType keyValueDataType) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public String visit(CharType charType) {
            throw new IllegalArgumentException("CharType is not supported.");
        }

        @Override
        public String visit(VarCharType varCharType) {
            return "{\"type\": \"text\"}";
        }

        @Override
        public String visit(BooleanType booleanType) {
            return "{\"type\": \"boolean\"}";
        }

        @Override
        public String visit(BinaryType binaryType) {
            return "{\"type\": \"binary\"}";
        }

        @Override
        public String visit(VarBinaryType varBinaryType) {
            return "{\"type\": \"binary\"}";
        }

        @Override
        public String visit(DecimalType decimalType) {
            throw new IllegalArgumentException("DecimalType is not supported.");
        }

        @Override
        public String visit(TinyIntType tinyIntType) {
            return "{\"type\": \"byte\"}";
        }

        @Override
        public String visit(SmallIntType smallIntType) {
            return "{\"type\": \"short\"}";
        }

        @Override
        public String visit(IntType intType) {
            return "{\"type\": \"integer\"}";
        }

        @Override
        public String visit(BigIntType bigIntType) {
            return "{\"type\": \"long\"}";
        }

        @Override
        public String visit(FloatType floatType) {
            return "{\"type\": \"float\"}";
        }

        @Override
        public String visit(DoubleType doubleType) {
            return "{\"type\": \"double\"}";
        }

        @Override
        public String visit(DateType dateType) {
            return "{\"type\": \"date\", \"format\": \"strict_date||basic_date\"}";
        }

        @Override
        public String visit(TimeType timeType) {
            throw new IllegalArgumentException("TimeType is not supported.");
        }

        @Override
        public String visit(TimestampType timestampType) {
            return timestampType.getPrecision() <= 3
                    ? "{\"type\": \"date\"}"
                    : "{\"type\": \"date_nanos\"}";
        }

        @Override
        public String visit(ZonedTimestampType zonedTimestampType) {
            throw new IllegalArgumentException("ZonedTimestampType is not supported.");
        }

        @Override
        public String visit(LocalZonedTimestampType localZonedTimestampType) {
            throw new IllegalArgumentException("LocalZonedTimestampType is not supported.");
        }

        @Override
        public String visit(YearMonthIntervalType yearMonthIntervalType) {
            throw new IllegalArgumentException("YearMonthIntervalType is not supported.");
        }

        @Override
        public String visit(DayTimeIntervalType dayTimeIntervalType) {
            throw new IllegalArgumentException("DayTimeIntervalType is not supported.");
        }

        @Override
        public String visit(ArrayType arrayType) {
            throw new IllegalArgumentException("ArrayType is not supported.");
        }

        @Override
        public String visit(MultisetType multisetType) {
            throw new IllegalArgumentException("MultisetType is not supported.");
        }

        @Override
        public String visit(MapType mapType) {
            throw new IllegalArgumentException("MapType is not supported.");
        }

        @Override
        public String visit(RowType rowType) {
            throw new IllegalArgumentException("RowType is not supported.");
        }

        @Override
        public String visit(DistinctType distinctType) {
            throw new IllegalArgumentException("DistinctType is not supported.");
        }

        @Override
        public String visit(StructuredType structuredType) {
            throw new IllegalArgumentException("StructuredType is not supported.");
        }

        @Override
        public String visit(NullType nullType) {
            throw new IllegalArgumentException("NullType is not supported.");
        }

        @Override
        public String visit(RawType<?> rawType) {
            throw new IllegalArgumentException("RawType is not supported.");
        }

        @Override
        public String visit(SymbolType<?> symbolType) {
            throw new IllegalArgumentException("SymbolType is not supported.");
        }

        @Override
        public String visit(LogicalType other) {
            return other.accept(this);
        }
    }
}
