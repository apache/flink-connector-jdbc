/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.templates;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.stream.Collectors;

/** Table builder template. * */
public class TableBuilder implements TableManaged {

    private final String name;
    private final TableField[] fields;

    TableBuilder(String name, TableField... fields) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "Table name must be defined");
        Preconditions.checkArgument(
                fields != null && fields.length != 0, "Table fields must be defined");
        this.name = name;
        this.fields = fields;
    }

    public static TableBuilder of(String name, TableField... fields) {
        return new TableBuilder(name, fields);
    }

    public static TableField field(String name, DataType dataType) {
        return field(name, dataType, false);
    }

    public static TableField field(String name, DataType dataType, boolean isPk) {
        return new TableField(name, dataType, isPk);
    }

    public String getTableName() {
        return name;
    }

    public String[] getTableFields() {
        return Arrays.stream(this.fields).map(TableField::getName).toArray(String[]::new);
    }

    public DataType[] getTableDataTypes() {
        return Arrays.stream(this.fields).map(TableField::getDataType).toArray(DataType[]::new);
    }

    public RowTypeInfo getTableRowTypeInfo() {
        TypeInformation<?>[] typesArray =
                Arrays.stream(this.fields)
                        .map(TableField::getDataType)
                        .map(TypeConversions::fromDataTypeToLegacyInfo)
                        .toArray(TypeInformation[]::new);
        String[] fieldsArray = getTableFields();
        return new RowTypeInfo(typesArray, fieldsArray);
    }

    public RowType getTableRowType() {
        LogicalType[] typesArray =
                Arrays.stream(this.fields)
                        .map(TableField::getDataType)
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new);
        String[] fieldsArray = getTableFields();
        return RowType.of(typesArray, fieldsArray);
    }

    public int[] getTableTypes() {
        return Arrays.stream(this.fields)
                .map(TableField::getDataType)
                .map(DataType::getLogicalType)
                .map(LogicalType::getTypeRoot)
                .map(JdbcTypeUtil::logicalTypeToSqlType)
                .mapToInt(x -> x)
                .toArray();
    }

    public String getCreateQuery() {
        String pkFields =
                Arrays.stream(this.fields)
                        .filter(f -> f.isPk)
                        .map(f -> f.name)
                        .collect(Collectors.joining(", "));
        return String.format(
                "CREATE TABLE %s (%s%s)",
                name,
                Arrays.stream(this.fields)
                        .map(TableField::asSummaryString)
                        .collect(Collectors.joining(", ")),
                pkFields.isEmpty() ? "" : String.format(", PRIMARY KEY (%s)", pkFields));
    }

    public String getInsertIntoQuery() {
        return String.format(
                "INSERT INTO %s (%s) VALUES(%s)",
                name,
                Arrays.stream(this.fields)
                        .map(TableField::getName)
                        .collect(Collectors.joining(", ")),
                Arrays.stream(this.fields).map(x -> "?").collect(Collectors.joining(", ")));
    }

    public String getSelectAllQuery() {
        return String.format(
                "SELECT %s FROM %s",
                Arrays.stream(this.fields)
                        .map(TableField::getName)
                        .collect(Collectors.joining(", ")),
                name);
    }

    /** Table field. * */
    public static final class TableField {
        private final String name;
        private final DataType dataType;

        private final boolean isPk;

        private TableField(String name, DataType dataType, boolean isPk) {
            Preconditions.checkNotNull(name, "Column name can not be null.");
            Preconditions.checkNotNull(dataType, "Column data type can not be null.");
            this.name = name;
            this.dataType = dataType;
            this.isPk = isPk;
        }

        public DataType getDataType() {
            return this.dataType;
        }

        public String getName() {
            return this.name;
        }

        public String asSummaryString() {
            if (DataTypes.TIMESTAMP(0).equals(this.dataType)) {
                return String.format("%s TIMESTAMP", this.name);
            }
            if (DataTypes.DOUBLE().equals(this.dataType)) {
                return String.format("%s FLOAT", this.name);
            }
            return String.format("%s %s", this.name, this.dataType);
        }

        @Override
        public String toString() {
            return asSummaryString();
        }
    }
}
