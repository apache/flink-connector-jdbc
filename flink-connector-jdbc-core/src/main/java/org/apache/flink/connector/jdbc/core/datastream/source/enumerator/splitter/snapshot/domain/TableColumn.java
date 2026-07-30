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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Objects;

/** Represents a column in a database table with its metadata. */
@PublicEvolving
public class TableColumn implements Serializable {

    private final Integer columnPosition;
    private final String columnName;
    private final String columnType;
    private final Boolean columnNullable;
    private final Boolean columnPrimaryKey;

    public TableColumn(
            Integer columnPosition,
            String columnName,
            String columnType,
            Boolean columnNullable,
            Boolean columnPrimaryKey) {
        this.columnPosition = columnPosition;
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnNullable = columnNullable;
        this.columnPrimaryKey = columnPrimaryKey;
    }

    public Integer columnPosition() {
        return columnPosition;
    }

    public String columnName() {
        return columnName;
    }

    public String columnType() {
        return columnType;
    }

    public Boolean columnNullable() {
        return columnNullable;
    }

    public Boolean columnPrimaryKey() {
        return columnPrimaryKey;
    }

    public boolean isUuidColumnType() {
        return "UUID".equalsIgnoreCase(columnType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableColumn)) {
            return false;
        }
        TableColumn that = (TableColumn) o;
        return Objects.equals(columnPosition, that.columnPosition)
                && Objects.equals(columnName, that.columnName)
                && Objects.equals(columnType, that.columnType)
                && Objects.equals(columnNullable, that.columnNullable)
                && Objects.equals(columnPrimaryKey, that.columnPrimaryKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                columnPosition, columnName, columnType, columnNullable, columnPrimaryKey);
    }

    @Override
    public String toString() {
        return "TableColumn{"
                + "columnPosition="
                + columnPosition
                + ", columnName='"
                + columnName
                + '\''
                + ", columnType='"
                + columnType
                + '\''
                + ", columnNullable="
                + columnNullable
                + ", columnPrimaryKey="
                + columnPrimaryKey
                + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder class for constructing TableColumn instances. */
    public static class Builder {
        private String columnName;
        private String columnType;
        private Integer columnPosition;
        private Boolean columnNullable;
        private Boolean columnPrimaryKey;

        Builder() {}

        public Builder withColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder withColumnType(String columnType) {
            this.columnType = columnType;
            return this;
        }

        public Builder withColumnPosition(Integer columnPosition) {
            this.columnPosition = columnPosition;
            return this;
        }

        public Builder withColumnNullable(Boolean isNullable) {
            this.columnNullable = isNullable;
            return this;
        }

        public Builder withColumnPk(Boolean isPk) {
            this.columnPrimaryKey = isPk;
            return this;
        }

        public TableColumn build() {
            return new TableColumn(
                    this.columnPosition,
                    this.columnName,
                    this.columnType,
                    this.columnNullable,
                    this.columnPrimaryKey);
        }
    }
}
