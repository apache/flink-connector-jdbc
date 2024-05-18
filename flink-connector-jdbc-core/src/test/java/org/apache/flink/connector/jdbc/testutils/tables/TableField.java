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

package org.apache.flink.connector.jdbc.testutils.tables;

import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

/** Table field. * */
public class TableField {
    private final String name;
    private final DbType dbType;
    private final DataType dataType;
    private final boolean pkField;

    protected TableField(String name, DataType dataType, DbType dbType, boolean pkField) {
        Preconditions.checkNotNull(name, "Column name can not be null.");
        Preconditions.checkNotNull(dataType, "Column data type can not be null.");
        this.name = name;
        this.dataType = dataType;
        this.dbType = dbType;
        this.pkField = pkField;
    }

    public String getName() {
        return this.name;
    }

    public DataType getDataType() {
        return this.dataType;
    }

    public boolean isPkField() {
        return pkField;
    }

    public String asString() {
        String fieldType =
                (this.dbType != null) ? this.dbType.toString() : this.dataType.toString();
        return String.format("%s %s", this.name, fieldType);
    }

    @Override
    public String toString() {
        return asString();
    }

    /** Field definition for database. */
    public static class DbType {
        private final String type;
        private Boolean nullable = true;

        public DbType(String type) {
            this.type = type;
        }

        public DbType notNull() {
            this.nullable = false;
            return this;
        }

        @Override
        public String toString() {
            return String.format("%s%s", this.type, this.nullable ? "" : " NOT NULL");
        }
    }
}
