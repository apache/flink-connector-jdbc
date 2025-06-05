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

package org.apache.flink.connector.jdbc.clickhouse.testutils;

import org.apache.flink.table.types.DataType;

/** ClickHouseTableBuilder. */
public class ClickHouseTableBuilder {

    public static ClickHouseTableRow clickHouseTableRow(
            String name, ClickHouseTableField... fields) {
        return new ClickHouseTableRow(name, fields);
    }

    public static ClickHouseTableField field(String name, DataType dataType) {
        return field(name, null, dataType);
    }

    public static ClickHouseTableField field(
            String name, ClickHouseTableField.DbType dbType, DataType dataType) {
        return createField(name, dbType, dataType, false);
    }

    public static ClickHouseTableField pkField(String name, DataType dataType) {
        return pkField(name, null, dataType);
    }

    public static ClickHouseTableField pkField(
            String name, ClickHouseTableField.DbType dbType, DataType dataType) {
        return createField(name, dbType, dataType, true);
    }

    public static ClickHouseTableField.DbType dbType(String type) {
        return new ClickHouseTableField.DbType(type);
    }

    private static ClickHouseTableField createField(
            String name, ClickHouseTableField.DbType dbType, DataType dataType, boolean pkField) {
        return new ClickHouseTableField(name, dataType, dbType, pkField);
    }
}
