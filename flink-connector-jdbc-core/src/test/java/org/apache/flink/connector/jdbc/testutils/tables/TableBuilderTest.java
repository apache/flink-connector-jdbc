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

package org.apache.flink.connector.jdbc.testutils.tables;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.tableRow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TableBuilderTest {

    TableBase<?> table =
            tableRow(
                    "test",
                    pkField("id", DataTypes.INT().notNull()),
                    field("name", DataTypes.VARCHAR(10)));

    @Test
    void testTableCreationFails() {
        assertThrows(
                IllegalArgumentException.class, () -> tableRow(""), "Table name must be defined");
        assertThrows(
                IllegalArgumentException.class,
                () -> tableRow("test"),
                "Table fields must be defined");
    }

    @Test
    void testTableCreationOk() {
        assertEquals("test", table.getTableName());
        assertEquals(2, table.getTableFields().length);
        assertEquals("id", table.getTableFields()[0]);
        assertEquals("name", table.getTableFields()[1]);
    }

    @Test
    void testQueryCreation() {
        String expected = "CREATE TABLE test (id INT NOT NULL, name VARCHAR(10), PRIMARY KEY (id))";
        assertEquals(expected, table.getCreateQuery());
    }

    @Test
    void testQueryCreationWithDbType() {
        TableRow table =
                tableRow(
                        "test",
                        pkField("id", dbType("DOUBLE").notNull(), DataTypes.FLOAT().notNull()),
                        field("type", dbType("REAL"), DataTypes.FLOAT()));
        String expected = "CREATE TABLE test (id DOUBLE NOT NULL, type REAL, PRIMARY KEY (id))";
        assertEquals(expected, table.getCreateQuery());
    }

    @Test
    void testQueryInsertInto() {
        String expected = "INSERT INTO test (id, name) VALUES (?, ?)";
        assertEquals(expected, table.getInsertIntoQuery());
    }

    @Test
    void testQuerySelectAll() {
        String expected = "SELECT id, name FROM test";
        assertEquals(expected, table.getSelectAllQuery());
    }

    @Test
    void testQueryDeleteFrom() {
        String expected = "DELETE FROM test";
        assertEquals(expected, table.getDeleteFromQuery());
    }

    @Test
    void testQueryDropTable() {
        String expected = "DROP TABLE test";
        assertEquals(expected, table.getDropTableQuery());
    }

    @Test
    void testRowTypeInfo() {
        RowTypeInfo expected =
                new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        assertEquals(expected, table.getTableRowTypeInfo());
    }

    @Test
    void testRowType() {
        RowType expected =
                RowType.of(
                        new LogicalType[] {new IntType(false), new VarCharType(10)},
                        new String[] {"id", "name"});

        assertEquals(expected, table.getTableRowType());
    }
}
