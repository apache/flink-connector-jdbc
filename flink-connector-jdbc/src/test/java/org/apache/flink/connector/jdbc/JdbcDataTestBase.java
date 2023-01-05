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

package org.apache.flink.connector.jdbc;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;

import java.sql.SQLException;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;

/**
 * Base class for JDBC test using data from {@link JdbcTestFixture}. It uses {@link DerbyDbMetadata}
 * and inserts data before each test.
 */
public abstract class JdbcDataTestBase extends JdbcTestBase {
    @BeforeEach
    void initData() throws SQLException {
        JdbcTestFixture.initData(getDbMetadata());
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return DERBY_EBOOKSHOP_DB;
    }

    public static Row toRow(JdbcTestFixture.TestEntry entry) {
        Row row = new Row(5);
        row.setField(0, entry.id);
        row.setField(1, entry.title);
        row.setField(2, entry.author);
        row.setField(3, entry.price);
        row.setField(4, entry.qty);
        return row;
    }

    // utils function to build a RowData, note: only support primitive type and String from now
    public static RowData buildGenericData(Object... args) {
        GenericRowData row = new GenericRowData(args.length);
        for (int i = 0; i < args.length; i++) {
            if (args[i] instanceof String) {
                row.setField(i, StringData.fromString((String) args[i]));
            } else {
                row.setField(i, args[i]);
            }
        }
        return row;
    }
}
