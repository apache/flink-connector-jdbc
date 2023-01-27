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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.templates.TableBuilder;
import org.apache.flink.connector.jdbc.templates.round2.TableManaged;
import org.apache.flink.table.api.DataTypes;

import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Base class for JDBC lookup test. */
class JdbcLookupTestBase implements JdbcTestBase {

    public static final String LOOKUP_TABLE_NAME = "lookup_table";

    protected static final TableBuilder LOOKUP_TABLE =
            TableBuilder.of(
                    "lookup_table",
                    TableBuilder.field("id1", DataTypes.INT().notNull()),
                    TableBuilder.field("id2", DataTypes.VARCHAR(20).notNull()),
                    TableBuilder.field("comment1", DataTypes.VARCHAR(1000)),
                    TableBuilder.field("comment2", DataTypes.VARCHAR(1000)));

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(LOOKUP_TABLE);
    }

    @BeforeEach
    void before() throws SQLException {
        try (Connection conn = getDbMetadata().getConnection();
                Statement stat = conn.createStatement()) {

            Object[][] data =
                    new Object[][] {
                        new Object[] {1, "1", "11-c1-v1", "11-c2-v1"},
                        new Object[] {1, "1", "11-c1-v2", "11-c2-v2"},
                        new Object[] {2, "3", null, "23-c2"},
                        new Object[] {2, "5", "25-c1", "25-c2"},
                        new Object[] {3, "8", "38-c1", "38-c2"}
                    };

            Function<Object, Object> valueChecker =
                    value -> {
                        if (value == null || value instanceof Number) {
                            return value;
                        }
                        return String.format("'%s'", value);
                    };

            String query =
                    String.format(
                            "INSERT INTO %s (%s) VALUES %s",
                            LOOKUP_TABLE.getTableName(),
                            String.join(", ", LOOKUP_TABLE.getTableFields()),
                            Arrays.stream(data)
                                    .map(
                                            row ->
                                                    String.format(
                                                            "(%s, %s, %s, %s)",
                                                            valueChecker.apply(row[0]),
                                                            valueChecker.apply(row[1]),
                                                            valueChecker.apply(row[2]),
                                                            valueChecker.apply(row[3])))
                                    .collect(Collectors.joining(", ")));

            stat.execute(query);
        }
    }

    public void insert(String insertQuery) throws SQLException {
        try (Connection conn = getDbMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute(insertQuery);
        }
    }
}
