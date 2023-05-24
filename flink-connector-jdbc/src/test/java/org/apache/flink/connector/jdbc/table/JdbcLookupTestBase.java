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

import org.apache.flink.connector.jdbc.databases.derby.DerbyTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** Base class for JDBC lookup test. */
class JdbcLookupTestBase implements DerbyTestBase {
    public static final String LOOKUP_TABLE = "lookup_table";

    @BeforeEach
    void before() throws SQLException {
        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.executeUpdate(
                    "CREATE TABLE "
                            + LOOKUP_TABLE
                            + " ("
                            + "id1 INT NOT NULL DEFAULT 0,"
                            + "id2 VARCHAR(20) NOT NULL,"
                            + "comment1 VARCHAR(1000),"
                            + "comment2 VARCHAR(1000))");

            Object[][] data =
                    new Object[][] {
                        new Object[] {1, "1", "11-c1-v1", "11-c2-v1"},
                        new Object[] {1, "1", "11-c1-v2", "11-c2-v2"},
                        new Object[] {2, "3", null, "23-c2"},
                        new Object[] {2, "5", "25-c1", "25-c2"},
                        new Object[] {3, "8", "38-c1", "38-c2"}
                    };
            boolean[] surroundedByQuotes = new boolean[] {false, true, true, true};

            StringBuilder sqlQueryBuilder =
                    new StringBuilder(
                            "INSERT INTO "
                                    + LOOKUP_TABLE
                                    + " (id1, id2, comment1, comment2) VALUES ");
            for (int i = 0; i < data.length; i++) {
                sqlQueryBuilder.append("(");
                for (int j = 0; j < data[i].length; j++) {
                    if (data[i][j] == null) {
                        sqlQueryBuilder.append("null");
                    } else {
                        if (surroundedByQuotes[j]) {
                            sqlQueryBuilder.append("'");
                        }
                        sqlQueryBuilder.append(data[i][j]);
                        if (surroundedByQuotes[j]) {
                            sqlQueryBuilder.append("'");
                        }
                    }
                    if (j < data[i].length - 1) {
                        sqlQueryBuilder.append(", ");
                    }
                }
                sqlQueryBuilder.append(")");
                if (i < data.length - 1) {
                    sqlQueryBuilder.append(", ");
                }
            }
            stat.execute(sqlQueryBuilder.toString());
        }
    }

    public void insert(String insertQuery) throws SQLException {
        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute(insertQuery);
        }
    }

    @AfterEach
    void clearOutputTable() throws Exception {
        try (Connection conn = getMetadata().getConnection();
                Statement stat = conn.createStatement()) {
            stat.execute("DROP TABLE " + LOOKUP_TABLE);
        }
    }
}
