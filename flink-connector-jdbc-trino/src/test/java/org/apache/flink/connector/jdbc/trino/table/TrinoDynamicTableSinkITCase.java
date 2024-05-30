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

package org.apache.flink.connector.jdbc.trino.table;

import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSinkITCase;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.trino.TrinoTestBase;
import org.apache.flink.connector.jdbc.trino.database.dialect.TrinoDialect;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

/** The Table Sink ITCase for {@link TrinoDialect}. */
@Disabled("Not working on jenkins as container not start.")
class TrinoDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase implements TrinoTestBase {

    @Override
    public List<TableManaged> getManagedTables() {
        return Arrays.asList(
                // upsertOutputTable,
                appendOutputTable, batchOutputTable, realOutputTable, checkpointOutputTable
                // userOutputTable
                );
    }

    @Disabled("Trino dont allow create tables with PK")
    @Test
    @Override
    protected void testUpsert() throws Exception {
        DatabaseMetadata dbMeta = getMetadataDatabase();
        try (Connection conn = dbMeta.getConnection()) {
            try {
                upsertOutputTable.createTable(conn);
                super.testUpsert();
            } finally {
                upsertOutputTable.deleteTable(conn);
            }
        }
    }

    @Disabled("Trino dont allow create tables with PK")
    @Test
    @Override
    protected void testReadingFromChangelogSource() throws Exception {
        try (Connection conn = getMetadataDatabase().getConnection()) {
            try {
                userOutputTable.createTable(conn);
                super.testReadingFromChangelogSource();
            } finally {
                userOutputTable.deleteTable(conn);
            }
        }
    }
}
