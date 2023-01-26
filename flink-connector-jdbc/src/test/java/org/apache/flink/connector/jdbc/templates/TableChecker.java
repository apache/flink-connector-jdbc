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

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Table checker. * */
public interface TableChecker {
    static void check(DatabaseMetadata metadata, TableManual table, Row[] rows)
            throws SQLException {
        try (Connection dbConn = metadata.getConnection()) {
            String[] results =
                    table.selectAllTable(dbConn).stream()
                            .map(Row::toString)
                            .sorted()
                            .toArray(String[]::new);

            assertThat(results)
                    .isEqualTo(
                            Arrays.stream(rows).map(Row::toString).sorted().toArray(String[]::new));
        }
    }
}
