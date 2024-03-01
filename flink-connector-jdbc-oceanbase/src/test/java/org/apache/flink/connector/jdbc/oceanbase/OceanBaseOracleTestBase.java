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

package org.apache.flink.connector.jdbc.oceanbase;

import org.apache.flink.connector.jdbc.oceanbase.table.OceanBaseTableRow;
import org.apache.flink.connector.jdbc.oceanbase.testutils.OceanBaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;

/** Base class for OceanBase Oracle mode testing. */
public interface OceanBaseOracleTestBase extends DatabaseTest {

    static TableRow tableRow(String name, TableField... fields) {
        return new OceanBaseTableRow("oracle", name, fields);
    }

    @Override
    default DatabaseMetadata getMetadata() {
        return new OceanBaseMetadata(
                System.getenv("test.oceanbase.username"),
                System.getenv("test.oceanbase.password"),
                System.getenv("test.oceanbase.url"),
                "com.oceanbase.jdbc.Driver",
                "test");
    }
}
