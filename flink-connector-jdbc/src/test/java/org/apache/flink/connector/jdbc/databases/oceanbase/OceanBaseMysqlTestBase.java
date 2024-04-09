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

package org.apache.flink.connector.jdbc.databases.oceanbase;

import org.apache.flink.connector.jdbc.databases.oceanbase.table.OceanBaseTableRow;
import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.databases.oceanbase.OceanBaseDatabase;
import org.apache.flink.connector.jdbc.testutils.tables.TableField;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;

import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for OceanBase Mysql mode testing. */
@ExtendWith(OceanBaseDatabase.class)
public interface OceanBaseMysqlTestBase extends DatabaseTest {

    default TableRow tableRow(String name, TableField... fields) {
        return new OceanBaseTableRow("mysql", name, fields);
    }

    @Override
    default DatabaseMetadata getMetadata() {
        return OceanBaseDatabase.getMetadata();
    }
}
