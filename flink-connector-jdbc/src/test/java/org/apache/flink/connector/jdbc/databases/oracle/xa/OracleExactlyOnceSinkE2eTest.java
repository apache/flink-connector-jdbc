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

package org.apache.flink.connector.jdbc.databases.oracle.xa;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.oracle.OracleXaDatabase;
import org.apache.flink.connector.jdbc.xa.JdbcExactlyOnceSinkE2eTest;
import org.apache.flink.util.function.SerializableSupplier;

import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.XADataSource;

/** A simple end-to-end test for {@link JdbcExactlyOnceSinkE2eTest}. */
@DisabledOnOs(OS.MAC)
@ExtendWith(OracleXaDatabase.class)
public class OracleExactlyOnceSinkE2eTest extends JdbcExactlyOnceSinkE2eTest {

    @Override
    public DatabaseMetadata getDbMetadata() {
        return OracleXaDatabase.getMetadata();
    }

    @Override
    protected SerializableSupplier<XADataSource> getDataSourceSupplier() {
        return () -> OracleXaDatabase.getMetadata().buildXaDataSource();
    }
}