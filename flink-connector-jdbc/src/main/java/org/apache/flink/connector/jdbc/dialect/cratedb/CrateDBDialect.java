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

package org.apache.flink.connector.jdbc.dialect.cratedb;

import org.apache.flink.connector.jdbc.dialect.AbstractPostgresCompatibleDialect;
import org.apache.flink.connector.jdbc.internal.converter.PostgresRowConverter;
import org.apache.flink.table.types.logical.RowType;

import java.util.Optional;

/** JDBC dialect for CrateDB. */
public class CrateDBDialect extends AbstractPostgresCompatibleDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public String dialectName() {
        return "CrateDB";
    }

    @Override
    public PostgresRowConverter getRowConverter(RowType rowType) {
        return new PostgresRowConverter(rowType);
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.postgresql.Driver");
    }

    @Override
    public String appendDefaultUrlProperties(String url) {
        // Use CrateDB URL to choose this dialect, but replace it with PostgreSQL prefix
        // to use the vanilla PostgreSQL JDBC Driver, and not the custom CrateDB one.
        return url.replace("jdbc:crate:", "jdbc:postgresql:");
    }
}
