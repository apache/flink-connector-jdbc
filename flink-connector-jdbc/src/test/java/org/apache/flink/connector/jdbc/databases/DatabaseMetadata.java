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

package org.apache.flink.connector.jdbc.databases;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.util.ExceptionUtils;

import javax.sql.XADataSource;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;

/** Describes a database: driver, schema and urls. */
public interface DatabaseMetadata extends Serializable {

    String getUrl();

    default String getSchema() {
        return "";
    }

    default String getUser() {
        return "";
    }

    default String getPassword() {
        return "";
    }

    default String getVersion() {
        return "";
    }

    XADataSource buildXaDataSource();

    String getDriverClass();

    default JdbcConnectionOptions toConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(getDriverClass())
                .withUrl(getUrl())
                .build();
    }

    default Connection getConnection() {
        try {
            Class.forName(getDriverClass());
            return DriverManager.getConnection(getUrl(), getUser(), getPassword());
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
            return null;
        }
    }
}
