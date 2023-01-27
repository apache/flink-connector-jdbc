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

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.util.function.FunctionWithException;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** Table template. * */
public interface TableManaged
        extends org.apache.flink.connector.jdbc.templates.round2.TableManaged {

    String getTableName();

    String getCreateQuery();

    default String getDeleteFromQuery() {
        return String.format("DELETE FROM %s", getTableName());
    }

    default String getDropTableQuery() {
        return String.format("DROP TABLE %s", getTableName());
    }

    default void createTable(Connection conn) throws SQLException {
        executeUpdate(conn, getCreateQuery());
    }

    default void deleteTable(Connection conn) throws SQLException {
        executeUpdate(conn, getDeleteFromQuery());
    }

    default void dropTable(Connection conn) throws SQLException {
        executeUpdate(conn, getDropTableQuery());
    }

    default void executeUpdate(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(sql);
        }
    }

    default <T> List<T> executeStatement(
            Connection conn, String sql, JdbcResultSetBuilder<T> rsGetter) throws SQLException {
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            return rsGetter.accept(rs);
        }
    }

    default <T> int[] executeStatement(
            Connection conn, String sql, JdbcStatementBuilder<T> psSetter, List<T> values)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (T value : values) {
                psSetter.accept(ps, value);
                ps.addBatch();
            }
            return ps.executeBatch();
        }
    }

    default <T> T getNullable(ResultSet rs, FunctionWithException<ResultSet, T, SQLException> get)
            throws SQLException {
        T value = get.apply(rs);
        return rs.wasNull() ? null : value;
    }

    /** ResultSet builder. * */
    @FunctionalInterface
    interface JdbcResultSetBuilder<T> extends Serializable {
        List<T> accept(ResultSet rs) throws SQLException;
    }
}
