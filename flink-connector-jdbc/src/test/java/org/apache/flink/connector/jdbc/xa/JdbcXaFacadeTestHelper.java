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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.connector.jdbc.JdbcTestCheckpoint;
import org.apache.flink.connector.jdbc.templates.BooksTable;

import javax.sql.XADataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcXaFacadeTestHelper implements AutoCloseable {
    private final String table;
    private final String dbUrl;
    private final String user;
    private final String pass;
    private final XaFacade xaFacade;

    private final BooksTable.BookEntry[] testData;

    JdbcXaFacadeTestHelper(
            XADataSource xaDataSource,
            String dbUrl,
            String table,
            String user,
            String pass,
            BooksTable.BookEntry[] testData)
            throws Exception {
        this.dbUrl = dbUrl;
        this.table = table;
        this.xaFacade = XaFacadeImpl.fromXaDataSource(xaDataSource);
        this.xaFacade.open();
        this.user = user;
        this.pass = pass;
        this.testData = testData;
    }

    void assertPreparedTxCountEquals(int expected) {
        assertThat(xaFacade.recover()).hasSize(expected);
    }

    void assertDbContentsEquals(JdbcTestCheckpoint... checkpoints) throws SQLException {
        assertDbContentsEquals(
                Arrays.stream(checkpoints).flatMapToInt(x -> Arrays.stream(x.dataItemsIdx)));
    }

    void assertDbContentsEquals(IntStream dataIdxStream) throws SQLException {
        assertDbContentsEquals(
                dataIdxStream.map(idx -> testData[idx].id).boxed().collect(Collectors.toList()));
    }

    void assertDbContentsEquals(List<Integer> expected) throws SQLException {
        assertThat(getInsertedIds()).isEqualTo(expected);
    }

    private List<Integer> getInsertedIds() throws SQLException {
        return getInsertedIds(dbUrl, user, pass, table);
    }

    static List<Integer> getInsertedIds(String dbUrl, String user, String pass, String table)
            throws SQLException {
        List<Integer> dbContents = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(dbUrl, user, pass)) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setReadOnly(true);
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery("select id from " + table)) {
                    while (rs.next()) {
                        dbContents.add(rs.getInt(1));
                    }
                }
            }
        }
        return dbContents;
    }

    int countInDb() throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbUrl)) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setReadOnly(true);
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery("select count(1) from " + table)) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        xaFacade.close();
    }

    void cancelAllTx() {
        xaFacade.recover().forEach(xaFacade::rollback);
    }
}
