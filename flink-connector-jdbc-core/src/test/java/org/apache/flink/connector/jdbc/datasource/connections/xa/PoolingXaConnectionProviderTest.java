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

package org.apache.flink.connector.jdbc.datasource.connections.xa;

import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.EmptyTransactionXaException;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PoolingXaConnectionProvider} against a database with two-phase commit. */
public abstract class PoolingXaConnectionProviderTest implements DatabaseTest {

    private static final BooksTable TEST_TABLE = new BooksTable("PoolingXaTable");
    private static final TransactionId TRANSACTION_BASE = TransactionId.empty();

    private final AtomicInteger createdProviders = new AtomicInteger();

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TEST_TABLE);
    }

    /** A failed test can leave a prepared transaction behind, which holds its locks. */
    @AfterEach
    void rollbackPreparedTransactions() throws SQLException {
        try (SimpleXaConnectionProvider xa = newSimpleXaConnectionProvider()) {
            xa.open();
            for (Xid xid : xa.recover()) {
                xa.rollback(xid);
            }
        }
    }

    /** A prepared transaction is committed elsewhere, so the next one reuses its connection. */
    @Test
    void testReuseConnectionAfterPrepare() throws Exception {
        BooksTable.BookEntry book1 = new BooksTable.BookEntry(1, "title1", "author1", 100D, 10);
        BooksTable.BookEntry book2 = new BooksTable.BookEntry(2, "title2", "author2", 111D, 11);
        long checkpointId = System.currentTimeMillis();
        Xid xid1 = TRANSACTION_BASE.withBranch(checkpointId);
        Xid xid2 = TRANSACTION_BASE.withBranch(checkpointId + 1);

        try (PoolingXaConnectionProvider xa = newPoolingXaConnectionProvider()) {
            xa.open();
            xa.start(xid1);
            insert(xa.getConnection(), book1);
            xa.endAndPrepare(xid1);
            xa.start(xid2);
            insert(xa.getConnection(), book2);
            xa.endAndPrepare(xid2);
        }
        assertThat(createdProviders).hasValue(1);

        try (PoolingXaConnectionProvider xa = newPoolingXaConnectionProvider()) {
            xa.open();
            xa.commit(xid1, false);
            xa.commit(xid2, false);
        }
        try (Connection connection = getMetadata().getConnection()) {
            assertThat(TEST_TABLE.selectAllTable(connection))
                    .isEqualTo(Arrays.asList(book1, book2));
        }
    }

    @Test
    void testReuseConnectionAfterEmptyTransaction() throws Exception {
        long checkpointId = System.currentTimeMillis();
        Xid xid1 = TRANSACTION_BASE.withBranch(checkpointId);
        Xid xid2 = TRANSACTION_BASE.withBranch(checkpointId + 1);

        try (PoolingXaConnectionProvider xa = newPoolingXaConnectionProvider()) {
            xa.open();
            xa.start(xid1);
            try {
                xa.endAndPrepare(xid1);
            } catch (EmptyTransactionXaException e) {
                // read-only on some databases, e.g. Derby; PostgreSQL prepares it
            }
            xa.start(xid2);
            xa.failAndRollback(xid2);
        }
        assertThat(createdProviders).hasValue(1);
    }

    private PoolingXaConnectionProvider newPoolingXaConnectionProvider() {
        return PoolingXaConnectionProvider.from(
                () -> {
                    createdProviders.incrementAndGet();
                    return newSimpleXaConnectionProvider();
                });
    }

    private SimpleXaConnectionProvider newSimpleXaConnectionProvider() {
        return SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource());
    }

    private static void insert(Connection connection, BooksTable.BookEntry book)
            throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
            TEST_TABLE.getStatementBuilder().accept(ps, book);
            ps.execute();
        }
    }
}
