package org.apache.flink.connector.jdbc.datasource.connections.xa;

import org.apache.flink.connector.jdbc.databases.derby.DerbyTestBase;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.EmptyTransactionXaException;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;

import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class SimpleXaConnectionProviderTest implements DerbyTestBase {

    private static final BooksTable TEST_TABLE = new BooksTable("XaTable");
    private static final TransactionId TRANSACTION_BASE = TransactionId.empty();

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TEST_TABLE);
    }

    public void assertBooks(List<BooksTable.BookEntry> expected) throws SQLException {
        List<BooksTable.BookEntry> current =
                TEST_TABLE.selectAllTable(getMetadata().getConnection());
        assertThat(current.size()).isEqualTo(expected.size());
        assertThat(current).isEqualTo(expected);
    }

    @Test
    void testSuccessfulTransaction() throws SQLException {
        List<BooksTable.BookEntry> expected =
                Arrays.asList(
                        new BooksTable.BookEntry(1, "title1", "author1", 100D, 10),
                        new BooksTable.BookEntry(2, "title2", "author2", 111D, 11));

        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Start transaction
            xa.start(xid);
            Connection connection = xa.getConnection();
            // Do modifications
            try (PreparedStatement ps =
                    connection.prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                for (BooksTable.BookEntry book : expected) {
                    TEST_TABLE.getStatementBuilder().accept(ps, book);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // Prepare the transaction
            xa.endAndPrepare(xid);
            // Commit the transaction
            xa.commit(xid, false);
        }

        assertBooks(expected);
    }

    @Test
    void testSuccessfulTransactionInTwoSteps() throws SQLException {
        List<BooksTable.BookEntry> expected =
                Arrays.asList(
                        new BooksTable.BookEntry(1, "title1", "author1", 100D, 10),
                        new BooksTable.BookEntry(2, "title2", "author2", 111D, 11));

        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Start transaction
            xa.start(xid);
            // Do modifications
            try (PreparedStatement ps =
                    xa.getConnection().prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                for (BooksTable.BookEntry book : expected) {
                    TEST_TABLE.getStatementBuilder().accept(ps, book);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // Prepare the transaction
            xa.endAndPrepare(xid);
        }

        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Commit the transaction
            xa.commit(xid, false);
        }

        assertBooks(expected);
    }

    @Test
    void testEmptyTransaction() throws SQLException {
        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        assertThatExceptionOfType(EmptyTransactionXaException.class)
                .isThrownBy(
                        () -> {
                            try (SimpleXaConnectionProvider xa =
                                    SimpleXaConnectionProvider.from(
                                            getMetadata().buildXaDataSource())) {
                                xa.open();
                                // Start transaction
                                xa.start(xid);
                                // Prepare the transaction
                                xa.endAndPrepare(xid);
                                // This should fail
                            }
                        })
                .withMessage("end response XA_RDONLY, xid: " + xid.toString());

        assertBooks(new ArrayList<>());
    }

    @Test
    void testEmptyFailAndRollbackTransaction() throws SQLException {

        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Start transaction
            xa.start(xid);
            // Rollback the transaction
            xa.failAndRollback(xid);
        }

        assertBooks(new ArrayList<>());
    }

    @Test
    void testFailAndRollbackWithTransaction() throws SQLException {
        List<BooksTable.BookEntry> expected =
                Arrays.asList(
                        new BooksTable.BookEntry(1, "title1", "author1", 100D, 10),
                        new BooksTable.BookEntry(2, "title2", "author2", 111D, 11));

        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Start transaction
            xa.start(xid);
            Connection connection = xa.getConnection();
            // Do modifications
            try (PreparedStatement ps =
                    connection.prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                for (BooksTable.BookEntry book : expected) {
                    TEST_TABLE.getStatementBuilder().accept(ps, book);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // Rollback the transaction
            xa.failAndRollback(xid);
        }

        assertBooks(new ArrayList<>());
    }

    @Test
    void testRollbackTransaction() throws SQLException {
        List<BooksTable.BookEntry> expected =
                Arrays.asList(
                        new BooksTable.BookEntry(1, "title1", "author1", 100D, 10),
                        new BooksTable.BookEntry(2, "title2", "author2", 111D, 11));

        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Start transaction
            xa.start(xid);
            Connection connection = xa.getConnection();
            // Do modifications
            try (PreparedStatement ps =
                    connection.prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                for (BooksTable.BookEntry book : expected) {
                    TEST_TABLE.getStatementBuilder().accept(ps, book);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // Prepare the transaction
            xa.endAndPrepare(xid);
            // Rollback the transaction
            xa.rollback(xid);
        }

        assertBooks(new ArrayList<>());
    }
}
