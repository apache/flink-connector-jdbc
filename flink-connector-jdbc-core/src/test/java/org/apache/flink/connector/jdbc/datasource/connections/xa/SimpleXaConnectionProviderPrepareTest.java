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

import org.junit.jupiter.api.Test;

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Tests how {@link SimpleXaConnectionProvider} handles the result of {@code prepare}. */
class SimpleXaConnectionProviderPrepareTest {

    @Test
    void testReadOnlyPrepareIsAnEmptyTransaction() throws SQLException {
        Xid xid = TransactionId.empty().withBranch(1L);
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(
                        new PrepareResultDataSource(XAResource.XA_RDONLY))) {
            xa.open();
            xa.start(xid);
            assertThatExceptionOfType(EmptyTransactionXaException.class)
                    .isThrownBy(() -> xa.endAndPrepare(xid))
                    .withMessage("end response XA_RDONLY, xid: " + xid);
        }
    }

    /** Hands out a plain H2 connection and an {@link XAResource} with a fixed prepare result. */
    private static class PrepareResultDataSource implements XADataSource, XAConnection, XAResource {
        private final int prepareResult;

        PrepareResultDataSource(int prepareResult) {
            this.prepareResult = prepareResult;
        }

        @Override
        public XAConnection getXAConnection() {
            return this;
        }

        @Override
        public XAConnection getXAConnection(String user, String password) {
            return this;
        }

        @Override
        public XAResource getXAResource() {
            return this;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection("jdbc:h2:mem:");
        }

        @Override
        public void close() {}

        @Override
        public int prepare(Xid xid) {
            return prepareResult;
        }

        @Override
        public void start(Xid xid, int flags) {}

        @Override
        public void end(Xid xid, int flags) {}

        @Override
        public void commit(Xid xid, boolean onePhase) {}

        @Override
        public void rollback(Xid xid) {}

        @Override
        public void forget(Xid xid) {}

        @Override
        public Xid[] recover(int flag) {
            return new Xid[0];
        }

        @Override
        public boolean isSameRM(XAResource other) {
            return other == this;
        }

        @Override
        public int getTransactionTimeout() {
            return 0;
        }

        @Override
        public boolean setTransactionTimeout(int seconds) {
            return false;
        }

        @Override
        public void addConnectionEventListener(ConnectionEventListener listener) {}

        @Override
        public void removeConnectionEventListener(ConnectionEventListener listener) {}

        @Override
        public void addStatementEventListener(StatementEventListener listener) {}

        @Override
        public void removeStatementEventListener(StatementEventListener listener) {}

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {}

        @Override
        public void setLoginTimeout(int seconds) {}

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }
    }
}
