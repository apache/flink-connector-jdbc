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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.EmptyTransactionXaException;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.XaError;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static javax.transaction.xa.XAResource.TMENDRSCAN;
import static javax.transaction.xa.XAResource.TMNOFLAGS;
import static javax.transaction.xa.XAResource.TMSTARTRSCAN;

/** Simple XA connection provider. */
@NotThreadSafe
@Internal
public class SimpleXaConnectionProvider implements XaConnectionProvider {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SimpleXaConnectionProvider.class);
    private static final int MAX_RECOVER_CALLS = 100;

    private final Supplier<XADataSource> dataSourceSupplier;
    private final Integer timeoutSec;
    private transient XAResource xaResource;
    private transient Connection connection;
    private transient XAConnection xaConnection;

    @VisibleForTesting
    public static SimpleXaConnectionProvider from(XADataSource ds) {
        return from(ds, null);
    }

    public static SimpleXaConnectionProvider from(XADataSource ds, Integer timeoutSec) {
        return from(() -> ds, timeoutSec);
    }

    public static SimpleXaConnectionProvider from(
            Supplier<XADataSource> dsSupplier, Integer timeoutSec) {
        return new SimpleXaConnectionProvider(dsSupplier, timeoutSec);
    }

    private SimpleXaConnectionProvider(
            Supplier<XADataSource> dataSourceSupplier, Integer timeoutSec) {
        this.dataSourceSupplier = Preconditions.checkNotNull(dataSourceSupplier);
        this.timeoutSec = timeoutSec;
    }

    @Override
    public void open() throws SQLException {
        Preconditions.checkState(!isOpen(), "already connected");
        XADataSource ds = dataSourceSupplier.get();
        xaConnection = ds.getXAConnection();
        xaResource = xaConnection.getXAResource();
        if (timeoutSec != null) {
            try {
                xaResource.setTransactionTimeout(timeoutSec);
            } catch (XAException e) {
                throw new SQLException(e);
            }
        }
        connection = xaConnection.getConnection();
        connection.setReadOnly(false);
        connection.setAutoCommit(false);
        Preconditions.checkState(!connection.getAutoCommit());
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) {
            connection.close(); // close connection - likely a wrapper
            connection = null;
        }
        try {
            xaConnection.close(); // close likely a pooled AND the underlying connection
        } catch (SQLException e) {
            // Some databases (e.g. MySQL) rollback changes on normal client disconnect which
            // causes an exception if an XA transaction was prepared. Note that resources are
            // still released in case of an error. Pinning MySQL connections doesn't help as
            // SuspendableXAConnection has the same close() logic.
            // Other DBs don't rollback, e.g. for PgSql the previous connection.close() call
            // disassociates the connection (and that call works because it has a check for XA)
            // and rollback() is not called.
            // In either case, not closing the XA connection here leads to the resource leak.
            LOG.warn("unable to close XA connection", e);
        }
        xaResource = null;
    }

    @Override
    public Connection getConnection() {
        Preconditions.checkNotNull(connection);
        return connection;
    }

    @Override
    public boolean isOpen() {
        return xaResource != null;
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return isOpen() && connection.isValid(connection.getNetworkTimeout());
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException {
        if (!isOpen()) {
            open();
        }
        return connection;
    }

    @Override
    public void closeConnection() {
        try {
            close();
        } catch (SQLException e) {
            LOG.warn("Connection close failed.", e);
        }
    }

    @Override
    public Connection reestablishConnection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start(Xid xid) {
        execute(XaCommand.fromRunnable("start", xid, () -> xaResource.start(xid, TMNOFLAGS)));
    }

    @Override
    public void endAndPrepare(Xid xid) {
        execute(
                XaCommand.fromRunnable(
                        "end", xid, () -> xaResource.end(xid, XAResource.TMSUCCESS)));
        int prepResult = execute(new XaCommand<>("prepare", xid, () -> xaResource.prepare(xid)));
        if (prepResult == XAResource.XA_RDONLY) {
            throw new EmptyTransactionXaException(xid);
        } else if (prepResult != XAResource.XA_OK) {
            throw new FlinkRuntimeException(
                    formatErrorMessage("prepare", xid, "response: " + prepResult));
        }
    }

    @Override
    public void failAndRollback(Xid xid) {
        execute(
                XaCommand.fromRunnable(
                        "end (fail)",
                        xid,
                        () -> {
                            xaResource.end(xid, XAResource.TMFAIL);
                            xaResource.rollback(xid);
                        },
                        err -> {
                            if (err.errorCode >= XAException.XA_RBBASE) {
                                rollback(xid);
                            } else {
                                LOG.warn(formatErrorMessage("end (fail)", xid, err.errorCode));
                            }
                        }));
    }

    @Override
    public void commit(Xid xid, boolean ignoreUnknown) {
        execute(
                XaCommand.fromRunnableRecoverByWarn(
                        "commit",
                        xid,
                        () ->
                                xaResource.commit(
                                        xid,
                                        false /* not onePhase because the transaction should be prepared already */),
                        e -> buildCommitErrorDesc(e, ignoreUnknown)));
    }

    @Override
    public void rollback(Xid xid) {
        execute(
                XaCommand.fromRunnableRecoverByWarn(
                        "rollback",
                        xid,
                        () -> xaResource.rollback(xid),
                        this::buildRollbackErrorDesc));
    }

    private void forget(Xid xid) {
        execute(
                XaCommand.fromRunnableRecoverByWarn(
                        "forget",
                        xid,
                        () -> xaResource.forget(xid),
                        e -> Optional.of("manual cleanup may be required")));
    }

    @Override
    public Collection<Xid> recover() {
        return execute(
                new XaCommand<>(
                        "recover",
                        null,
                        () -> {
                            List<Xid> list = recover(TMSTARTRSCAN);
                            try {
                                for (int i = 0; list.addAll(recover(TMNOFLAGS)); i++) {
                                    // H2 sometimes returns same tx list here - should probably use
                                    // recover(TMSTARTRSCAN | TMENDRSCAN)
                                    Preconditions.checkState(
                                            i < MAX_RECOVER_CALLS, "too many xa_recover() calls");
                                }
                            } finally {
                                recover(TMENDRSCAN);
                            }
                            return list;
                        }));
    }

    private List<Xid> recover(int flags) throws XAException {
        return Arrays.asList(xaResource.recover(flags));
    }

    private <T> T execute(XaCommand<T> cmd) throws FlinkRuntimeException {
        Preconditions.checkState(isOpen(), "not connected");
        try {
            return cmd.execute();
        } catch (XAException e) {
            if (XaError.isHeurErrorCode(e.errorCode)) {
                cmd.getXid().ifPresent(this::forget);
            }
            return cmd.recover(e);
        } catch (FlinkRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw wrapException(cmd.getName(), cmd.getXid().orElse(null), e);
        }
    }

    private static FlinkRuntimeException wrapException(String action, Xid xid, Exception ex) {
        return XaError.wrapException(action, xid, ex);
    }

    private Optional<String> buildCommitErrorDesc(XAException err, boolean ignoreUnknown) {
        return XaError.buildCommitErrorDesc(err, ignoreUnknown);
    }

    private Optional<String> buildRollbackErrorDesc(XAException err) {
        return XaError.buildRollbackErrorDesc(err);
    }

    private static String formatErrorMessage(String action, Xid xid, String... more) {
        return formatErrorMessage(action, xid, null, more);
    }

    private static String formatErrorMessage(
            String action, Xid xid, Integer errorCode, String... more) {
        return XaError.errorMessage(action, xid, errorCode, more);
    }
}
