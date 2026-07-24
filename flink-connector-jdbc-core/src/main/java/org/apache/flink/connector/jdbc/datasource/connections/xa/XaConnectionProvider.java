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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.TransientXaException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import java.util.Collection;

/**
 * Provider to the XA operations.
 *
 * <p>Typical workflow:
 *
 * <ol>
 *   <li>{@link #open}
 *   <li>{@link #start} transaction
 *   <li>{@link #getConnection}, write some data
 *   <li>{@link #endAndPrepare} (or {@link #failAndRollback})
 *   <li>{@link #commit} / {@link #rollback}
 *   <li>{@link #close}
 * </ol>
 *
 * {@link #recover} can be used to get abandoned prepared transactions for cleanup.
 */
@PublicEvolving
public interface XaConnectionProvider extends JdbcConnectionProvider {

    void open() throws Exception;

    boolean isOpen();

    /** Start a new transaction. */
    void start(Xid xid) throws Exception;

    /** End and then prepare the transaction. Transaction can't be resumed afterwards. */
    void endAndPrepare(Xid xid) throws Exception;

    /**
     * Commit previously prepared transaction.
     *
     * @param ignoreUnknown whether to ignore {@link XAException#XAER_NOTA XAER_NOTA} error.
     */
    void commit(Xid xid, boolean ignoreUnknown) throws TransientXaException;

    /** Rollback previously prepared transaction. */
    void rollback(Xid xid) throws TransientXaException;

    /**
     * End transaction as {@link javax.transaction.xa.XAResource#TMFAIL failed}; in case of error,
     * try to roll it back.
     */
    void failAndRollback(Xid xid) throws TransientXaException;

    /**
     * Note: this can block on some non-MVCC databases if there are ended not prepared transactions.
     */
    Collection<Xid> recover() throws TransientXaException;
}
