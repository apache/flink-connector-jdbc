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

package org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.XaTransaction;
import org.apache.flink.util.FlinkRuntimeException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

/**
 * Thrown by {@link XaTransaction} when RM responds with {@link
 * javax.transaction.xa.XAResource#XA_RDONLY XA_RDONLY} indicating that the transaction doesn't
 * include any changes. When such a transaction is committed RM may return an error (usually, {@link
 * XAException#XAER_NOTA XAER_NOTA}).
 */
@PublicEvolving
public class EmptyTransactionXaException extends FlinkRuntimeException {
    private final Xid xid;

    public EmptyTransactionXaException(Xid xid) {
        super("end response XA_RDONLY, xid: " + xid);
        this.xid = xid;
    }

    public Xid getXid() {
        return xid;
    }
}
