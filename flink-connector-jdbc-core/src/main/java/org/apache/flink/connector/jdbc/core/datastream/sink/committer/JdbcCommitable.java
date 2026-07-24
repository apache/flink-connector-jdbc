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

package org.apache.flink.connector.jdbc.core.datastream.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.XaTransaction;

import javax.annotation.Nullable;
import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.util.Optional;

/** A pair of Xid and transaction that can be committed. */
@Internal
public class JdbcCommitable implements Serializable {

    private final Xid xid;
    private final XaTransaction transaction;

    protected JdbcCommitable(Xid xid, @Nullable XaTransaction transaction) {
        this.xid = xid;
        this.transaction = transaction;
    }

    public static JdbcCommitable of(Xid xid) {
        return of(xid, null);
    }

    public static JdbcCommitable of(Xid xid, XaTransaction transaction) {
        return new JdbcCommitable(xid, transaction);
    }

    public Xid getXid() {
        return xid;
    }

    public Optional<XaTransaction> getTransaction() {
        return Optional.ofNullable(transaction);
    }
}
