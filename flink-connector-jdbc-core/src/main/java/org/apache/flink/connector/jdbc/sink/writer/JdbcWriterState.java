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

package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import javax.annotation.concurrent.ThreadSafe;
import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/** Thread-safe (assuming immutable {@link Xid} implementation). */
@ThreadSafe
@Internal
public class JdbcWriterState implements Serializable {
    private final Collection<TransactionId> prepared;
    private final Collection<TransactionId> hanging;

    public static JdbcWriterState empty() {
        return new JdbcWriterState(Collections.emptyList(), Collections.emptyList());
    }

    public static JdbcWriterState of(
            Collection<TransactionId> prepared, Collection<TransactionId> hanging) {
        return new JdbcWriterState(
                Collections.unmodifiableList(new ArrayList<>(prepared)),
                Collections.unmodifiableList(new ArrayList<>(hanging)));
    }

    protected JdbcWriterState(
            Collection<TransactionId> prepared, Collection<TransactionId> hanging) {
        this.prepared = prepared;
        this.hanging = hanging;
    }

    /**
     * @return immutable collection of prepared XA transactions to {@link
     *     javax.transaction.xa.XAResource#commit commit}.
     */
    public Collection<TransactionId> getPrepared() {
        return prepared;
    }

    /**
     * @return immutable collection of XA transactions to {@link
     *     javax.transaction.xa.XAResource#rollback rollback} (if they were prepared) or {@link
     *     javax.transaction.xa.XAResource#end end} (if they were only started).
     */
    public Collection<TransactionId> getHanging() {
        return hanging;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JdbcWriterState that = (JdbcWriterState) o;
        return new EqualsBuilder()
                .append(prepared, that.prepared)
                .append(hanging, that.hanging)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(prepared).append(hanging).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("prepared", prepared)
                .append("hanging", hanging)
                .toString();
    }
}
