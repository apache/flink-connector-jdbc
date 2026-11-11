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
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.XaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.TransientXaException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.Collection;
import java.util.OptionalLong;

/** The responsible for committing the {@link JdbcCommitable}. */
@Internal
public class JdbcCommitter implements Committer<JdbcCommitable> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCommitter.class);

    private transient XaConnectionProvider xaConnectionProvider;

    private final DeliveryGuarantee deliveryGuarantee;
    private final int maxCommitAttempts;
    private final OptionalLong restoredCheckpointId;

    public JdbcCommitter(
            DeliveryGuarantee deliveryGuarantee,
            JdbcConnectionProvider connectionProvider,
            JdbcExactlyOnceOptions exactlyOnceOptions,
            OptionalLong restoredCheckpointId)
            throws IOException {

        if (DeliveryGuarantee.EXACTLY_ONCE == deliveryGuarantee) {
            this.xaConnectionProvider = (XaConnectionProvider) connectionProvider;
            try {
                this.xaConnectionProvider.open();
            } catch (Exception e) {
                ExceptionUtils.rethrowIOException(e);
            }
            this.maxCommitAttempts = exactlyOnceOptions.getMaxCommitAttempts();
        } else {
            this.maxCommitAttempts = 0;
        }

        this.deliveryGuarantee = deliveryGuarantee;
        this.restoredCheckpointId = restoredCheckpointId;
    }

    @Override
    public void commit(Collection<CommitRequest<JdbcCommitable>> committables)
            throws IOException, InterruptedException {

        if (DeliveryGuarantee.EXACTLY_ONCE != this.deliveryGuarantee && !committables.isEmpty()) {
            throw new FlinkRuntimeException("Non XA sink with commitables");
        }

        for (CommitRequest<JdbcCommitable> request : committables) {
            Xid xid = request.getCommittable().getXid();
            try {
                xaConnectionProvider.commit(xid, isRestored(xid));
            } catch (TransientXaException e) {
                if (request.getNumberOfRetries() + 1 >= maxCommitAttempts) {
                    request.signalFailedWithUnknownReason(e);
                } else {
                    LOG.warn("failed to commit transaction {} (retrying)", xid, e);
                    request.retryLater();
                }
            }
        }
    }

    /**
     * A transaction from before the restored checkpoint may have been committed before the failure.
     * Any other transaction unknown to the database was not committed.
     */
    private boolean isRestored(Xid xid) throws IOException {
        if (!restoredCheckpointId.isPresent()) {
            return false;
        }
        TransactionId transactionId =
                TransactionId.createFromXid(
                        xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
        return transactionId.getCheckpointId() < restoredCheckpointId.getAsLong();
    }

    @Override
    public void close() throws Exception {
        if (this.xaConnectionProvider != null) {
            this.xaConnectionProvider.close();
        }
    }
}
