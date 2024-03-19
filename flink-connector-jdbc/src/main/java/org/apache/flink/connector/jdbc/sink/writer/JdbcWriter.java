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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.XaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.XaTransaction;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.sink.committer.JdbcCommitable;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Jdbc writer that allow at-least-once (non-XA operation) and exactly-once (XA operation)
 * semantics.
 */
@Internal
public class JdbcWriter<IN>
        implements StatefulSink.StatefulSinkWriter<IN, JdbcWriterState>,
                TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, JdbcCommitable> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriter.class);

    private final DeliveryGuarantee deliveryGuarantee;
    private final JdbcOutputFormat<IN, IN, JdbcBatchStatementExecutor<IN>> jdbcOutput;

    private XaTransaction jdbcTransaction;
    private long lastCheckpointId;
    private boolean pendingRecords;

    public JdbcWriter(
            JdbcConnectionProvider connectionProvider,
            JdbcExecutionOptions executionOptions,
            JdbcExactlyOnceOptions exactlyOnceOptions,
            JdbcQueryStatement<IN> queryStatement,
            JdbcOutputSerializer<IN> outputSerializer,
            DeliveryGuarantee deliveryGuarantee,
            Collection<JdbcWriterState> recoveredState,
            Sink.InitContext initContext)
            throws IOException {

        this.deliveryGuarantee =
                checkNotNull(deliveryGuarantee, "deliveryGuarantee must be defined");

        checkNotNull(initContext, "initContext must be defined");

        pendingRecords = false;
        this.lastCheckpointId =
                initContext
                        .getRestoredCheckpointId()
                        .orElse(Sink.InitContext.INITIAL_CHECKPOINT_ID - 1);

        checkNotNull(connectionProvider, "connectionProvider must be defined");

        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            checkArgument(
                    executionOptions.getMaxRetries() == 0,
                    "JDBC XA sink requires maxRetries equal to 0, otherwise it could "
                            + "cause duplicates. See issue FLINK-22311 for details.");

            checkNotNull(exactlyOnceOptions, "exactlyOnceOptions must be defined");
            checkNotNull(recoveredState, "recoveredState must be defined");
            checkState(recoveredState.size() <= 1, "more than one state to recover");

            JdbcWriterState state =
                    recoveredState.stream().findFirst().orElse(JdbcWriterState.empty());

            TransactionId transactionId =
                    TransactionId.create(
                            initContext.getJobId().getBytes(),
                            initContext.getSubtaskId(),
                            initContext.getNumberOfParallelSubtasks());

            this.jdbcTransaction =
                    new XaTransaction(
                            exactlyOnceOptions,
                            transactionId,
                            ((XaConnectionProvider) connectionProvider));
            this.jdbcTransaction.open(state);
            this.jdbcTransaction.createTx(lastCheckpointId);
        }

        checkNotNull(executionOptions, "executionOptions must be defined");
        checkNotNull(queryStatement, "queryStatement must be defined");
        this.jdbcOutput =
                new JdbcOutputFormat<>(
                        connectionProvider,
                        executionOptions,
                        () ->
                                JdbcBatchStatementExecutor.simple(
                                        queryStatement.query(), queryStatement::statement));
        this.jdbcOutput.open(outputSerializer);
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            jdbcTransaction.checkState();
        }
        this.jdbcOutput.writeRecord(element);
        if (!this.pendingRecords) {
            this.pendingRecords = true;
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE || endOfInput) {
            LOG.debug("final flush={}", endOfInput);
            flush();
        } else {
            jdbcOutput.checkFlushException();
        }
    }

    private void flush() throws IOException {
        jdbcOutput.flush();
        jdbcOutput.checkFlushException();
    }

    @Override
    public Collection<JdbcCommitable> prepareCommit() throws IOException, InterruptedException {
        if (deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE) {
            return Collections.emptyList();
        }

        Xid currentXid = jdbcTransaction.getCurrentXid();

        jdbcTransaction.checkState();

        flush();
        jdbcTransaction.prepareTx();

        // If no records, commit the empty transaction
        if (!this.pendingRecords) {
            jdbcTransaction.commitTxUntil(this.lastCheckpointId);
            return Collections.emptyList();
        }

        // otherwise, return a committable
        this.pendingRecords = false;
        final JdbcCommitable committable = JdbcCommitable.of(currentXid, jdbcTransaction);
        LOG.debug("Committing {} committable.", committable);
        return Collections.singletonList(committable);
    }

    @Override
    public List<JdbcWriterState> snapshotState(long checkpointId) throws IOException {
        if (deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE) {
            return Collections.emptyList();
        }

        Preconditions.checkState(
                checkpointId > lastCheckpointId,
                "Expected %s > %s",
                checkpointId,
                lastCheckpointId);

        // If no records, commit the empty transaction
        if (!this.pendingRecords) {
            jdbcTransaction.commitTxUntil(this.lastCheckpointId);
        }

        this.lastCheckpointId = checkpointId;
        jdbcTransaction.createTx(this.lastCheckpointId);
        return Collections.singletonList(jdbcTransaction.getState());
    }

    @Override
    public void close() throws Exception {
        if (deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE) {
            this.jdbcOutput.close();
        } else {
            // don't jdbcOutput.close(); as we don't want neither to flush nor to close connection
            // here
            this.jdbcOutput.checkFlushException();
            this.jdbcTransaction.close();
        }
    }
}
