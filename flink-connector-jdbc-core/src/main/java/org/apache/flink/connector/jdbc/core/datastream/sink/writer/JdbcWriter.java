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

package org.apache.flink.connector.jdbc.core.datastream.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.committer.JdbcCommitable;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.XaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.XaTransaction;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
        implements StatefulSinkWriter<IN, JdbcWriterState>,
                CommittingSinkWriter<IN, JdbcCommitable> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWriter.class);

    private final JdbcOutputFormat<IN, ?, ?> jdbcOutput;

    /** The XA transaction of an exactly-once writer; null for at-least-once. */
    @Nullable private final XaTransaction jdbcTransaction;

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
            WriterInitContext initContext)
            throws IOException {
        // evaluated left to right: the XA branch is started before the private constructor opens
        // the format on its connection, and the format must not touch the provider before start()
        this(
                simpleOutputFormat(
                        checkNotNull(connectionProvider, "connectionProvider must be defined"),
                        checkNotNull(executionOptions, "executionOptions must be defined"),
                        checkNotNull(queryStatement, "queryStatement must be defined")),
                outputSerializer,
                checkNotNull(deliveryGuarantee, "deliveryGuarantee must be defined")
                                == DeliveryGuarantee.EXACTLY_ONCE
                        ? openXaTransaction(
                                connectionProvider,
                                executionOptions,
                                exactlyOnceOptions,
                                recoveredState,
                                checkNotNull(initContext, "initContext must be defined"))
                        : null,
                initContext);
    }

    /**
     * An at-least-once writer over a format built for it, for the Table sink. The writer opens and
     * closes the format; its executor must accept the writer's input type, there is no extraction
     * step in between.
     */
    public JdbcWriter(
            JdbcOutputFormat<IN, ?, ?> jdbcOutput,
            JdbcOutputSerializer<IN> outputSerializer,
            WriterInitContext initContext)
            throws IOException {
        this(jdbcOutput, outputSerializer, null, initContext);
    }

    private JdbcWriter(
            JdbcOutputFormat<IN, ?, ?> jdbcOutput,
            JdbcOutputSerializer<IN> outputSerializer,
            @Nullable XaTransaction jdbcTransaction,
            WriterInitContext initContext)
            throws IOException {
        this.jdbcOutput = checkNotNull(jdbcOutput, "jdbcOutput must be defined");
        this.jdbcTransaction = jdbcTransaction;
        this.lastCheckpointId =
                lastCheckpointId(checkNotNull(initContext, "initContext must be defined"));
        this.pendingRecords = false;
        try {
            this.jdbcOutput.open(outputSerializer);
        } catch (IOException | RuntimeException e) {
            // nobody calls close() on a writer whose constructor threw; without this the
            // connection the format established, or the XA branch, stays open
            try {
                releaseResources();
            } catch (Exception closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    /** Rejects execution options an exactly-once writer cannot run with. */
    public static void checkExactlyOnceExecutionOptions(JdbcExecutionOptions executionOptions) {
        checkArgument(
                executionOptions.getMaxRetries() == 0,
                "JDBC XA sink requires maxRetries equal to 0, otherwise it could "
                        + "cause duplicates. See issue FLINK-22311 for details.");
    }

    private static long lastCheckpointId(WriterInitContext initContext) {
        return initContext
                .getRestoredCheckpointId()
                .orElse(WriterInitContext.INITIAL_CHECKPOINT_ID - 1);
    }

    /**
     * Opens the XA connection, recovers {@code recoveredState} and starts the branch of the last
     * checkpoint. The transaction is closed again when any of that fails.
     */
    private static XaTransaction openXaTransaction(
            JdbcConnectionProvider connectionProvider,
            JdbcExecutionOptions executionOptions,
            JdbcExactlyOnceOptions exactlyOnceOptions,
            Collection<JdbcWriterState> recoveredState,
            WriterInitContext initContext)
            throws IOException {
        checkExactlyOnceExecutionOptions(executionOptions);
        checkArgument(
                connectionProvider instanceof XaConnectionProvider,
                "exactly-once delivery needs an XaConnectionProvider, got %s",
                connectionProvider.getClass().getName());
        checkNotNull(exactlyOnceOptions, "exactlyOnceOptions must be defined");
        checkNotNull(recoveredState, "recoveredState must be defined");
        checkState(recoveredState.size() <= 1, "more than one state to recover");
        JdbcWriterState state = recoveredState.stream().findFirst().orElse(JdbcWriterState.empty());
        TransactionId transactionId =
                TransactionId.create(
                        initContext.getJobInfo().getJobId().getBytes(),
                        initContext.getTaskInfo().getIndexOfThisSubtask(),
                        initContext.getTaskInfo().getNumberOfParallelSubtasks());
        XaTransaction transaction =
                new XaTransaction(
                        exactlyOnceOptions,
                        transactionId,
                        (XaConnectionProvider) connectionProvider);
        try {
            transaction.open(state);
            transaction.createTx(lastCheckpointId(initContext));
        } catch (IOException | RuntimeException e) {
            try {
                transaction.close();
            } catch (Exception closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        return transaction;
    }

    private static <IN> JdbcOutputFormat<IN, IN, JdbcBatchStatementExecutor<IN>> simpleOutputFormat(
            JdbcConnectionProvider connectionProvider,
            JdbcExecutionOptions executionOptions,
            JdbcQueryStatement<IN> queryStatement) {
        return new JdbcOutputFormat<>(
                connectionProvider,
                executionOptions,
                () ->
                        JdbcBatchStatementExecutor.simple(
                                queryStatement.query(), queryStatement::statement));
    }

    /** Releases what the constructor acquired; an open XA branch is rolled back with it. */
    private void releaseResources() throws Exception {
        if (jdbcTransaction == null) {
            jdbcOutput.close();
        } else {
            // not jdbcOutput.close(): it would flush, and close the connection the branch runs on
            jdbcTransaction.close();
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        if (jdbcTransaction != null) {
            jdbcTransaction.checkState();
        }
        this.jdbcOutput.writeRecord(element);
        if (!this.pendingRecords) {
            this.pendingRecords = true;
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (jdbcTransaction == null || endOfInput) {
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
        if (jdbcTransaction == null) {
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
        if (jdbcTransaction == null) {
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
        if (jdbcTransaction != null) {
            jdbcOutput.checkFlushException();
        }
        releaseResources();
    }
}
