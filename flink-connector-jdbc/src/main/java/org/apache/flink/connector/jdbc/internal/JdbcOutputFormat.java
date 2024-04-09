/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.SerializableSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A JDBC outputFormat that supports batching records before writing records to database. */
@Internal
public class JdbcOutputFormat<In, JdbcIn, JdbcExec extends JdbcBatchStatementExecutor<JdbcIn>>
        implements Flushable, AutoCloseable, Serializable {

    protected final JdbcConnectionProvider connectionProvider;

    /**
     * A factory for creating {@link JdbcBatchStatementExecutor} instance.
     *
     * @param <T> The type of instance.
     */
    public interface StatementExecutorFactory<T extends JdbcBatchStatementExecutor<?>>
            extends SerializableSupplier<T> {}

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

    private final JdbcExecutionOptions executionOptions;
    private final StatementExecutorFactory<JdbcExec> statementExecutorFactory;

    @SuppressWarnings("unchecked")
    protected Function<In, JdbcIn> getExtractor() {
        return in -> (JdbcIn) in;
    }

    private transient JdbcOutputSerializer<In> serializer;
    private transient JdbcExec jdbcStatementExecutor;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public JdbcOutputFormat(
            @Nonnull JdbcConnectionProvider connectionProvider,
            @Nonnull JdbcExecutionOptions executionOptions,
            @Nonnull StatementExecutorFactory<JdbcExec> statementExecutorFactory) {
        this.connectionProvider = checkNotNull(connectionProvider);
        this.executionOptions = checkNotNull(executionOptions);
        this.statementExecutorFactory = checkNotNull(statementExecutorFactory);
    }

    /** Connects to the target database and initializes the prepared statement. */
    public void open(@Nonnull JdbcOutputSerializer<In> serializer) throws IOException {
        this.serializer = checkNotNull(serializer, "Serializer must be defined");
        try {
            connectionProvider.getOrEstablishConnection();
        } catch (Exception e) {
            throw new IOException("unable to open JDBC writer", e);
        }

        jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("jdbc-upsert-output-format"));
            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (JdbcOutputFormat.this) {
                                    if (!closed) {
                                        try {
                                            flush();
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            executionOptions.getBatchIntervalMs(),
                            executionOptions.getBatchIntervalMs(),
                            TimeUnit.MILLISECONDS);
        }
    }

    private JdbcExec createAndOpenStatementExecutor(
            StatementExecutorFactory<JdbcExec> statementExecutorFactory) throws IOException {
        JdbcExec exec = statementExecutorFactory.get();
        try {
            exec.prepareStatements(connectionProvider.getConnection());
        } catch (SQLException e) {
            throw new IOException("unable to open JDBC writer", e);
        }
        return exec;
    }

    public void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to JDBC failed.", flushException);
        }
    }

    public final synchronized void writeRecord(In record) throws IOException {
        checkFlushException();

        try {
            In recordCopy = copyIfNecessary(record);
            addToBatch(record, getExtractor().apply(recordCopy));
            batchCount++;
            if (executionOptions.getBatchSize() > 0
                    && batchCount >= executionOptions.getBatchSize()) {
                flush();
            }
        } catch (Exception e) {
            throw new IOException("Writing records to JDBC failed.", e);
        }
    }

    private In copyIfNecessary(In record) {
        return this.serializer.serialize(record);
    }

    protected void addToBatch(In original, JdbcIn extracted) throws SQLException {
        jdbcStatementExecutor.addToBatch(extracted);
    }

    @Override
    public synchronized void flush() throws IOException {
        checkFlushException();

        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                attemptFlush();
                batchCount = 0;
                break;
            } catch (SQLException e) {
                LOG.error("JDBC executeBatch error, retry times = {}", i, e);
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    if (!connectionProvider.isConnectionValid()) {
                        updateExecutor(true);
                    }
                } catch (Exception exception) {
                    LOG.error(
                            "JDBC connection is not valid, and reestablish connection failed.",
                            exception);
                    throw new IOException("Reestablish JDBC connection failed", exception);
                }
                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    protected void attemptFlush() throws SQLException {
        jdbcStatementExecutor.executeBatch();
    }

    /** Executes prepared statement and closes all resources of this instance. */
    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.warn("Writing records to JDBC failed.", e);
                    throw new RuntimeException("Writing records to JDBC failed.", e);
                }
            }

            try {
                if (jdbcStatementExecutor != null) {
                    jdbcStatementExecutor.closeStatements();
                }
            } catch (SQLException e) {
                LOG.warn("Close JDBC writer failed.", e);
            }
        }
        connectionProvider.closeConnection();
        checkFlushException();
    }

    public void updateExecutor(boolean reconnect) throws SQLException, ClassNotFoundException {
        jdbcStatementExecutor.closeStatements();
        jdbcStatementExecutor.prepareStatements(
                reconnect
                        ? connectionProvider.reestablishConnection()
                        : connectionProvider.getConnection());
    }

    /** Returns configured {@code JdbcExecutionOptions}. */
    public JdbcExecutionOptions getExecutionOptions() {
        return executionOptions;
    }

    @VisibleForTesting
    public Connection getConnection() {
        return connectionProvider.getConnection();
    }
}
