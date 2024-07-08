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

package org.apache.flink.connector.jdbc.core.datastream.source.reader;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.jdbc.core.datastream.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;

import static org.apache.flink.connector.jdbc.core.datastream.source.JdbcSourceOptions.AUTO_COMMIT;
import static org.apache.flink.connector.jdbc.core.datastream.source.JdbcSourceOptions.READER_FETCH_BATCH_SIZE;
import static org.apache.flink.connector.jdbc.core.datastream.source.JdbcSourceOptions.RESULTSET_CONCURRENCY;
import static org.apache.flink.connector.jdbc.core.datastream.source.JdbcSourceOptions.RESULTSET_FETCH_SIZE;
import static org.apache.flink.connector.jdbc.core.datastream.source.JdbcSourceOptions.RESULTSET_TYPE;

/**
 * The JDBC source reader to read data from jdbc splits.
 *
 * @param <T> The type of the record read from the source.
 */
public class JdbcSourceSplitReader<T>
        implements SplitReader<RecordAndOffset<T>, JdbcSourceSplit>, ResultTypeQueryable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSplitReader.class);

    private final Configuration config;
    @Nullable private JdbcSourceSplit currentSplit;
    private final Queue<JdbcSourceSplit> splits;
    private final TypeInformation<T> typeInformation;
    private final JdbcConnectionProvider connectionProvider;

    private transient Connection connection;
    private transient PreparedStatement statement;
    // Boolean to distinguish between default value and explicitly set autoCommit mode.
    private final Boolean autoCommit;
    private transient ResultSet resultSet;
    protected boolean hasNextRecordCurrentSplit;
    private int currentSplitOffset;

    private final ResultExtractor<T> resultExtractor;

    private final DeliveryGuarantee deliveryGuarantee;

    private final int splitReaderFetchBatchSize;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetFetchSize;

    private final SourceReaderContext context;

    private @Nullable JdbcSourceSplit skippedSplit;

    public JdbcSourceSplitReader(
            SourceReaderContext context,
            Configuration config,
            TypeInformation<T> typeInformation,
            JdbcConnectionProvider connectionProvider,
            DeliveryGuarantee deliveryGuarantee,
            ResultExtractor<T> resultExtractor) {
        this.context = Preconditions.checkNotNull(context);
        this.config = Preconditions.checkNotNull(config);
        this.typeInformation = Preconditions.checkNotNull(typeInformation);
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.resultSetType = config.getInteger(RESULTSET_TYPE);
        this.resultSetConcurrency = config.getInteger(RESULTSET_CONCURRENCY);
        this.resultSetFetchSize = config.getInteger(RESULTSET_FETCH_SIZE);
        this.autoCommit = config.getBoolean(AUTO_COMMIT);
        this.deliveryGuarantee = Preconditions.checkNotNull(deliveryGuarantee);
        this.splits = new ArrayDeque<>();
        this.hasNextRecordCurrentSplit = false;
        this.currentSplit = null;
        int splitReaderFetchBatchSize = config.getInteger(READER_FETCH_BATCH_SIZE);
        Preconditions.checkArgument(
                splitReaderFetchBatchSize > 0 && splitReaderFetchBatchSize < Integer.MAX_VALUE);
        this.splitReaderFetchBatchSize = splitReaderFetchBatchSize;
        this.resultExtractor = Preconditions.checkNotNull(resultExtractor);
        this.currentSplitOffset = 0;
    }

    @Override
    public RecordsWithSplitIds<RecordAndOffset<T>> fetch() throws IOException {

        boolean couldFetch = checkSplitOrStartNext();
        if (!couldFetch) {
            return new RecordsBySplits.Builder<RecordAndOffset<T>>().build();
        }

        if (!hasNextRecordCurrentSplit) {
            return finishSplit();
        }

        RecordsBySplits.Builder<RecordAndOffset<T>> recordAndOffsetBuilder =
                new RecordsBySplits.Builder<>();
        Preconditions.checkState(currentSplit != null, "currentSplit");
        int batch = this.splitReaderFetchBatchSize;
        while (batch > 0 && hasNextRecordCurrentSplit) {
            try {
                T record = resultExtractor.extract(resultSet);
                recordAndOffsetBuilder.add(
                        currentSplit, new RecordAndOffset<>(record, ++currentSplitOffset, 0));
                batch--;
                hasNextRecordCurrentSplit = resultSet.next();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (!hasNextRecordCurrentSplit) {
            currentSplitOffset = 0;
            recordAndOffsetBuilder.addFinishedSplit(currentSplit.splitId());
            closeResultSetAndStatement();
        }
        return recordAndOffsetBuilder.build();
    }

    private RecordsWithSplitIds<RecordAndOffset<T>> finishSplit() {

        closeResultSetAndStatement();

        RecordsBySplits.Builder<RecordAndOffset<T>> builder = new RecordsBySplits.Builder<>();
        JdbcSourceSplit splitToFinish = Objects.nonNull(currentSplit) ? currentSplit : skippedSplit;
        Preconditions.checkState(splitToFinish != null, "Split to finish mustn't be null.");
        builder.addFinishedSplit(splitToFinish.splitId());
        currentSplit = null;
        skippedSplit = null;
        return builder.build();
    }

    private void closeResultSetAndStatement() {
        closeResultSetIfNeeded();
        closeStatementIfNeeded();
    }

    private void closeResultSetIfNeeded() {
        try {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            resultSet = null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void closeStatementIfNeeded() {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            statement = null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<JdbcSourceSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOG.debug("Handling split change {}", splitsChanges);
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        closeResultSetAndStatement();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        connection = null;
        currentSplit = null;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInformation;
    }

    @VisibleForTesting
    public List<JdbcSourceSplit> getSplits() {
        return Collections.unmodifiableList(Arrays.asList(splits.toArray(new JdbcSourceSplit[0])));
    }

    @VisibleForTesting
    public Connection getConnection() {
        return connection;
    }

    @VisibleForTesting
    public PreparedStatement getStatement() {
        return statement;
    }

    @VisibleForTesting
    public ResultSet getResultSet() {
        return resultSet;
    }

    // ---------------- Private methods --------------------------------

    private boolean checkSplitOrStartNext() {

        try {
            if (hasNextRecordCurrentSplit && resultSet != null) {
                return true;
            }

            final JdbcSourceSplit nextSplit = splits.poll();
            if (nextSplit != null) {
                currentSplit = nextSplit;
                openResultSetForSplit(currentSplit);
                return true;
            }
            return false;
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        connection = connectionProvider.getOrEstablishConnection();
        if (autoCommit == null) {
            return;
        }
        if (autoCommit != connection.getAutoCommit()) {
            connection.setAutoCommit(autoCommit);
        }
    }

    private void openResultSetForSplit(JdbcSourceSplit split)
            throws SQLException, ClassNotFoundException {
        switch (deliveryGuarantee) {
            case EXACTLY_ONCE:
                openResultSetForSplitWhenExactlyOnce(split);
                break;
            case AT_LEAST_ONCE:
                openResultSetForSplitWhenAtLeastOnce(split);
                break;
            case NONE:
            default:
                openResultSetForSplitWhenAtMostOnce(split);
                break;
        }
    }

    private void openResultSetForSplitWhenAtMostOnce(JdbcSourceSplit split)
            throws SQLException, ClassNotFoundException {
        if (split.getReaderPosition() != 0) {
            skippedSplit = currentSplit;
            currentSplit = null;
            hasNextRecordCurrentSplit = false;
            currentSplitOffset = 0;
            closeResultSetAndStatement();
        } else {
            openResultSetForSplitWhenAtLeastOnce(split);
        }
    }

    private void openResultSetForSplitWhenExactlyOnce(JdbcSourceSplit split)
            throws SQLException, ClassNotFoundException {
        getOrEstablishConnection();
        closeResultSetIfNeeded();
        prepareStatement(split);
        resultSet = statement.executeQuery();
        currentSplitOffset = 0;
        hasNextRecordCurrentSplit = resultSet.next();
        if (hasNextRecordCurrentSplit) {
            moveResultSetCursorByOffset();
        }
    }

    private void moveResultSetCursorByOffset() throws SQLException {
        int resultSetOffset = currentSplit.getReaderPosition();
        if (resultSetOffset == 0) {
            return;
        }
        resultSet.last();
        int last = resultSet.getRow();
        resultSet.absolute(1);
        if (resultSetOffset < last) {
            currentSplitOffset = resultSetOffset;
            resultSet.absolute(resultSetOffset + 1);
        } else {
            hasNextRecordCurrentSplit = false;
            LOG.warn(
                    "The offset will not be set from splitState, because the last cursor is {}, the expected cursor is {}.",
                    last,
                    resultSetOffset + 1);
        }
    }

    private void openResultSetForSplitWhenAtLeastOnce(JdbcSourceSplit split)
            throws SQLException, ClassNotFoundException {
        getOrEstablishConnection();
        closeResultSetIfNeeded();
        prepareStatement(split);
        resultSet = statement.executeQuery();
        // AT_LEAST_ONCE
        hasNextRecordCurrentSplit = resultSet.next();
        currentSplitOffset = 0;
    }

    private void prepareStatement(JdbcSourceSplit split) throws SQLException {
        closeStatementIfNeeded();
        statement =
                connection.prepareStatement(
                        split.getSqlTemplate(), resultSetType, resultSetConcurrency);
        if (split.getParameters() != null) {
            Object[] objs = split.getParameters();
            for (int i = 0; i < objs.length; i++) {
                statement.setObject(i + 1, objs[i]);
            }
        }
        statement.setFetchSize(resultSetFetchSize);
    }
}
