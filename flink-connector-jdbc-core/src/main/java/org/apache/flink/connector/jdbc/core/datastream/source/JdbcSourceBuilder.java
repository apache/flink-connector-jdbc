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

package org.apache.flink.connector.jdbc.core.datastream.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.core.datastream.source.config.ContinuousUnBoundingSettings;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.SqlTemplateSplitEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.connector.jdbc.split.JdbcSlideTimingParameterProvider;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;

/**
 * A tool is used to build {@link JdbcSource} quickly.
 *
 * <pre><code>
 * JdbcSource&lt;Row> source = JdbcSource.&lt;Row>builder()
 *           .setSql(validSql)
 *           .setResultExtractor(new RowResultExtractor())
 *           .setDBUrl(dbUrl)
 *           .setDriverName(driverName)
 *           .setTypeInformation(new TypeHint&lt;Row>() {}.getTypeInfo())
 *           .setPassword(password)
 *           .setUsername(username)
 *           .build();
 * </code></pre>
 *
 * <p>In order to query the JDBC source in parallel, you need to provide a parameterized query
 * template (i.e. a valid {@link PreparedStatement}) and a {@link JdbcParameterValuesProvider} which
 * provides binding values for the query parameters. E.g.:
 *
 * <pre><code>
 *
 * Serializable[][] queryParameters = new String[2][1];
 * queryParameters[0] = new String[]{"Kumar"};
 * queryParameters[1] = new String[]{"Tan Ah Teck"};
 *
 * JdbcSource&lt;Row> jdbcSource =  JdbcSource.&lt;Row>builder()
 *          .setResultExtractor(new RowResultExtractor())
 *          .setTypeInformation(new TypeHint&lt;Row>() {}.getTypeInfo())
 *          .setPassword(password)
 *          .setUsername(username)
 * 			.setDriverName("org.apache.derby.jdbc.EmbeddedDriver")
 * 			.setDBUrl("jdbc:derby:memory:ebookshop")
 * 			.setSql("select * from books WHERE author = ?")
 * 			.setJdbcParameterValuesProvider(new JdbcGenericParameterValuesProvider(queryParameters))
 *          .build();
 * </code></pre>
 *
 * @see Row
 * @see JdbcParameterValuesProvider
 * @see PreparedStatement
 * @see DriverManager
 * @see JdbcSource
 */
@PublicEvolving
public class JdbcSourceBuilder<OUT> {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcSourceBuilder.class);

    public static final String INVALID_CONTINUOUS_SLIDE_TIMING_HINT =
            "The 'jdbcParameterValuesProvider' must be specified with in type of 'JdbcSlideTimingParameterProvider' when using 'continuousUnBoundingSettings'.";
    public static final String INVALID_SLIDE_TIMING_CONTINUOUS_HINT =
            "The 'continuousUnBoundingSettings' must be specified with in type of 'continuousUnBoundingSettings' when using 'jdbcParameterValuesProvider' in type of 'JdbcSlideTimingParameterProvider'.";

    private final Configuration configuration;

    private int splitReaderFetchBatchSize;
    private int resultSetType;
    private int resultSetConcurrency;
    private int resultSetFetchSize;
    // Boolean to distinguish between default value and explicitly set autoCommit mode.
    private Boolean autoCommit;

    private DeliveryGuarantee deliveryGuarantee;
    private @Nullable ContinuousUnBoundingSettings continuousUnBoundingSettings;

    private TypeInformation<OUT> typeInformation;

    private final JdbcConnectionOptions.JdbcConnectionOptionsBuilder connOptionsBuilder;
    private String sql;
    private JdbcParameterValuesProvider jdbcParameterValuesProvider;
    private @Nullable Serializable optionalSqlSplitEnumeratorState;
    private ResultExtractor<OUT> resultExtractor;

    private JdbcConnectionProvider connectionProvider;

    JdbcSourceBuilder() {
        this.configuration = new Configuration();
        this.connOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        this.splitReaderFetchBatchSize = JdbcSourceOptions.READER_FETCH_BATCH_SIZE.defaultValue();
        this.resultSetType = JdbcSourceOptions.RESULTSET_TYPE.defaultValue();
        this.resultSetConcurrency = JdbcSourceOptions.RESULTSET_CONCURRENCY.defaultValue();
        this.deliveryGuarantee = DeliveryGuarantee.NONE;
        // Boolean to distinguish between default value and explicitly set autoCommit mode.
        this.autoCommit = true;
    }

    public JdbcSourceBuilder<OUT> setSql(@Nonnull String sql) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(sql),
                "It's required to set the 'sql' with non-empty value.");
        this.sql = sql;
        return this;
    }

    public JdbcSourceBuilder<OUT> setResultExtractor(ResultExtractor<OUT> resultExtractor) {
        this.resultExtractor =
                Preconditions.checkNotNull(
                        resultExtractor, "It's required to set the 'resultExtractor'.");
        return this;
    }

    public JdbcSourceBuilder<OUT> setUsername(String username) {
        connOptionsBuilder.withUsername(username);
        return this;
    }

    public JdbcSourceBuilder<OUT> setPassword(String password) {
        connOptionsBuilder.withPassword(password);
        return this;
    }

    public JdbcSourceBuilder<OUT> setDriverName(String driverName) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(driverName),
                "It's required to set the 'driverName'.");
        connOptionsBuilder.withDriverName(driverName);
        return this;
    }

    public JdbcSourceBuilder<OUT> setDBUrl(String dbURL) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(dbURL), "It's required to set the 'dbURL'.");
        connOptionsBuilder.withUrl(dbURL);
        return this;
    }

    public JdbcSourceBuilder<OUT> setTypeInformation(
            @Nonnull TypeInformation<OUT> typeInformation) {
        this.typeInformation =
                Preconditions.checkNotNull(
                        typeInformation, "It's required to set the 'typeInformation'.");
        return this;
    }

    // ------ Optional ------------------------------------------------------------------

    /**
     * The continuousUnBoundingSettings to discovery the next available batch splits. Note: If the
     * value was set, the {@link #jdbcParameterValuesProvider} must specified with the {@link
     * org.apache.flink.connector.jdbc.split.JdbcSlideTimingParameterProvider}.
     */
    public JdbcSourceBuilder<OUT> setContinuousUnBoundingSettings(
            ContinuousUnBoundingSettings continuousUnBoundingSettings) {
        this.continuousUnBoundingSettings = continuousUnBoundingSettings;
        return this;
    }

    /**
     * If the value was set as an instance of {@link JdbcSlideTimingParameterProvider}, it's
     * required to specify the {@link #continuousUnBoundingSettings}.
     */
    public JdbcSourceBuilder<OUT> setJdbcParameterValuesProvider(
            @Nonnull JdbcParameterValuesProvider parameterValuesProvider) {
        this.jdbcParameterValuesProvider = Preconditions.checkNotNull(parameterValuesProvider);
        return this;
    }

    public JdbcSourceBuilder<OUT> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = Preconditions.checkNotNull(deliveryGuarantee);
        return this;
    }

    public JdbcSourceBuilder<OUT> setConnectionCheckTimeoutSeconds(
            int connectionCheckTimeoutSeconds) {
        connOptionsBuilder.withConnectionCheckTimeoutSeconds(connectionCheckTimeoutSeconds);
        return this;
    }

    public JdbcSourceBuilder<OUT> setConnectionProperty(String propKey, String propVal) {
        Preconditions.checkNotNull(propKey, "Connection property key mustn't be null");
        Preconditions.checkNotNull(propVal, "Connection property value mustn't be null");
        connOptionsBuilder.withProperty(propKey, propVal);
        return this;
    }

    public JdbcSourceBuilder<OUT> setSplitReaderFetchBatchSize(int splitReaderFetchBatchSize) {
        Preconditions.checkArgument(
                splitReaderFetchBatchSize > 0,
                "'splitReaderFetchBatchSize' must be in range (0, %s]",
                Integer.MAX_VALUE);
        this.splitReaderFetchBatchSize = splitReaderFetchBatchSize;
        return this;
    }

    public JdbcSourceBuilder<OUT> setResultSetType(int resultSetType) {
        this.resultSetType = resultSetType;
        return this;
    }

    public JdbcSourceBuilder<OUT> setResultSetConcurrency(int resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
        return this;
    }

    public JdbcSourceBuilder<OUT> setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    public JdbcSourceBuilder<OUT> setResultSetFetchSize(int resultSetFetchSize) {
        Preconditions.checkArgument(
                resultSetFetchSize == Integer.MIN_VALUE || resultSetFetchSize > 0,
                "Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.",
                resultSetFetchSize);
        this.resultSetFetchSize = resultSetFetchSize;
        return this;
    }

    public JdbcSourceBuilder<OUT> setConnectionProvider(
            @Nonnull JdbcConnectionProvider connectionProvider) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        return this;
    }

    public JdbcSourceBuilder<OUT> setOptionalSqlSplitEnumeratorState(
            Serializable optionalSqlSplitEnumeratorState) {
        this.optionalSqlSplitEnumeratorState = optionalSqlSplitEnumeratorState;
        return this;
    }

    public JdbcSource<OUT> build() {
        this.connectionProvider = new SimpleJdbcConnectionProvider(connOptionsBuilder.build());
        if (resultSetFetchSize > 0) {
            this.configuration.set(JdbcSourceOptions.RESULTSET_FETCH_SIZE, resultSetFetchSize);
        }

        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            Preconditions.checkArgument(
                    this.resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE
                            || this.resultSetType == ResultSet.CONCUR_READ_ONLY,
                    "The 'resultSetType' must be ResultSet.TYPE_SCROLL_INSENSITIVE or ResultSet.CONCUR_READ_ONLY when using %s",
                    DeliveryGuarantee.EXACTLY_ONCE);
        }

        this.configuration.set(JdbcSourceOptions.RESULTSET_CONCURRENCY, resultSetConcurrency);
        this.configuration.set(JdbcSourceOptions.RESULTSET_TYPE, resultSetType);
        this.configuration.set(
                JdbcSourceOptions.READER_FETCH_BATCH_SIZE, splitReaderFetchBatchSize);
        this.configuration.set(JdbcSourceOptions.AUTO_COMMIT, autoCommit);

        Preconditions.checkState(
                !StringUtils.isNullOrWhitespaceOnly(sql), "'sql' mustn't be null or empty.");
        Preconditions.checkNotNull(resultExtractor, "'resultExtractor' mustn't be null.");
        Preconditions.checkNotNull(typeInformation, "'typeInformation' mustn't be null.");

        if (Objects.nonNull(continuousUnBoundingSettings)) {
            Preconditions.checkArgument(
                    Objects.nonNull(jdbcParameterValuesProvider)
                            && jdbcParameterValuesProvider
                                    instanceof JdbcSlideTimingParameterProvider,
                    INVALID_SLIDE_TIMING_CONTINUOUS_HINT);
        }

        if (Objects.nonNull(jdbcParameterValuesProvider)
                && jdbcParameterValuesProvider instanceof JdbcSlideTimingParameterProvider) {
            Preconditions.checkArgument(
                    Objects.nonNull(continuousUnBoundingSettings),
                    INVALID_CONTINUOUS_SLIDE_TIMING_HINT);
        }

        return new JdbcSource<>(
                configuration,
                connectionProvider,
                new SqlTemplateSplitEnumerator.TemplateSqlSplitEnumeratorProvider()
                        .setOptionalSqlSplitEnumeratorState(optionalSqlSplitEnumeratorState)
                        .setSqlTemplate(sql)
                        .setParameterValuesProvider(jdbcParameterValuesProvider),
                resultExtractor,
                typeInformation,
                deliveryGuarantee,
                continuousUnBoundingSettings);
    }
}
