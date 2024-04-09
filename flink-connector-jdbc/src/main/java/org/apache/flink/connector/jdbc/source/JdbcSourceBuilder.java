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

package org.apache.flink.connector.jdbc.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.source.enumerator.SqlTemplateSplitEnumerator;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.sql.DriverManager;
import java.sql.PreparedStatement;

import static org.apache.flink.connector.jdbc.source.JdbcSourceOptions.AUTO_COMMIT;
import static org.apache.flink.connector.jdbc.source.JdbcSourceOptions.READER_FETCH_BATCH_SIZE;
import static org.apache.flink.connector.jdbc.source.JdbcSourceOptions.RESULTSET_CONCURRENCY;
import static org.apache.flink.connector.jdbc.source.JdbcSourceOptions.RESULTSET_FETCH_SIZE;
import static org.apache.flink.connector.jdbc.source.JdbcSourceOptions.RESULTSET_TYPE;

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

    private final Configuration configuration;

    private int splitReaderFetchBatchSize;
    private int resultSetType;
    private int resultSetConcurrency;
    private int resultSetFetchSize;
    // Boolean to distinguish between default value and explicitly set autoCommit mode.
    private Boolean autoCommit;

    // TODO It would be used to introduce streaming semantic and tracked in
    //  https://issues.apache.org/jira/browse/FLINK-33461
    private DeliveryGuarantee deliveryGuarantee;

    private TypeInformation<OUT> typeInformation;

    private final JdbcConnectionOptions.JdbcConnectionOptionsBuilder connOptionsBuilder;
    private String sql;
    private JdbcParameterValuesProvider jdbcParameterValuesProvider;
    private ResultExtractor<OUT> resultExtractor;

    private JdbcConnectionProvider connectionProvider;

    JdbcSourceBuilder() {
        this.configuration = new Configuration();
        this.connOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        this.splitReaderFetchBatchSize = READER_FETCH_BATCH_SIZE.defaultValue();
        this.resultSetType = RESULTSET_TYPE.defaultValue();
        this.resultSetConcurrency = RESULTSET_CONCURRENCY.defaultValue();
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
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(username),
                "It's required to set the 'username'.");
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

    public JdbcSourceBuilder<OUT> setJdbcParameterValuesProvider(
            @Nonnull JdbcParameterValuesProvider parameterValuesProvider) {
        this.jdbcParameterValuesProvider = Preconditions.checkNotNull(parameterValuesProvider);
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

    public JdbcSource<OUT> build() {
        this.connectionProvider = new SimpleJdbcConnectionProvider(connOptionsBuilder.build());
        if (resultSetFetchSize > 0) {
            this.configuration.set(RESULTSET_FETCH_SIZE, resultSetFetchSize);
        }
        this.configuration.set(RESULTSET_CONCURRENCY, resultSetConcurrency);
        this.configuration.set(RESULTSET_TYPE, resultSetType);
        this.configuration.set(READER_FETCH_BATCH_SIZE, splitReaderFetchBatchSize);
        this.configuration.set(AUTO_COMMIT, autoCommit);

        Preconditions.checkState(
                !StringUtils.isNullOrWhitespaceOnly(sql), "'sql' mustn't be null or empty.");
        Preconditions.checkNotNull(resultExtractor, "'resultExtractor' mustn't be null.");
        Preconditions.checkNotNull(typeInformation, "'typeInformation' mustn't be null.");

        return new JdbcSource<>(
                configuration,
                connectionProvider,
                new SqlTemplateSplitEnumerator.TemplateSqlSplitEnumeratorProvider()
                        .setOptionalSqlSplitEnumeratorState(null)
                        .setSqlTemplate(sql)
                        .setParameterValuesProvider(jdbcParameterValuesProvider),
                resultExtractor,
                typeInformation,
                deliveryGuarantee);
    }
}
