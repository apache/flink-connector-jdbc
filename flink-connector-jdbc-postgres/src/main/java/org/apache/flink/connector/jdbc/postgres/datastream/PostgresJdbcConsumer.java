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

package org.apache.flink.connector.jdbc.postgres.datastream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.core.datastream.source.JdbcSource;
import org.apache.flink.connector.jdbc.core.datastream.source.JdbcSourceBuilder;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.SplitterEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.postgres.datastream.connection.PostgresConnectionOptions;
import org.apache.flink.connector.jdbc.postgres.datastream.connection.PostgresConnectionProvider;

/** Facade to create Postgres JDBC stream sources. */
@PublicEvolving
public class PostgresJdbcConsumer {

    private final PostgresConnectionProvider connectionProvider;

    private SplitterEnumerator splitterEnumerator;
    private Integer resultFetchSize;
    private Boolean autoCommit;

    private PostgresJdbcConsumer(PostgresConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public static PostgresJdbcConsumer of(PostgresConnectionOptions connectionOptions) {
        return new PostgresJdbcConsumer(new PostgresConnectionProvider(connectionOptions.build()));
    }

    public PostgresJdbcConsumer withSplitterEnumerator(SplitterEnumerator splitterEnumerator) {
        this.splitterEnumerator = splitterEnumerator;
        return this;
    }

    public PostgresJdbcConsumer withResultFetchSize(int fetchSize) {
        this.resultFetchSize = fetchSize;
        return this;
    }

    public PostgresJdbcConsumer withAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    public <OUT> JdbcSource<OUT> build(JdbcExtractor<OUT> jdbcExtractor) {
        JdbcSourceBuilder<OUT> builder =
                JdbcSource.<OUT>builder()
                        .setConnectionProvider(connectionProvider)
                        .setSplitter(splitterEnumerator)
                        .setResultExtractor(jdbcExtractor)
                        .setTypeInformation(jdbcExtractor.typeInformation());

        if (resultFetchSize != null && resultFetchSize > 0) {
            builder.setResultSetFetchSize(resultFetchSize);
        }

        if (autoCommit != null) {
            builder.setAutoCommit(autoCommit);
        }

        return builder.build();
    }

    /**
     * A {@link ResultExtractor} that also knows the {@link TypeInformation} of what it extracts.
     */
    public interface JdbcExtractor<OUT> extends ResultExtractor<OUT> {
        TypeInformation<OUT> typeInformation();
    }
}
