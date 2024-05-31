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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.source.enumerator.SqlTemplateSplitEnumerator;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.jdbc.source.JdbcSourceOptions.READER_FETCH_BATCH_SIZE;
import static org.apache.flink.connector.jdbc.source.JdbcSourceOptions.RESULTSET_FETCH_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link JdbcSourceBuilder}. */
class JdbcSourceBuilderTest {

    private final String emptySql = "";
    private final String validSql = "select 1";
    private final String username = "username";
    private final String password = "password";
    private final String driverName = "driver";
    private final String dbUrl = "dbUrl";
    private final ResultExtractor<Row> extractor = ResultExtractor.ofRowResultExtractor();
    private final JdbcParameterValuesProvider parameterValuesProvider =
            new JdbcNumericBetweenParametersProvider(0, 3);

    private final TypeInformation<Row> typeInformation = new TypeHint<Row>() {}.getTypeInfo();

    private final JdbcSourceBuilder<Row> sourceBuilder =
            JdbcSource.<Row>builder()
                    .setSql(validSql)
                    .setResultExtractor(extractor)
                    .setDBUrl(dbUrl)
                    .setDriverName(driverName)
                    .setPassword(password)
                    .setUsername(username)
                    .setTypeInformation(typeInformation);

    @Test
    void testSetSql() {
        // For invalid.
        assertThatThrownBy(() -> JdbcSource.builder().setSql(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JdbcSource.builder().setSql(emptySql))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JdbcSource.builder().setDBUrl(dbUrl).build())
                .isInstanceOf(IllegalStateException.class);
        // For valid.
        JdbcSource<Row> jdbcSource = sourceBuilder.build();
        SqlTemplateSplitEnumerator sqlEnumerator =
                (SqlTemplateSplitEnumerator) jdbcSource.getSqlSplitEnumeratorProvider().create();
        assertThat(sqlEnumerator.getSqlTemplate()).isEqualTo(validSql);
    }

    @Test
    void testSetParameterProvider() {
        assertThatThrownBy(() -> JdbcSource.builder().setJdbcParameterValuesProvider(null))
                .isInstanceOf(NullPointerException.class);
        JdbcSource<Row> jdbcSource =
                sourceBuilder.setJdbcParameterValuesProvider(parameterValuesProvider).build();
        SqlTemplateSplitEnumerator sqlSplitEnumerator =
                (SqlTemplateSplitEnumerator) jdbcSource.getSqlSplitEnumeratorProvider().create();
        assertThat(sqlSplitEnumerator.getParameterValuesProvider())
                .isEqualTo(parameterValuesProvider);
    }

    @Test
    void testSetResultExtractor() {
        assertThatThrownBy(() -> JdbcSource.builder().setResultExtractor(null))
                .isInstanceOf(NullPointerException.class);
        JdbcSource<Row> jdbcSource = sourceBuilder.build();
        assertThat(jdbcSource.getResultExtractor()).isSameAs(extractor);
    }

    @Test
    void testSetSplitReaderFetchBatchSize() {
        assertThatThrownBy(() -> JdbcSource.builder().setSplitReaderFetchBatchSize(-1))
                .isInstanceOf(IllegalArgumentException.class);

        JdbcSource<Row> jdbcSource = sourceBuilder.setSplitReaderFetchBatchSize(10).build();
        assertThat(jdbcSource.getConfiguration().get(READER_FETCH_BATCH_SIZE)).isEqualTo(10);
    }

    @Test
    void testSetResultSetFetchSize() {
        assertThatThrownBy(() -> JdbcSource.builder().setResultSetFetchSize(-1))
                .isInstanceOf(IllegalArgumentException.class);

        JdbcSource<Row> jdbcSource = sourceBuilder.setResultSetFetchSize(10).build();
        assertThat(jdbcSource.getConfiguration().get(RESULTSET_FETCH_SIZE)).isEqualTo(10);
    }

    @Test
    void testSetConnectionInfo() {
        assertThatThrownBy(() -> JdbcSource.builder().setDriverName(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JdbcSource.builder().setUsername(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JdbcSource.builder().setDBUrl(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetTypeInformation() {
        assertThatThrownBy(() -> JdbcSource.builder().setTypeInformation(null))
                .isInstanceOf(NullPointerException.class);

        JdbcSource<Row> jdbcSource = sourceBuilder.setTypeInformation(typeInformation).build();
        assertThat(jdbcSource.getTypeInformation()).isSameAs(typeInformation);

        assertThatThrownBy(
                        () ->
                                JdbcSource.<Row>builder()
                                        .setSql(validSql)
                                        .setResultExtractor(extractor)
                                        .setDBUrl(dbUrl)
                                        .setDriverName(driverName)
                                        .setPassword(password)
                                        .setUsername(username)
                                        .build())
                .isInstanceOf(NullPointerException.class);
    }
}
