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

package org.apache.flink.connector.jdbc;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;
import org.apache.flink.connector.jdbc.databases.derby.DerbyDatabase;
import org.apache.flink.connector.jdbc.templates.BooksTable;
import org.apache.flink.connector.jdbc.templates.WordsTable;
import org.apache.flink.connector.jdbc.templates.round2.TableManaged;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Smoke tests for the {@link JdbcSink} and the underlying classes. */
class JdbcITCase implements JdbcTestBase {

    private static final BooksTable booksTable = new BooksTable("books");
    private static final WordsTable wordsTable = new WordsTable("words");

    @Override
    public DatabaseMetadata getDbMetadata() {
        return DerbyDatabase.getMetadata();
    }

    @Override
    public List<TableManaged> getManagedTables() {
        return Arrays.asList(booksTable, wordsTable);
    }

    @Test
    void testInsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.setParallelism(1);
        env.fromCollection(booksTable.getTestData())
                .addSink(
                        JdbcSink.sink(
                                booksTable.getInsertIntoQuery(),
                                booksTable.getStatementBuilder(),
                                getDbMetadata().toConnectionOptions()));
        env.execute();

        assertThat(selectAllBooks()).isEqualTo(booksTable.getTestData());
    }

    @Test
    void testObjectReuse() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(OBJECT_REUSE, true);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.setParallelism(1);

        AtomicInteger counter = new AtomicInteger(0);
        List<String> words = Arrays.asList("a", "and", "b", "were", "sitting in the buffer");
        StringHolder reused = new StringHolder();
        env.fromCollection(words)
                .map(reused::setContent)
                .addSink(
                        JdbcSink.sink(
                                wordsTable.getInsertIntoQuery(),
                                (ps, e) -> {
                                    ps.setInt(1, counter.getAndIncrement());
                                    ps.setString(2, e.content);
                                },
                                getDbMetadata().toConnectionOptions()));
        env.execute();

        assertThat(selectAllWords()).isEqualTo(words);
    }

    private List<BooksTable.BookEntry> selectAllBooks() throws Exception {
        try (Connection connection = getDbMetadata().getConnection()) {
            return booksTable.selectAllTable(connection);
        }
    }

    private List<String> selectAllWords() throws Exception {
        try (Connection connection = getDbMetadata().getConnection()) {
            return wordsTable.selectAllTable(connection).stream()
                    .map(word -> word.word)
                    .collect(Collectors.toList());
        }
    }

    private static class StringHolder implements Serializable {
        private static final long serialVersionUID = 1L;
        private String content;

        public StringHolder setContent(String content) {
            this.content = checkNotNull(content);
            return this;
        }
    }
}
