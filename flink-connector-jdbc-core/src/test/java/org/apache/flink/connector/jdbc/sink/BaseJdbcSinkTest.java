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

package org.apache.flink.connector.jdbc.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.jdbc.databases.derby.DerbyTestBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke tests for the {@link org.apache.flink.connector.jdbc.sink.JdbcSink} and the underlying
 * classes.
 */
public abstract class BaseJdbcSinkTest implements DerbyTestBase {

    private static final BooksTable TEST_TABLE = new BooksTable("SinkTable");

    private static final List<BooksTable.BookEntry> BOOKS =
            Arrays.stream(TEST_DATA)
                    .map(
                            book ->
                                    new BooksTable.BookEntry(
                                            book.id, book.title, book.author, book.price, book.qty))
                    .collect(Collectors.toList());

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TEST_TABLE);
    }

    protected abstract <T> JdbcSink<T> finishSink(JdbcSinkBuilder<T> builder);

    @Test
    public void testInsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.setParallelism(1);

        assertResult(new ArrayList<>());

        env.fromCollection(BOOKS)
                .sinkTo(
                        finishSink(
                                new JdbcSinkBuilder<BooksTable.BookEntry>()
                                        .withQueryStatement(
                                                TEST_TABLE.getInsertIntoQuery(),
                                                TEST_TABLE.getStatementBuilder())));
        env.execute();

        assertResult(BOOKS);
    }

    @Test
    public void testInsertWithObjectReuse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.setParallelism(1);

        assertResult(new ArrayList<>());

        BookEntryReuse reused = new BookEntryReuse();
        env.fromCollection(BOOKS)
                .map(
                        book -> {
                            reused.setBook(book);
                            return reused;
                        })
                .sinkTo(
                        finishSink(
                                new JdbcSinkBuilder<BookEntryReuse>()
                                        .withQueryStatement(
                                                TEST_TABLE.getInsertIntoQuery(),
                                                (ps, t) ->
                                                        TEST_TABLE
                                                                .getStatementBuilder()
                                                                .accept(ps, t.getBook()))));
        env.execute();

        assertResult(BOOKS);
    }

    private void assertResult(List<BooksTable.BookEntry> expected) throws SQLException {
        assertThat(TEST_TABLE.selectAllTable(getMetadata().getConnection())).isEqualTo(expected);
    }

    /** */
    public static class BookEntryReuse implements Serializable {
        public BooksTable.BookEntry book;

        public void setBook(BooksTable.BookEntry book) {
            this.book = book;
        }

        public BooksTable.BookEntry getBook() {
            return book;
        }
    }
}
