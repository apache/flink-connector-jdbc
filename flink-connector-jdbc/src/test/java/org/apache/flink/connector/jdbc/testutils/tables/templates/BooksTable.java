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

package org.apache.flink.connector.jdbc.testutils.tables.templates;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.functions.JdbcResultSetBuilder;
import org.apache.flink.connector.jdbc.testutils.tables.TableBase;
import org.apache.flink.connector.jdbc.testutils.tables.TableField;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.VARCHAR;

/** Book table template. * */
public class BooksTable extends TableBase<BooksTable.BookEntry> implements TableManaged {

    private final JdbcStatementBuilder<BookEntry> statementBuilder =
            (ps, book) -> {
                ps.setInt(1, book.id);
                ps.setString(2, book.title);
                ps.setString(3, book.author);
                if (book.price == null) {
                    ps.setNull(4, Types.DOUBLE);
                } else {
                    ps.setDouble(4, book.price);
                }
                ps.setInt(5, book.qty);
            };

    private final JdbcResultSetBuilder<BookEntry> resultSetBuilder =
            (rs) -> {
                List<BookEntry> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(
                            new BookEntry(
                                    getNullable(rs, r -> r.getInt(1)),
                                    getNullable(rs, r -> r.getString(2)),
                                    getNullable(rs, r -> r.getString(3)),
                                    getNullable(rs, r -> r.getDouble(4)),
                                    getNullable(rs, r -> r.getInt(5))));
                }
                return result;
            };

    public BooksTable(String name) {
        super(
                name,
                Arrays.asList(
                                pkField("id", INT().notNull()),
                                field("title", VARCHAR(50)),
                                field("author", VARCHAR(50)),
                                field("price", dbType("FLOAT"), DOUBLE()),
                                field("qty", INT()))
                        .toArray(new TableField[0]));
    }

    public String getSelectByIdBetweenQuery() {
        return format("%s WHERE id BETWEEN ? AND ?", getSelectAllQuery());
    }

    public String getSelectByAuthorQuery() {
        return format("%s WHERE author = ?", getSelectAllQuery());
    }

    public String getSelectAllNoQuantityQuery() {
        return format("%s WHERE QTY < 0", getSelectAllQuery());
    }

    //    public List<BookEntry> getTestData() {
    //        return Arrays.asList(BooksStore.TEST_DATA);
    //    }
    //    public void insertTableTestData(Connection conn) throws SQLException {
    //        executeStatement(conn, getInsertIntoQuery(), statementBuilder, getTestData());
    //    }

    public JdbcStatementBuilder<BookEntry> getStatementBuilder() {
        return statementBuilder;
    }

    @Override
    protected JdbcResultSetBuilder<BookEntry> getResultSetBuilder() {
        return resultSetBuilder;
    }

    public List<BookEntry> selectAllTable(Connection conn) throws SQLException {
        return executeStatement(conn, getSelectAllQuery(), resultSetBuilder);
    }

    /** Book table entry. * */
    public static class BookEntry implements Serializable {
        public final Integer id;
        public final String title;
        public final String author;
        public final Double price;
        public final Integer qty;

        public BookEntry(Integer id, String title, String author, Double price, Integer qty) {
            this.id = id;
            this.title = title;
            this.author = author;
            this.price = price;
            this.qty = qty;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("id", id)
                    .append("title", title)
                    .append("author", author)
                    .append("price", price)
                    .append("qty", qty)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BookEntry bookEntry = (BookEntry) o;
            return new EqualsBuilder()
                    .append(id, bookEntry.id)
                    .append(title, bookEntry.title)
                    .append(author, bookEntry.author)
                    .append(price, bookEntry.price)
                    .append(qty, bookEntry.qty)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(id)
                    .append(title)
                    .append(author)
                    .append(price)
                    .append(qty)
                    .toHashCode();
        }
    }
}
