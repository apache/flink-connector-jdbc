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

package org.apache.flink.connector.jdbc.templates;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.table.api.DataTypes;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Words table template. * */
public class WordsTable extends TableBuilder implements TableManaged {

    JdbcStatementBuilder<WordEntry> statementBuilder =
            (ps, word) -> {
                ps.setInt(1, word.id);
                ps.setString(2, word.word);
            };

    JdbcResultSetBuilder<WordEntry> resultSetBuilder =
            (rs) -> {
                List<WordEntry> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(
                            new WordEntry(
                                    getNullable(rs, r -> r.getInt(1)),
                                    getNullable(rs, r -> r.getString(2))));
                }
                return result;
            };

    public WordsTable(String name) {
        super(
                name,
                TableBuilder.field("id", DataTypes.INT().notNull(), true),
                TableBuilder.field("word", DataTypes.VARCHAR(50)));
    }

    public void createTable(Connection conn) throws SQLException {
        executeUpdate(conn, getCreateQuery());
    }

    public List<WordEntry> selectAllTable(Connection conn) throws SQLException {
        return executeStatement(conn, getSelectAllQuery(), resultSetBuilder);
    }

    public void deleteTable(Connection conn) throws SQLException {
        executeUpdate(conn, getDeleteFromQuery());
    }

    public void dropTable(Connection conn) throws SQLException {
        executeUpdate(conn, getDropTableQuery());
    }

    /** Word table entry. * */
    public static class WordEntry implements Serializable {
        public final Integer id;
        public final String word;

        public WordEntry(Integer id, String word) {
            this.id = id;
            this.word = word;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this).append("id", id).append("word", word).toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            WordEntry wordEntry = (WordEntry) o;
            return new EqualsBuilder()
                    .append(id, wordEntry.id)
                    .append(word, wordEntry.word)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(id).append(word).toHashCode();
        }
    }
}
