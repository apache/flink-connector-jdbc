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

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import java.sql.Connection;
import java.sql.SQLException;

/** Book store for testing. * */
public interface BooksStore {

    BooksTable BOOKS_TABLE = new BooksTable("books");
    BooksTable NEWBOOKS_TABLE = new BooksTable("newbooks");

    BooksTable.BookEntry[] TEST_DATA = {
        new BooksTable.BookEntry(1001, ("Java public for dummies"), ("Tan Ah Teck"), 11.11, 11),
        new BooksTable.BookEntry(1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22),
        new BooksTable.BookEntry(1003, ("More Java for more dummies"), ("Mohammad Ali"), 33.33, 33),
        new BooksTable.BookEntry(1004, ("A Cup of Java"), ("Kumar"), 44.44, 44),
        new BooksTable.BookEntry(1005, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55),
        new BooksTable.BookEntry(1006, ("A Teaspoon of Java 1.4"), ("Kevin Jones"), 66.66, 66),
        new BooksTable.BookEntry(1007, ("A Teaspoon of Java 1.5"), ("Kevin Jones"), 77.77, 77),
        new BooksTable.BookEntry(1008, ("A Teaspoon of Java 1.6"), ("Kevin Jones"), 88.88, 88),
        new BooksTable.BookEntry(1009, ("A Teaspoon of Java 1.7"), ("Kevin Jones"), 99.99, 99),
        new BooksTable.BookEntry(1010, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), null, 1010)
    };

    default void fillBooksWithTestData(DatabaseMetadata dbMetadata) throws SQLException {
        try (Connection conn = dbMetadata.getConnection()) {
            BOOKS_TABLE.insertTableTestData(conn);
        }
    }
}
