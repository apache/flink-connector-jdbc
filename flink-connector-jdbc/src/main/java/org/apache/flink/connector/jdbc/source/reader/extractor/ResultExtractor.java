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

package org.apache.flink.connector.jdbc.source.reader.extractor;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The Extractor to extract the data from {@link ResultSet}.
 *
 * @param <T> The target data type.
 */
@PublicEvolving
public interface ResultExtractor<T> extends Serializable {

    /**
     * Extract the data from the current point line of the result.
     *
     * @param resultSet Result set queried from a sql.
     * @return The data object filled by the current line of the resultSet.
     * @throws SQLException SQL exception.
     */
    T extract(ResultSet resultSet) throws SQLException;

    /**
     * The identifier of the extractor.
     *
     * @return identifier in {@link String} type.
     */
    default String identifier() {
        return this.getClass().getSimpleName();
    }

    static ResultExtractor<Row> ofRowResultExtractor() {
        return new RowResultExtractor();
    }
}
