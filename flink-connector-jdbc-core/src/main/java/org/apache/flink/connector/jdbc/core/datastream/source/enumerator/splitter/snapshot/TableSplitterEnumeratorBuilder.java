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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain.TableId;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

/** Builder for creating instances of {@link TableSplitterEnumerator}. */
@PublicEvolving
public class TableSplitterEnumeratorBuilder {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private int chunkSize;
    private final Set<String> columnNames;

    protected TableSplitterEnumeratorBuilder() {
        this.columnNames = new LinkedHashSet<>();
        this.chunkSize = 100_000;
    }

    public TableSplitterEnumeratorBuilder withCatalogName(String catalogName) {
        this.catalogName = catalogName;
        return this;
    }

    public TableSplitterEnumeratorBuilder withSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public TableSplitterEnumeratorBuilder withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public TableSplitterEnumeratorBuilder withColumnNames(String... columnName) {
        return withColumnNames(new LinkedHashSet<>(Arrays.asList(columnName)));
    }

    public TableSplitterEnumeratorBuilder withColumnNames(Set<String> columnNames) {
        this.columnNames.addAll(columnNames);
        return this;
    }

    public TableSplitterEnumeratorBuilder withChunkSize(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException(
                    "chunkSize must be greater than 0 (rows per split), but was: " + chunkSize);
        }
        this.chunkSize = chunkSize;
        return this;
    }

    public TableSplitterEnumerator build() {
        return new TableSplitterEnumerator(
                new TableId(this.catalogName, this.schemaName, this.tableName),
                this.columnNames,
                this.chunkSize);
    }
}
