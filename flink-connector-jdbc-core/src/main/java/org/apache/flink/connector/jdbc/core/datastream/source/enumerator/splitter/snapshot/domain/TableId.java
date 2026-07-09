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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Represents a unique identifier for a database table, including catalog name, schema name, and
 * table name.
 */
@PublicEvolving
public class TableId implements Serializable {

    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    public TableId(String catalogName, String schemaName, String tableName) {
        this.catalogName = checkNotNull(catalogName, "Catalog name cannot be null");
        this.schemaName = checkNotNull(schemaName, "Schema name cannot be null");
        this.tableName = checkNotNull(tableName, "Table name cannot be null");
    }

    public String catalogName() {
        return catalogName;
    }

    public String schemaName() {
        return schemaName;
    }

    public String tableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableId)) {
            return false;
        }
        TableId that = (TableId) o;
        return Objects.equals(catalogName, that.catalogName)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, schemaName, tableName);
    }

    @Override
    public String toString() {
        return String.format("%s.%s.%s", catalogName, schemaName, tableName);
    }

    public static TableIdBuilder builder() {
        return new TableIdBuilder();
    }

    /** Builder class for constructing {@link TableId} instances. */
    public static class TableIdBuilder {
        private String catalogName;
        private String schemaName;
        private String tableName;

        protected TableIdBuilder() {}

        public TableIdBuilder withCatalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        public TableIdBuilder withSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public TableIdBuilder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public TableId build() {
            return new TableId(this.catalogName, this.schemaName, this.tableName);
        }
    }
}
