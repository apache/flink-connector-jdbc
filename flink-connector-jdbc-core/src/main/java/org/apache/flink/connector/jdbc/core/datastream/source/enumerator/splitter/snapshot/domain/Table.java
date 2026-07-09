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
import java.util.Set;

/** Represents a table together with the set of partition names discovered for it, if any. */
@PublicEvolving
public class Table implements Serializable {

    private final TableId tableId;
    private final Set<String> partitions;

    public Table(TableId tableId, Set<String> partitions) {
        this.tableId = tableId;
        this.partitions = partitions;
    }

    public TableId tableId() {
        return tableId;
    }

    public Set<String> partitions() {
        return partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Table)) {
            return false;
        }
        Table that = (Table) o;
        return Objects.equals(tableId, that.tableId) && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitions);
    }

    @Override
    public String toString() {
        return "Table{" + "tableId=" + tableId + ", partitions=" + partitions + '}';
    }
}
