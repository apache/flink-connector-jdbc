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

package org.apache.flink.connector.jdbc.source.split;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** JdbcSourceSplit class. */
@PublicEvolving
public class JdbcSourceSplit implements SourceSplit, Serializable {

    private final String id;

    private final String sqlTemplate;

    private final @Nullable Serializable[] parameters;

    private final int offset;

    private final @Nullable CheckpointedOffset checkpointedOffset;

    public JdbcSourceSplit(
            String id,
            String sqlTemplate,
            @Nullable Serializable[] parameters,
            int offset,
            @Nullable CheckpointedOffset checkpointedOffset) {
        this.id = id;
        this.sqlTemplate = sqlTemplate;
        this.parameters = parameters;
        this.offset = offset;
        this.checkpointedOffset = checkpointedOffset;
    }

    public int getOffset() {
        return offset;
    }

    @Nullable
    public CheckpointedOffset getCheckpointedOffset() {
        return checkpointedOffset;
    }

    public JdbcSourceSplit updateWithCheckpointedPosition(
            @Nullable CheckpointedOffset checkpointedOffset) {
        return new JdbcSourceSplit(id, sqlTemplate, parameters, offset, checkpointedOffset);
    }

    public Optional<CheckpointedOffset> getReaderPosition() {
        return Optional.ofNullable(checkpointedOffset);
    }

    public String getSqlTemplate() {
        return sqlTemplate;
    }

    @Nullable
    public Object[] getParameters() {
        return parameters;
    }

    @Override
    public String splitId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcSourceSplit that = (JdbcSourceSplit) o;
        return offset == that.offset
                && Objects.equals(id, that.id)
                && Objects.equals(sqlTemplate, that.sqlTemplate)
                && Arrays.equals(parameters, that.parameters)
                && Objects.equals(checkpointedOffset, that.checkpointedOffset);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, sqlTemplate, offset, checkpointedOffset);
        result = 31 * result + Arrays.hashCode(parameters);
        return result;
    }

    @Override
    public String toString() {
        return "JdbcSourceSplit{"
                + "id='"
                + id
                + '\''
                + ", sqlTemplate='"
                + sqlTemplate
                + '\''
                + ", parameters="
                + Arrays.toString(parameters)
                + ", offset="
                + offset
                + ", checkpointedOffset="
                + checkpointedOffset
                + '}';
    }
}
