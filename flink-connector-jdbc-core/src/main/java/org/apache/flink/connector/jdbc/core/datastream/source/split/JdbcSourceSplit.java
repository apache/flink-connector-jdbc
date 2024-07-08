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

package org.apache.flink.connector.jdbc.core.datastream.source.split;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

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

    private final @Nullable CheckpointedOffset checkpointedOffset;

    public JdbcSourceSplit(
            String id,
            String sqlTemplate,
            @Nullable Serializable[] parameters,
            @Nullable CheckpointedOffset checkpointedOffset) {
        this.id = id;
        this.sqlTemplate = sqlTemplate;
        this.parameters = parameters;
        this.checkpointedOffset = checkpointedOffset;
    }

    @Nullable
    public CheckpointedOffset getCheckpointedOffset() {
        return checkpointedOffset;
    }

    public JdbcSourceSplit updateWithCheckpointedPosition(
            @Nullable CheckpointedOffset checkpointedOffset) {
        return new JdbcSourceSplit(id, sqlTemplate, parameters, checkpointedOffset);
    }

    public Optional<CheckpointedOffset> getReaderPositionOptional() {
        return Optional.ofNullable(checkpointedOffset);
    }

    public int getReaderPosition() {
        if (Objects.nonNull(checkpointedOffset)) {
            Preconditions.checkState(checkpointedOffset.getOffset() <= Integer.MAX_VALUE);
            return (int) checkpointedOffset.getOffset();
        }
        return 0;
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
        return Objects.equals(id, that.id)
                && Objects.equals(sqlTemplate, that.sqlTemplate)
                && Arrays.equals(parameters, that.parameters)
                && Objects.equals(checkpointedOffset, that.checkpointedOffset);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, sqlTemplate, checkpointedOffset);
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
                + ", checkpointedOffset="
                + checkpointedOffset
                + '}';
    }
}
