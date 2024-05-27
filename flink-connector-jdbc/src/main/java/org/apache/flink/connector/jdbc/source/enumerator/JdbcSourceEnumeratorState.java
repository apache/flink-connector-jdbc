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

package org.apache.flink.connector.jdbc.source.enumerator;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.source.split.JdbcSourceSplit;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** Enumerator state class for {@link JdbcSourceEnumerator}. */
@PublicEvolving
public class JdbcSourceEnumeratorState implements Serializable {

    private final @Nonnull List<JdbcSourceSplit> completedSplits;
    private final @Nonnull List<JdbcSourceSplit> pendingSplits;
    private final @Nonnull List<JdbcSourceSplit> remainingSplits;

    private @Nullable final Serializable optionalUserDefinedSplitEnumeratorState;

    public JdbcSourceEnumeratorState(
            @Nonnull List<JdbcSourceSplit> completedSplits,
            @Nonnull List<JdbcSourceSplit> pendingSplits,
            @Nonnull List<JdbcSourceSplit> remainingSplits,
            @Nullable Serializable optionalUserDefinedSplitEnumeratorState) {
        this.completedSplits = Preconditions.checkNotNull(completedSplits);
        this.pendingSplits = Preconditions.checkNotNull(pendingSplits);
        this.remainingSplits = Preconditions.checkNotNull(remainingSplits);
        this.optionalUserDefinedSplitEnumeratorState = optionalUserDefinedSplitEnumeratorState;
    }

    public @Nullable Serializable getOptionalUserDefinedSplitEnumeratorState() {
        return optionalUserDefinedSplitEnumeratorState;
    }

    public @Nonnull List<JdbcSourceSplit> getCompletedSplits() {
        return completedSplits;
    }

    public @Nonnull List<JdbcSourceSplit> getPendingSplits() {
        return pendingSplits;
    }

    public @Nonnull List<JdbcSourceSplit> getRemainingSplits() {
        return remainingSplits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcSourceEnumeratorState that = (JdbcSourceEnumeratorState) o;
        return Objects.equals(completedSplits, that.completedSplits)
                && Objects.equals(pendingSplits, that.pendingSplits)
                && Objects.equals(remainingSplits, that.remainingSplits)
                && Objects.equals(
                        optionalUserDefinedSplitEnumeratorState,
                        that.optionalUserDefinedSplitEnumeratorState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                completedSplits,
                pendingSplits,
                remainingSplits,
                optionalUserDefinedSplitEnumeratorState);
    }
}
