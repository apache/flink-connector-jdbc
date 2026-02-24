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

package org.apache.flink.connector.jdbc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/** JDBC sink batch options. */
@PublicEvolving
public class JdbcExecutionOptions implements Serializable {
    public static final int DEFAULT_MAX_RETRY_TIMES = 3;
    private static final int DEFAULT_INTERVAL_MILLIS = 0;
    public static final int DEFAULT_SIZE = 5000;

    private final long batchIntervalMs;
    private final int batchSize;
    private final int maxRetries;
    private final boolean postgresUnnestEnabled;

    private JdbcExecutionOptions(
            long batchIntervalMs, int batchSize, int maxRetries, boolean postgresUnnestEnabled) {
        Preconditions.checkArgument(maxRetries >= 0);
        this.batchIntervalMs = batchIntervalMs;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.postgresUnnestEnabled = postgresUnnestEnabled;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Returns whether PostgreSQL UNNEST-based batch insert optimization is enabled.
     *
     * <p>When enabled, uses PostgreSQL's UNNEST() function for batch inserts, which can
     * significantly improve performance (5-10x) and reduce query plan explosion in
     * pg_stat_statements.
     *
     * @return true if UNNEST optimization is enabled, false otherwise
     */
    public boolean isPostgresUnnestEnabled() {
        return postgresUnnestEnabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcExecutionOptions that = (JdbcExecutionOptions) o;
        return batchIntervalMs == that.batchIntervalMs
                && batchSize == that.batchSize
                && maxRetries == that.maxRetries
                && postgresUnnestEnabled == that.postgresUnnestEnabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchIntervalMs, batchSize, maxRetries, postgresUnnestEnabled);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static JdbcExecutionOptions defaults() {
        return builder().build();
    }

    /** Builder for {@link JdbcExecutionOptions}. */
    @PublicEvolving
    public static final class Builder {
        private long intervalMs = DEFAULT_INTERVAL_MILLIS;
        private int size = DEFAULT_SIZE;
        private int maxRetries = DEFAULT_MAX_RETRY_TIMES;
        private boolean postgresUnnestEnabled = false;

        public Builder withBatchSize(int size) {
            this.size = size;
            return this;
        }

        public Builder withBatchIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
            return this;
        }

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Enable or disable PostgreSQL UNNEST-based batch insert optimization.
         *
         * <p>When enabled, uses PostgreSQL's UNNEST() function for batch inserts, which can
         * provide 5-10x performance improvement for high-throughput scenarios and significantly
         * reduce the number of query plans in pg_stat_statements.
         *
         * <p>This optimization:
         * <ul>
         *   <li>Uses a single INSERT with UNNEST instead of multiple INSERT statements</li>
         *   <li>Binds column values as SQL arrays using PreparedStatement.setArray()</li>
         *   <li>Ensures a single query plan regardless of batch size</li>
         *   <li>Is compatible with both INSERT and UPSERT modes</li>
         * </ul>
         *
         * <p>Default is false for backward compatibility.
         *
         * @param enabled true to enable UNNEST optimization, false otherwise
         * @return this builder
         */
        public Builder withPostgresUnnestEnabled(boolean enabled) {
            this.postgresUnnestEnabled = enabled;
            return this;
        }

        public JdbcExecutionOptions build() {
            return new JdbcExecutionOptions(intervalMs, size, maxRetries, postgresUnnestEnabled);
        }
    }
}
