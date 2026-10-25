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

package org.apache.flink.connector.jdbc;

import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JdbcExactlyOnceOptions}. */
class JdbcExactlyOnceOptionsTest {

    @Test
    void testTransactionIdPrefix() {
        assertThat(JdbcExactlyOnceOptions.defaults().getTransactionIdPrefix()).isEmpty();
        assertThat(
                        JdbcExactlyOnceOptions.builder()
                                .withTransactionIdPrefix("orders")
                                .build()
                                .getTransactionIdPrefix())
                .hasValue("orders");
    }

    @Test
    void testTransactionIdPrefixMustNotBeEmpty() {
        assertThatThrownBy(() -> JdbcExactlyOnceOptions.builder().withTransactionIdPrefix(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** The prefix counts in bytes: a two-byte character takes two of them. */
    @Test
    void testTransactionIdPrefixTooLong() {
        char[] prefix = new char[TransactionId.MAX_PREFIX_BYTES / 2 + 1];
        Arrays.fill(prefix, 'é');

        assertThatThrownBy(
                        () ->
                                JdbcExactlyOnceOptions.builder()
                                        .withTransactionIdPrefix(new String(prefix)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 43 bytes");
    }
}
