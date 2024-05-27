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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link JdbcSourceSplitSerializer}. */
class JdbcSourceSplitSerializerTest {

    private final JdbcSourceSplit split = new JdbcSourceSplit("1", "select 1", null, 0, null);
    private final JdbcSourceSplit mockedSplit = new MockedJdbcSourceSplit(split);
    private final JdbcSourceSplitSerializer serializer = new JdbcSourceSplitSerializer();
    private final JdbcSourceSplitSerializer mockedSerializer =
            new JdbcSourceSplitSerializer() {
                @Override
                public int getVersion() {
                    return new Random().nextInt(10) + 1;
                }
            };

    @Test
    void testSerialize() throws IOException {
        // Test for un-matched instance of splits.
        assertThatThrownBy(() -> serializer.serialize(mockedSplit))
                .isInstanceOf(IllegalArgumentException.class);

        // Test for matched version.
        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(split)))
                .isEqualTo(split);
    }

    @Test
    void testDeserialize() throws IOException {
        // Test for un-matched version.
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        mockedSerializer.getVersion(), serializer.serialize(split)))
                .isInstanceOf(IOException.class);

        // Test for matched version.
        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(split)))
                .isEqualTo(split);
    }

    static class MockedJdbcSourceSplit extends JdbcSourceSplit {

        public MockedJdbcSourceSplit(JdbcSourceSplit jdbcSourceSplit) {
            super(
                    jdbcSourceSplit.splitId(),
                    jdbcSourceSplit.getSqlTemplate(),
                    (Serializable[]) jdbcSourceSplit.getParameters(),
                    jdbcSourceSplit.getOffset(),
                    jdbcSourceSplit.getCheckpointedOffset());
        }
    }
}
