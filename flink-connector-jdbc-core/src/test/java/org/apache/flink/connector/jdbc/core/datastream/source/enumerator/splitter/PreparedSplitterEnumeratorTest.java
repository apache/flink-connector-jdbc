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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PreparedSplitterEnumerator}. */
class PreparedSplitterEnumeratorTest {

    private static final String QUERY = "select * from books where author = ?";
    private static final String QUERY_WITHOUT_PARAMETERS = "select * from books";

    @Test
    void testQueryWithoutParametersIsOneSplit() {
        // the first example in the JdbcSourceBuilder javadoc: a plain query, read once
        PreparedSplitterEnumerator enumerator =
                PreparedSplitterEnumerator.of("select * from books");

        List<JdbcSourceSplit> splits = enumerator.enumerateSplits();

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getSqlTemplate()).isEqualTo("select * from books");
        assertThat(splits.get(0).getParameters()).isEmpty();
        assertThat(enumerator.isAllSplitsFinished()).isTrue();
        assertThat(enumerator.enumerateSplits()).isEmpty();
    }

    @Test
    void testOneSplitPerParameterRow() {
        Serializable[][] parameters = {{"Kumar"}, {"Tan Ah Teck"}};

        List<JdbcSourceSplit> splits =
                PreparedSplitterEnumerator.of(QUERY, parameters).enumerateSplits();

        assertThat(splits).hasSize(2);
        assertThat(splits.get(0).getParameters()).containsExactly("Kumar");
        assertThat(splits.get(1).getParameters()).containsExactly("Tan Ah Teck");
        assertThat(splits.get(0).splitId()).isNotEqualTo(splits.get(1).splitId());
    }

    @Test
    void testBoundedAndLineage() {
        PreparedSplitterEnumerator enumerator =
                PreparedSplitterEnumerator.of(QUERY_WITHOUT_PARAMETERS);

        assertThat(enumerator.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(enumerator.lineageQueries()).containsExactly(QUERY_WITHOUT_PARAMETERS);
        assertThat(enumerator.serializableState()).isNull();
    }
}
