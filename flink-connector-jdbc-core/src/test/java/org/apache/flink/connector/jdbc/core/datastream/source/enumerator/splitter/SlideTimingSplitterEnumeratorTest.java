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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SlideTimingSplitterEnumeratorTest {

    @Test
    void testBuilderWithValidParameters() {
        SlideTimingSplitterEnumerator enumerator = defaultEnumerator();

        assertThat(enumerator).isNotNull();
    }

    @Test
    void testBuilderWithInvalidStartMills() {
        assertThatThrownBy(() -> createEnumerator(0L, 500L, 100L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBuilderWithInvalidSlideSpanMillsAndStepMills() {
        assertThatThrownBy(() -> createEnumerator(1000L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBuilderWithNegativeSplitGenerateDelayMillis() {
        assertThatThrownBy(() -> createEnumerator(1000L, 500L, 100L, -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEquals() {
        SlideTimingSplitterEnumerator enumerator1 = defaultEnumerator();

        SlideTimingSplitterEnumerator enumerator2 = defaultEnumerator();

        assertThat(enumerator1).isEqualTo(enumerator2);
    }

    @Test
    void testHashCode() {
        SlideTimingSplitterEnumerator enumerator1 = defaultEnumerator();

        SlideTimingSplitterEnumerator enumerator2 = defaultEnumerator();

        assertThat(enumerator1.hashCode()).isEqualTo(enumerator2.hashCode());
    }

    @Test
    void testStart() {
        SlideTimingSplitterEnumerator enumerator = defaultEnumerator();

        assertThatCode(() -> enumerator.start(null)).doesNotThrowAnyException();
    }

    @Test
    void testClose() {
        SlideTimingSplitterEnumerator enumerator = defaultEnumerator();

        assertThatCode(enumerator::close).doesNotThrowAnyException();
    }

    @Test
    void testIsAllSplitsFinished() {
        SlideTimingSplitterEnumerator enumerator = defaultEnumerator();

        assertThat(enumerator.isAllSplitsFinished()).isFalse();
    }

    @Test
    void testRestoreState() {
        SlideTimingSplitterEnumerator enumerator = defaultEnumerator();

        SlideTimingSplitterEnumerator restored = enumerator.restoreState(2000L);
        assertThat(restored).isNotNull();
    }

    @Test
    void testSerializableState() {
        SlideTimingSplitterEnumerator enumerator = defaultEnumerator();

        assertThat(enumerator.serializableState()).isNotNull();
    }

    SlideTimingSplitterEnumerator defaultEnumerator() {
        return createEnumerator(1000L, 500L, 100L);
    }

    SlideTimingSplitterEnumerator createEnumerator(
            Long startMillis, Long slideSpanMillis, Long slideStepMillis) {
        return createEnumerator(startMillis, slideSpanMillis, slideStepMillis, 0L);
    }

    SlideTimingSplitterEnumerator createEnumerator(
            Long startMillis, Long slideSpanMillis, Long slideStepMillis, Long splitDelayMillis) {
        return SlideTimingSplitterEnumerator.builder()
                .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                .setStartMillis(startMillis)
                .setSlideSpanMillis(slideSpanMillis)
                .setSlideStepMillis(slideStepMillis)
                .setSplitGenerateDelayMillis(splitDelayMillis)
                .build();
    }
}
