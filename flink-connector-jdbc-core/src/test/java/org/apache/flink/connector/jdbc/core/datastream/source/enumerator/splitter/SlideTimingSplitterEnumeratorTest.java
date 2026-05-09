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
        SlideTimingSplitterEnumerator enumerator =
                SlideTimingSplitterEnumerator.builder()
                        .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                        .setStartMills(1000L)
                        .setSlideSpanMills(500L)
                        .setSlideStepMills(100L)
                        .setSplitGenerateDelayMillis(0L)
                        .build();

        assertThat(enumerator).isNotNull();
    }

    @Test
    void testBuilderWithInvalidStartMills() {
        assertThatThrownBy(
                        () ->
                                SlideTimingSplitterEnumerator.builder()
                                        .setSqlTemplate(
                                                "SELECT * FROM table WHERE time >= ? AND time < ?")
                                        .setStartMills(0L)
                                        .setSlideSpanMills(500L)
                                        .setSlideStepMills(100L)
                                        .setSplitGenerateDelayMillis(0L)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBuilderWithInvalidSlideSpanMillsAndStepMills() {
        assertThatThrownBy(
                        () ->
                                SlideTimingSplitterEnumerator.builder()
                                        .setSqlTemplate(
                                                "SELECT * FROM table WHERE time >= ? AND time < ?")
                                        .setStartMills(1000L)
                                        .setSlideSpanMills(0L)
                                        .setSlideStepMills(0L)
                                        .setSplitGenerateDelayMillis(0L)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBuilderWithNegativeSplitGenerateDelayMillis() {
        assertThatThrownBy(
                        () ->
                                SlideTimingSplitterEnumerator.builder()
                                        .setSqlTemplate(
                                                "SELECT * FROM table WHERE time >= ? AND time < ?")
                                        .setStartMills(1000L)
                                        .setSlideSpanMills(500L)
                                        .setSlideStepMills(100L)
                                        .setSplitGenerateDelayMillis(-1L)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEquals() {
        SlideTimingSplitterEnumerator enumerator1 =
                SlideTimingSplitterEnumerator.builder()
                        .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                        .setStartMills(1000L)
                        .setSlideSpanMills(500L)
                        .setSlideStepMills(100L)
                        .setSplitGenerateDelayMillis(0L)
                        .build();

        SlideTimingSplitterEnumerator enumerator2 =
                SlideTimingSplitterEnumerator.builder()
                        .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                        .setStartMills(1000L)
                        .setSlideSpanMills(500L)
                        .setSlideStepMills(100L)
                        .setSplitGenerateDelayMillis(0L)
                        .build();

        assertThat(enumerator1).isEqualTo(enumerator2);
    }

    @Test
    void testHashCode() {
        SlideTimingSplitterEnumerator enumerator1 =
                SlideTimingSplitterEnumerator.builder()
                        .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                        .setStartMills(1000L)
                        .setSlideSpanMills(500L)
                        .setSlideStepMills(100L)
                        .setSplitGenerateDelayMillis(0L)
                        .build();

        SlideTimingSplitterEnumerator enumerator2 =
                SlideTimingSplitterEnumerator.builder()
                        .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                        .setStartMills(1000L)
                        .setSlideSpanMills(500L)
                        .setSlideStepMills(100L)
                        .setSplitGenerateDelayMillis(0L)
                        .build();

        assertThat(enumerator1.hashCode()).isEqualTo(enumerator2.hashCode());
    }

    @Test
    void testStart() {
        SlideTimingSplitterEnumerator enumerator =
                SlideTimingSplitterEnumerator.builder()
                        .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                        .setStartMills(1000L)
                        .setSlideSpanMills(500L)
                        .setSlideStepMills(100L)
                        .setSplitGenerateDelayMillis(0L)
                        .build();

        assertThatCode(() -> enumerator.start(null)).doesNotThrowAnyException();
    }

    @Test
    void testClose() {
        SlideTimingSplitterEnumerator enumerator =
                SlideTimingSplitterEnumerator.builder()
                        .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                        .setStartMills(1000L)
                        .setSlideSpanMills(500L)
                        .setSlideStepMills(100L)
                        .setSplitGenerateDelayMillis(0L)
                        .build();

        assertThatCode(enumerator::close).doesNotThrowAnyException();
    }

    @Test
    void testIsAllSplitsFinished() {
        SlideTimingSplitterEnumerator enumerator =
                SlideTimingSplitterEnumerator.builder()
                        .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                        .setStartMills(1000L)
                        .setSlideSpanMills(500L)
                        .setSlideStepMills(100L)
                        .setSplitGenerateDelayMillis(0L)
                        .build();

        assertThat(enumerator.isAllSplitsFinished()).isFalse();
    }

    @Test
    void testRestoreState() {
        SlideTimingSplitterEnumerator enumerator =
                SlideTimingSplitterEnumerator.builder()
                        .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                        .setStartMills(1000L)
                        .setSlideSpanMills(500L)
                        .setSlideStepMills(100L)
                        .setSplitGenerateDelayMillis(0L)
                        .build();

        SlideTimingSplitterEnumerator restored = enumerator.restoreState(2000L);
        assertThat(restored).isNotNull();
    }

    @Test
    void testSerializableState() {
        SlideTimingSplitterEnumerator enumerator =
                SlideTimingSplitterEnumerator.builder()
                        .setSqlTemplate("SELECT * FROM table WHERE time >= ? AND time < ?")
                        .setStartMills(1000L)
                        .setSlideSpanMills(500L)
                        .setSlideStepMills(100L)
                        .setSplitGenerateDelayMillis(0L)
                        .build();

        assertThat(enumerator.serializableState()).isNotNull();
    }
}
