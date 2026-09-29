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

import java.io.Serializable;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PreparedSplitterNumericParametersTest {

    @Test
    void testBatchSizeDivisible() {
        Serializable[][] parameters =
                new PreparedSplitterNumericParameters(-5, 9).withBatchSize(3).getParameterValues();

        long[][] expected = {
            new long[] {-5, -3},
            new long[] {-2, 0},
            new long[] {1, 3},
            new long[] {4, 6},
            new long[] {7, 9}
        };
        check(expected, parameters);
    }

    @Test
    void testBatchSizeNotDivisible() {
        Serializable[][] parameters =
                new PreparedSplitterNumericParameters(-5, 11).withBatchSize(4).getParameterValues();

        long[][] expected = {
            new long[] {-5, -2},
            new long[] {-1, 2},
            new long[] {3, 5},
            new long[] {6, 8},
            new long[] {9, 11}
        };
        check(expected, parameters);
    }

    @Test
    void testBatchSizeTooLarge() {
        Serializable[][] parameters =
                new PreparedSplitterNumericParameters(0, 2).withBatchSize(5).getParameterValues();

        long[][] expected = {new long[] {0, 2}};
        check(expected, parameters);
    }

    @Test
    void testBatchNumDivisible() {
        Serializable[][] parameters =
                new PreparedSplitterNumericParameters(-5, 9).withBatchNum(5).getParameterValues();

        long[][] expected = {
            new long[] {-5, -3},
            new long[] {-2, 0},
            new long[] {1, 3},
            new long[] {4, 6},
            new long[] {7, 9}
        };
        check(expected, parameters);
    }

    @Test
    void testBatchNumNotDivisible() {
        Serializable[][] parameters =
                new PreparedSplitterNumericParameters(-5, 11).withBatchNum(5).getParameterValues();

        long[][] expected = {
            new long[] {-5, -2},
            new long[] {-1, 2},
            new long[] {3, 5},
            new long[] {6, 8},
            new long[] {9, 11}
        };
        check(expected, parameters);
    }

    @Test
    void testBatchNumTooLarge() {
        Serializable[][] parameters =
                new PreparedSplitterNumericParameters(0, 2).withBatchNum(5).getParameterValues();

        long[][] expected = {
            new long[] {0, 0},
            new long[] {1, 1},
            new long[] {2, 2}
        };
        check(expected, parameters);
    }

    @Test
    void testBatchMaxMinTooLarge() {
        Serializable[][] parameters =
                new PreparedSplitterNumericParameters(2260418954055131340L, 3875220057236942850L)
                        .withBatchNum(3)
                        .getParameterValues();

        long[][] expected = {
            new long[] {2260418954055131340L, 2798685988449068510L},
            new long[] {2798685988449068511L, 3336953022843005680L},
            new long[] {3336953022843005681L, 3875220057236942850L}
        };
        check(expected, parameters);
    }

    @Test
    void testBatchSizeNeverReadsPastMaxVal() {
        // 5 values in batches of 4: two splits, and the second must stop at 9, not at 10
        Serializable[][] parameters =
                new PreparedSplitterNumericParameters(5, 9).withBatchSize(4).getParameterValues();

        long[][] expected = {new long[] {5, 7}, new long[] {8, 9}};
        check(expected, parameters);
    }

    @Test
    void testSplitsCoverTheRangeExactly() {
        // every batch size and batch number for every range up to 40 values: contiguous splits
        // from minVal to maxVal
        for (long minVal = -3; minVal <= 3; minVal += 3) {
            for (int count = 1; count <= 40; count++) {
                long maxVal = minVal + count - 1;
                for (int k = 1; k <= 45; k++) {
                    assertCoversRange(
                            minVal,
                            maxVal,
                            new PreparedSplitterNumericParameters(minVal, maxVal)
                                    .withBatchSize(k)
                                    .getParameterValues());
                    assertCoversRange(
                            minVal,
                            maxVal,
                            new PreparedSplitterNumericParameters(minVal, maxVal)
                                    .withBatchNum(k)
                                    .getParameterValues());
                }
            }
        }
    }

    @Test
    void testMinValMustNotExceedMaxVal() {
        assertThatThrownBy(() -> new PreparedSplitterNumericParameters(10, 9))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("minVal must not be larger than maxVal");
    }

    private static void assertCoversRange(long minVal, long maxVal, Serializable[][] splits) {
        long next = minVal;
        for (Serializable[] split : splits) {
            long start = (Long) split[0];
            long end = (Long) split[1];
            assertThat(start)
                    .as("start of %s for [%s, %s]", Arrays.toString(split), minVal, maxVal)
                    .isEqualTo(next);
            assertThat(end)
                    .as("end of %s for [%s, %s]", Arrays.toString(split), minVal, maxVal)
                    .isGreaterThanOrEqualTo(start);
            next = end + 1;
        }
        assertThat(next - 1).as("last end for [%s, %s]", minVal, maxVal).isEqualTo(maxVal);
    }

    private void check(long[][] expected, Serializable[][] actual) {
        assertThat(actual).hasDimensions(expected.length, expected[0].length);
        for (int i = 0; i < expected.length; i++) {
            for (int j = 0; j < expected[i].length; j++) {
                assertThat(((Long) actual[i][j]).longValue()).isEqualTo(expected[i][j]);
            }
        }
    }
}
