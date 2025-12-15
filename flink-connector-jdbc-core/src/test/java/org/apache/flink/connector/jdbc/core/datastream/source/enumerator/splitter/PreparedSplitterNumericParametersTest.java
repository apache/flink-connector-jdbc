package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter;

import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThat;

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
            new long[] {2260418954055131340L, 2798685988449068491L},
            new long[] {2798685988449068492L, 3336953022843005643L},
            new long[] {3336953022843005644L, 3875220057236942795L}
        };
        check(expected, parameters);
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
