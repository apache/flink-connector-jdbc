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

package org.apache.flink.connector.jdbc.split;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** The parameters provider generate parameters by slide timing window strategy. */
@PublicEvolving
public class JdbcSlideTimingParameterProvider implements JdbcParameterValuesProvider {

    private final long slideStepMills;
    private final long slideSpanMills;
    private final long splitGenerateDelayMillis;

    private @Nonnull Long startMills;

    public JdbcSlideTimingParameterProvider(
            Long startMills,
            long slideSpanMills,
            long slideStepMills,
            long splitGenerateDelayMillis) {
        this.startMills = Preconditions.checkNotNull(startMills);
        Preconditions.checkArgument(
                startMills > 0L,
                "'startMillis' of JdbcSlideTimingParameterProvider must be greater than 0. ");
        Preconditions.checkArgument(
                slideSpanMills > 0 || slideStepMills > 0,
                "JdbcSlideTimingParameterProvider parameters must satisfy "
                        + "slideSpanMills > 0 and slideStepMills > 0");
        Preconditions.checkArgument(
                splitGenerateDelayMillis >= 0L,
                "JdbcSlideTimingParameterProvider parameters must satisfy "
                        + "splitGenerateDelayMillis >= 0");
        this.slideStepMills = slideStepMills;
        this.slideSpanMills = slideSpanMills;
        this.splitGenerateDelayMillis = splitGenerateDelayMillis;
    }

    private boolean nextSplitAvailable(Long nextSpanStartMillis) {
        final long delayedNextSpanStartMillis = nextSpanStartMillis + splitGenerateDelayMillis;
        final long currentAvailableMillis = currentAvailableMillis();
        return currentAvailableMillis >= delayedNextSpanStartMillis
                && (currentAvailableMillis - delayedNextSpanStartMillis >= slideSpanMills);
    }

    @Override
    public Long getLatestOptionalState() {
        return startMills;
    }

    @Override
    public void setOptionalState(Serializable optionalState) {
        Preconditions.checkArgument((Long) optionalState > 0L);
        this.startMills = (Long) optionalState;
    }

    public Long currentAvailableMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public Serializable[][] getParameterValues() {
        List<Serializable[]> tmpList = new ArrayList<>();
        while (nextSplitAvailable(startMills)) {
            Serializable[] params = new Serializable[] {startMills, startMills + slideSpanMills};
            tmpList.add(params);
            startMills += slideStepMills;
        }
        return tmpList.toArray(new Serializable[0][]);
    }
}
