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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** A split enumerator based on sql-parameters grains. */
@PublicEvolving
public class SlideTimingSplitterEnumerator extends SqlSplitterEnumerator {
    public static final Logger LOG = LoggerFactory.getLogger(SlideTimingSplitterEnumerator.class);

    private final long slideStepMills;
    private final long slideSpanMills;
    private final long splitGenerateDelayMillis;

    private @Nonnull Long startMills;

    protected SlideTimingSplitterEnumerator(
            String sqlTemplate,
            Long startMills,
            long slideSpanMills,
            long slideStepMills,
            long splitGenerateDelayMillis) {
        super(sqlTemplate);
        this.startMills = Preconditions.checkNotNull(startMills);
        this.slideStepMills = slideStepMills;
        this.slideSpanMills = slideSpanMills;
        this.splitGenerateDelayMillis = splitGenerateDelayMillis;
    }

    @Override
    public void start(JdbcConnectionProvider connectionProvider) {}

    @Override
    public void close() {}

    @Override
    public boolean isAllSplitsFinished() {
        return false;
    }

    @VisibleForTesting
    public Serializable[][] getSqlParameters() {
        List<Serializable[]> tmpList = new ArrayList<>();
        while (nextSplitAvailable(startMills)) {
            Serializable[] params = new Serializable[] {startMills, startMills + slideSpanMills};
            tmpList.add(params);
            startMills += slideStepMills;
        }
        return tmpList.toArray(new Serializable[0][]);
    }

    @Override
    public @Nullable Serializable serializableState() {
        return this.startMills;
    }

    @Override
    public SlideTimingSplitterEnumerator restoreState(Serializable state) {
        Preconditions.checkArgument((Long) state > 0L);
        this.startMills = (Long) state;
        return this;
    }

    private boolean nextSplitAvailable(Long nextSpanStartMillis) {
        final long delayedNextSpanStartMillis = nextSpanStartMillis + splitGenerateDelayMillis;
        final long currentAvailableMillis = currentAvailableMillis();
        return currentAvailableMillis >= delayedNextSpanStartMillis
                && (currentAvailableMillis - delayedNextSpanStartMillis >= slideSpanMills);
    }

    private Long currentAvailableMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SlideTimingSplitterEnumerator)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SlideTimingSplitterEnumerator that = (SlideTimingSplitterEnumerator) o;
        return slideStepMills == that.slideStepMills
                && slideSpanMills == that.slideSpanMills
                && splitGenerateDelayMillis == that.splitGenerateDelayMillis
                && Objects.equals(startMills, that.startMills);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                slideStepMills,
                slideSpanMills,
                splitGenerateDelayMillis,
                startMills);
    }

    public static SlideTimingSplitterEnumeratorBuilder builder() {
        return new SlideTimingSplitterEnumeratorBuilder();
    }

    /** A slide split enumerator builder. */
    public static class SlideTimingSplitterEnumeratorBuilder {
        private String sqlTemplate;
        private Long startMills;
        private Long slideSpanMills;
        private Long slideStepMills;
        private Long splitGenerateDelayMillis;

        public SlideTimingSplitterEnumeratorBuilder setSqlTemplate(String sqlTemplate) {
            this.sqlTemplate = sqlTemplate;
            return this;
        }

        public SlideTimingSplitterEnumeratorBuilder setStartMills(Long startMills) {
            this.startMills = startMills;
            return this;
        }

        public SlideTimingSplitterEnumeratorBuilder setSlideSpanMills(Long slideSpanMills) {
            this.slideSpanMills = slideSpanMills;
            return this;
        }

        public SlideTimingSplitterEnumeratorBuilder setSlideStepMills(Long slideStepMills) {
            this.slideStepMills = slideStepMills;
            return this;
        }

        public SlideTimingSplitterEnumeratorBuilder setSplitGenerateDelayMillis(
                Long splitGenerateDelayMillis) {
            this.splitGenerateDelayMillis = splitGenerateDelayMillis;
            return this;
        }

        public SlideTimingSplitterEnumerator build() {
            Preconditions.checkArgument(startMills > 0L, "'startMillis' must be greater than 0. ");
            Preconditions.checkArgument(
                    slideSpanMills > 0 || slideStepMills > 0,
                    "parameters must satisfy slideSpanMills > 0 and slideStepMills > 0");
            Preconditions.checkArgument(
                    splitGenerateDelayMillis >= 0L,
                    "parameters must satisfy splitGenerateDelayMillis >= 0");
            return new SlideTimingSplitterEnumerator(
                    sqlTemplate,
                    startMills,
                    slideSpanMills,
                    slideStepMills,
                    splitGenerateDelayMillis);
        }
    }
}
