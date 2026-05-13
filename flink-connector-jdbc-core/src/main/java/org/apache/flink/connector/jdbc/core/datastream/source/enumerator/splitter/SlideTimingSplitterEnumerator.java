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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** A split enumerator based on sql-parameters grains. */
@PublicEvolving
public class SlideTimingSplitterEnumerator extends SqlSplitterEnumerator {

    private final long slideStepMillis;
    private final long slideSpanMillis;
    private final long splitGenerateDelayMillis;

    private @Nonnull Long startMillis;

    protected SlideTimingSplitterEnumerator(
            String sqlTemplate,
            Long startMillis,
            long slideSpanMillis,
            long slideStepMillis,
            long splitGenerateDelayMillis) {
        super(sqlTemplate);
        this.startMillis = Preconditions.checkNotNull(startMillis);
        this.slideStepMillis = slideStepMillis;
        this.slideSpanMillis = slideSpanMillis;
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
        while (nextSplitAvailable(startMillis)) {
            Serializable[] params = new Serializable[] {startMillis, startMillis + slideSpanMillis};
            tmpList.add(params);
            startMillis += slideStepMillis;
        }
        return tmpList.toArray(new Serializable[0][]);
    }

    @Override
    public @Nullable Serializable serializableState() {
        return this.startMillis;
    }

    @Override
    public SlideTimingSplitterEnumerator restoreState(Serializable state) {
        Preconditions.checkArgument((Long) state > 0L);
        this.startMillis = (Long) state;
        return this;
    }

    private boolean nextSplitAvailable(Long nextSpanStartMillis) {
        final long delayedNextSpanStartMillis = nextSpanStartMillis + splitGenerateDelayMillis;
        final long currentAvailableMillis = currentAvailableMillis();
        return currentAvailableMillis >= delayedNextSpanStartMillis
                && (currentAvailableMillis - delayedNextSpanStartMillis >= slideSpanMillis);
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
        return slideStepMillis == that.slideStepMillis
                && slideSpanMillis == that.slideSpanMillis
                && splitGenerateDelayMillis == that.splitGenerateDelayMillis
                && Objects.equals(startMillis, that.startMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                slideStepMillis,
                slideSpanMillis,
                splitGenerateDelayMillis,
                startMillis);
    }

    public static SlideTimingSplitterEnumeratorBuilder builder() {
        return new SlideTimingSplitterEnumeratorBuilder();
    }

    /** A slide split enumerator builder. */
    public static class SlideTimingSplitterEnumeratorBuilder {
        private String sqlTemplate;
        private Long startMillis;
        private Long slideSpanMillis;
        private Long slideStepMillis;
        private Long splitGenerateDelayMillis;

        public SlideTimingSplitterEnumeratorBuilder setSqlTemplate(String sqlTemplate) {
            this.sqlTemplate = sqlTemplate;
            return this;
        }

        public SlideTimingSplitterEnumeratorBuilder setStartMillis(Long startMillis) {
            this.startMillis = startMillis;
            return this;
        }

        public SlideTimingSplitterEnumeratorBuilder setSlideSpanMillis(Long slideSpanMillis) {
            this.slideSpanMillis = slideSpanMillis;
            return this;
        }

        public SlideTimingSplitterEnumeratorBuilder setSlideStepMillis(Long slideStepMillis) {
            this.slideStepMillis = slideStepMillis;
            return this;
        }

        public SlideTimingSplitterEnumeratorBuilder setSplitGenerateDelayMillis(
                Long splitGenerateDelayMillis) {
            this.splitGenerateDelayMillis = splitGenerateDelayMillis;
            return this;
        }

        public SlideTimingSplitterEnumerator build() {
            Preconditions.checkArgument(startMillis > 0L, "'startMillis' must be greater than 0. ");
            Preconditions.checkArgument(
                    slideSpanMillis > 0 || slideStepMillis > 0,
                    "parameters must satisfy slideSpanMillis > 0 and slideStepMillis > 0");
            Preconditions.checkArgument(
                    splitGenerateDelayMillis >= 0L,
                    "parameters must satisfy splitGenerateDelayMillis >= 0");
            return new SlideTimingSplitterEnumerator(
                    sqlTemplate,
                    startMillis,
                    slideSpanMillis,
                    slideStepMillis,
                    splitGenerateDelayMillis);
        }
    }
}
