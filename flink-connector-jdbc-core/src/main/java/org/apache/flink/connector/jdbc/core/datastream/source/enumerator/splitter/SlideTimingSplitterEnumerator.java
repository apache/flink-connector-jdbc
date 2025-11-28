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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;

/** A split enumerator based on sql-parameters grains. */
public class SlideTimingSplitterEnumerator extends PreparedSplitterEnumerator {
    public static final Logger LOG = LoggerFactory.getLogger(SlideTimingSplitterEnumerator.class);

    private final JdbcParameterValuesProvider parametersProvider;
    private Serializable userDefinedState;

    protected SlideTimingSplitterEnumerator(
            String sqlTemplate,
            @Nullable JdbcParameterValuesProvider parametersProvider,
            Serializable userDefinedState) {
        super(sqlTemplate, new Serializable[0][]);
        this.parametersProvider = parametersProvider;
        this.userDefinedState = userDefinedState;
    }

    public static SlideTimingSplitterEnumerator of(
            String sqlTemplate,
            @Nullable JdbcParameterValuesProvider parametersProvider,
            Serializable userDefinedState) {
        return new SlideTimingSplitterEnumerator(sqlTemplate, parametersProvider, userDefinedState);
    }

    @Override
    public boolean isAllSplitsFinished() {
        return false;
    }

    @VisibleForTesting
    public Serializable[][] getSqlParameters() {
        if (parametersProvider == null) {
            return new Serializable[0][];
        }

        if (userDefinedState != null) {
            parametersProvider.setOptionalState(userDefinedState);
        }
        Serializable[][] parameters = parametersProvider.getParameterValues();

        // update state
        userDefinedState = parametersProvider.getLatestOptionalState();

        return parameters == null ? new Serializable[0][] : parameters;
    }

    @Override
    public @Nullable Serializable serializableState() {
        return this.userDefinedState;
    }

    @Override
    public SlideTimingSplitterEnumerator restoreState(Serializable state) {
        this.userDefinedState = state;
        return this;
    }
}
