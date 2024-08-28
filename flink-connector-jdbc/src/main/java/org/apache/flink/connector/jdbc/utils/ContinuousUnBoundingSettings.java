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

package org.apache.flink.connector.jdbc.utils;

import org.apache.flink.annotation.PublicEvolving;

import java.time.Duration;

/**
 * Settings describing how to do continuous file discovery and enumeration for the file source's
 * continuous discovery and streaming mode.
 *
 * @deprecated Use {@link
 *     org.apache.flink.connector.jdbc.core.datastream.source.config.ContinuousUnBoundingSettings}
 */
@PublicEvolving
@Deprecated
public final class ContinuousUnBoundingSettings
        extends org.apache.flink.connector.jdbc.core.datastream.source.config
                .ContinuousUnBoundingSettings {

    public ContinuousUnBoundingSettings(
            Duration initialDiscoveryDelay, Duration discoveryInterval) {
        super(initialDiscoveryDelay, discoveryInterval);
    }
}
