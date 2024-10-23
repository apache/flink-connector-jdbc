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

package org.apache.flink.connector.jdbc.core.datastream.source.config;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Settings describing how to do continuous file discovery and enumeration for the file source's
 * continuous discovery and streaming mode.
 */
@PublicEvolving
public class ContinuousUnBoundingSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Duration initialDiscoveryDelay;
    private final Duration discoveryInterval;

    public ContinuousUnBoundingSettings(
            Duration initialDiscoveryDelay, Duration discoveryInterval) {
        this.initialDiscoveryDelay = initialDiscoveryDelay;
        this.discoveryInterval = checkNotNull(discoveryInterval);
    }

    public Duration getDiscoveryInterval() {
        return discoveryInterval;
    }

    public Duration getInitialDiscoveryDelay() {
        return initialDiscoveryDelay;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "ContinuousUnBoundingSettings{"
                + "initialDiscoveryDelay="
                + initialDiscoveryDelay
                + ", discoveryInterval="
                + discoveryInterval
                + '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        ContinuousUnBoundingSettings that = (ContinuousUnBoundingSettings) object;
        return Objects.equals(initialDiscoveryDelay, that.initialDiscoveryDelay)
                && Objects.equals(discoveryInterval, that.discoveryInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialDiscoveryDelay, discoveryInterval);
    }
}
