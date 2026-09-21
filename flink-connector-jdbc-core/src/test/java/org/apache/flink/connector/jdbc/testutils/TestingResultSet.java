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

package org.apache.flink.connector.jdbc.testutils;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;

/**
 * A {@link ResultSet} over a single fixed row, for converters that read their columns through
 * {@link ResultSet#getObject(int)}. It lets a test hand a column a value no driver would return for
 * it, such as a {@link java.time.LocalDateTime} for a timestamp. Any other method throws, so a test
 * that starts to depend on one fails instead of silently reading a default.
 */
public final class TestingResultSet {

    private TestingResultSet() {}

    /** Returns a {@link ResultSet} whose {@code getObject(i)} answers {@code columns[i - 1]}. */
    public static ResultSet of(Object... columns) {
        return (ResultSet)
                Proxy.newProxyInstance(
                        TestingResultSet.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("getObject")
                                    && args != null
                                    && args.length == 1
                                    && args[0] instanceof Integer) {
                                return columns[(Integer) args[0] - 1];
                            }
                            throw new UnsupportedOperationException(
                                    "TestingResultSet only answers getObject(int), not "
                                            + method.getName());
                        });
    }
}
