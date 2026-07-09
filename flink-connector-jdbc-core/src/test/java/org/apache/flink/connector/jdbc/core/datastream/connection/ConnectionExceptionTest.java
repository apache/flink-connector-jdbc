/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.core.datastream.connection;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionExceptionTest {

    @Test
    void testMessageOnlyConstructor() {
        ConnectionException exception = new ConnectionException("failed");

        assertThat(exception.getMessage()).isEqualTo("failed");
        assertThat(exception.getCause()).isNull();
        assertThat(exception).isInstanceOf(RuntimeException.class);
    }

    @Test
    void testCauseOnlyConstructor() {
        Exception cause = new SQLException("root cause");

        ConnectionException exception = new ConnectionException(cause);

        assertThat(exception.getCause()).isSameAs(cause);
    }

    @Test
    void testMessageAndCauseConstructor() {
        Exception cause = new SQLException("root cause");

        ConnectionException exception = new ConnectionException("failed", cause);

        assertThat(exception.getMessage()).isEqualTo("failed");
        assertThat(exception.getCause()).isSameAs(cause);
    }
}
