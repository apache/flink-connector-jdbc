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

package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcOutputSerializerTest {

    @Test
    void testSerializer() {
        TypeInformation<Row> typeInformation = TypeInformation.of(Row.class);
        TypeSerializer<Row> typeSerializer =
                typeInformation.createSerializer(new ExecutionConfig());
        JdbcOutputSerializer<Row> serializer = JdbcOutputSerializer.of(typeSerializer);

        Row original = Row.of(123);
        Row noReuse = serializer.withObjectReuseEnabled(false).serialize(original);
        Row withReuse = serializer.withObjectReuseEnabled(true).serialize(original);

        assertThat(noReuse).isEqualTo(original);
        assertThat(withReuse).isEqualTo(original);

        original.setField(0, 321);

        // if disable object is reusable
        assertThat(noReuse).isEqualTo(original);
        // if enabled object is duplicate
        assertThat(withReuse).isNotEqualTo(original);
    }
}
