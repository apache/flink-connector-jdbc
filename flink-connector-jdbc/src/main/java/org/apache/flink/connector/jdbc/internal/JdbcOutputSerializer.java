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

package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;

/** A Serializer that have in account the actual configuration. */
@Internal
public class JdbcOutputSerializer<T> implements Serializable {

    private final TypeInformation<T> typeInformation;
    private TypeSerializer<T> typeSerializer;

    private JdbcOutputSerializer(TypeInformation<T> typeInformation) {
        this.typeInformation = typeInformation;
    }

    public static <S> JdbcOutputSerializer<S> of(TypeInformation<S> typeInformation) {
        return new JdbcOutputSerializer<>(typeInformation);
    }

    public JdbcOutputSerializer<T> configure(ExecutionConfig executionConfig) {
        if (executionConfig != null && executionConfig.isObjectReuseEnabled()) {
            this.typeSerializer = typeInformation.createSerializer(executionConfig);
        }
        return this;
    }

    public T serialize(T record) {
        return this.typeSerializer == null ? record : typeSerializer.copy(record);
    }
}
