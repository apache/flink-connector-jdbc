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
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;

/** A Serializer that have in account the actual configuration. */
@Internal
public class JdbcOutputSerializer<T> implements Serializable {

    private final TypeSerializer<T> typeSerializer;
    private boolean objectReuse;

    private JdbcOutputSerializer(TypeSerializer<T> typeSerializer, boolean objectReuse) {
        this.typeSerializer = typeSerializer;
        this.objectReuse = objectReuse;
    }

    public static <S> JdbcOutputSerializer<S> of(TypeSerializer<S> typeSerializer) {
        return of(typeSerializer, false);
    }

    public static <S> JdbcOutputSerializer<S> of(
            TypeSerializer<S> typeSerializer, boolean objectReuse) {
        return new JdbcOutputSerializer<>(typeSerializer, objectReuse);
    }

    public JdbcOutputSerializer<T> withObjectReuseEnabled(boolean enabled) {
        this.objectReuse = enabled;
        return this;
    }

    public T serialize(T record) {
        return this.objectReuse ? typeSerializer.copy(record) : record;
    }
}
