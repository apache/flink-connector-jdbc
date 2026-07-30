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

package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter.snapshot.domain;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/** Represents the bounds of a table split with lower and upper bound values. */
@PublicEvolving
public class TableBounds implements Serializable {

    private final Object lowerBound;
    private final Object upperBound;

    public TableBounds(Object lowerBound, Object upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public Object lowerBound() {
        return lowerBound;
    }

    public Object upperBound() {
        return upperBound;
    }

    public static TableBounds of(Object lowerBound, Object upperBound) {
        return new TableBounds(lowerBound, upperBound);
    }

    public static TableBounds empty() {
        return new TableBounds(null, null);
    }

    public boolean isEmpty() {
        return lowerBound == null && upperBound == null;
    }

    public Serializable[] getBoundsAsParams() {
        if (isEmpty()) {
            return null;
        }

        List<Serializable> list = new ArrayList<>();
        if (lowerBound != null) {
            list.add((Serializable) lowerBound);
        }
        if (upperBound != null) {
            list.add((Serializable) upperBound);
        }

        return list.toArray(new Serializable[0]);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TableBounds)) {
            return false;
        }
        TableBounds that = (TableBounds) o;
        return Objects.equals(lowerBound, that.lowerBound)
                && Objects.equals(upperBound, that.upperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lowerBound, upperBound);
    }

    @Override
    public String toString() {
        if (isEmpty()) {
            return "[]";
        }
        return new StringJoiner(",", "[", "]")
                .add(lowerBound == null ? "" : lowerBound.toString())
                .add(upperBound == null ? "" : upperBound.toString())
                .toString();
    }
}
