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

package org.apache.flink.connector.jdbc.lineage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.datasource.statements.SimpleJdbcQueryStatement;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import io.openlineage.sql.OpenLineageSql;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utils for Lineage metadata extraction. */
@PublicEvolving
public class LineageUtils {

    public static Optional<String> nameOf(JdbcQueryStatement<?> jdbcQueryStatement) {
        if (!(jdbcQueryStatement instanceof SimpleJdbcQueryStatement)) {
            return Optional.empty();
        }

        SimpleJdbcQueryStatement<?> simpleJdbcQueryStatement =
                (SimpleJdbcQueryStatement<?>) jdbcQueryStatement;
        return nameOf(simpleJdbcQueryStatement.query());
    }

    public static Optional<String> nameOf(String query) {
        return OpenLineageSql.parse(Arrays.asList(query))
                .map(
                        sqlMeta ->
                                sqlMeta.inTables().isEmpty()
                                        ? ""
                                        : sqlMeta.inTables().get(0).qualifiedName());
    }

    public static String namespaceOf(JdbcConnectionProvider jdbcConnectionProvider) {
        if (!(jdbcConnectionProvider instanceof SimpleJdbcConnectionProvider)) {
            return "";
        }

        SimpleJdbcConnectionProvider simpleJdbcConnectionProvider =
                (SimpleJdbcConnectionProvider) jdbcConnectionProvider;

        return JdbcUtils.getJdbcNamespace(
                simpleJdbcConnectionProvider.getDbURL(),
                simpleJdbcConnectionProvider.getProperties());
    }

    public static LineageDataset datasetOf(
            String name, String namespace, List<LineageDatasetFacet> facets) {

        return new LineageDataset() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String namespace() {
                return namespace;
            }

            @Override
            public Map<String, LineageDatasetFacet> facets() {
                Map<String, LineageDatasetFacet> facetMap = new HashMap<>();
                facetMap.putAll(
                        facets.stream()
                                .collect(
                                        Collectors.toMap(LineageDatasetFacet::name, item -> item)));
                return facetMap;
            }
        };
    }

    public static LineageVertex lineageVertexOf(Collection<LineageDataset> datasets) {
        return new LineageVertex() {
            @Override
            public List<LineageDataset> datasets() {
                return datasets.stream().collect(Collectors.toList());
            }
        };
    }

    public static SourceLineageVertex sourceLineageVertexOf(
            Boundedness boundedness, Collection<LineageDataset> datasets) {
        return new SourceLineageVertex() {
            @Override
            public Boundedness boundedness() {
                return boundedness;
            }

            @Override
            public List<LineageDataset> datasets() {
                return datasets.stream().collect(Collectors.toList());
            }
        };
    }
}
