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

package org.apache.flink.connector.jdbc.testutils.databases.elasticsearch;

import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.types.Row;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/** Creates content for Elastic Bulk API call. */
public class ElasticsearchBulkBuilder {

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public static String createBulkContent(TableRow schema, List<Row> data)
            throws JsonProcessingException {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < data.size(); ++i) {
            builder.append(
                    format(
                            "{\"create\":{\"_index\":\"%s\",\"_id\":\"%d\"}}\n",
                            schema.getTableName(), i + 1));
            builder.append(rowToJson(schema, data.get(i))).append('\n');
        }
        return builder.toString();
    }

    private static String rowToJson(TableRow schema, Row data) throws JsonProcessingException {
        int fieldCount = schema.getTableDataFields().length;
        Map<String, Object> fieldMap = new HashMap<>(fieldCount);
        for (int i = 0; i < fieldCount; ++i) {
            fieldMap.put(schema.getTableFields()[i], adjustValueIfNeeded(data.getField(i)));
        }
        return OBJECT_MAPPER.writeValueAsString(fieldMap);
    }

    private static Object adjustValueIfNeeded(Object object) {
        if (object instanceof LocalDateTime) {
            return ((LocalDateTime) object)
                    .atZone(ZoneId.systemDefault())
                    .withZoneSameInstant(ZoneId.of("UTC"))
                    .toLocalDateTime();
        } else {
            return object;
        }
    }
}
