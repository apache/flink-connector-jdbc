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

package org.apache.flink.connector.jdbc.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.io.Serializable;
import java.sql.ResultSet;

/** JDBC source options. */
public class JdbcSourceOptions implements Serializable {

    private JdbcSourceOptions() {}

    public static final ConfigOption<Integer> READER_FETCH_BATCH_SIZE =
            ConfigOptions.key("connectors.jdbc.split-reader.fetch-batch-size")
                    .intType()
                    .defaultValue(1024);
    public static final ConfigOption<Integer> RESULTSET_TYPE =
            ConfigOptions.key("connectors.jdbc.split-reader.resultset-type")
                    .intType()
                    .defaultValue(ResultSet.TYPE_FORWARD_ONLY);
    public static final ConfigOption<Integer> RESULTSET_CONCURRENCY =
            ConfigOptions.key("connectors.jdbc.split-reader.resultset-concurrency")
                    .intType()
                    .defaultValue(ResultSet.CONCUR_READ_ONLY);
    public static final ConfigOption<Integer> RESULTSET_FETCH_SIZE =
            ConfigOptions.key("connectors.jdbc.split-reader.resultset-fetch-size")
                    .intType()
                    .defaultValue(0);
    public static final ConfigOption<Boolean> AUTO_COMMIT =
            ConfigOptions.key("connectors.jdbc.split-reader.auto-commit")
                    .booleanType()
                    .defaultValue(true);
}
