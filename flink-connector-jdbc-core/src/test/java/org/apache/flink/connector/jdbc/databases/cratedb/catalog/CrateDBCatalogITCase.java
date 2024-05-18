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

package org.apache.flink.connector.jdbc.databases.cratedb.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.connector.jdbc.databases.cratedb.catalog.CrateDBCatalog.DEFAULT_DATABASE;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

/** E2E test for {@link CrateDBCatalog}. */
class CrateDBCatalogITCase extends CrateDBCatalogTestBase {

    private TableEnvironment tEnv;

    @BeforeEach
    void setup() {
        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        // use CrateDB catalog
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    @Test
    void testSelectField() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select id from %s", TABLE1))
                                .execute()
                                .collect());
        assertThat(results).hasToString("[+I[1]]");
    }

    @Test
    void testWithoutSchema() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TABLE1))
                                .execute()
                                .collect());
        assertThat(results).hasToString("[+I[1]]");
    }

    @Test
    void testWithSchema() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`",
                                                CrateDBTablePath.fromFlinkTableName(TABLE1)))
                                .execute()
                                .collect());
        assertThat(results).hasToString("[+I[1]]");
    }

    @Test
    void testFullPath() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s.%s.`%s`",
                                                TEST_CATALOG_NAME,
                                                DEFAULT_DATABASE,
                                                CrateDBTablePath.fromFlinkTableName(TABLE1)))
                                .execute()
                                .collect());
        assertThat(results).hasToString("[+I[1]]");
    }

    @Test
    void testGroupByInsert() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "insert into `%s`"
                                        + "select `int`, `short`, max(`long`), max(`real`), "
                                        + "max(`double`), max(`boolean`), "
                                        + "max(`text`), max(`timestamp`) "
                                        + "from `%s` group by `int`, `short`",
                                TABLE_TARGET_PRIMITIVE, TABLE_PRIMITIVE_TYPE))
                .await();
        executeSQL(String.format("REFRESH TABLE doc.%s", TABLE_TARGET_PRIMITIVE));
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from `%s`", TABLE_TARGET_PRIMITIVE))
                                .execute()
                                .collect());
        assertThat(results)
                .hasToString("[+I[1, 3, 4, 5.5, 6.6, true, b, 2016-06-22T19:10:25.123]]");
    }

    @Test
    void testPrimitiveTypes() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TABLE_PRIMITIVE_TYPE))
                                .execute()
                                .collect());

        assertThat(results)
                .hasToString(
                        "[+I[1, 3, 3, 4, 4, 5.5, 5.5, 6.6, 6.6, true, a, b, c, d  , e, 192.168.0.100, 2016-06-22T19:10:25.123]]");
    }

    @Test
    void testArrayTypes() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TABLE_ARRAY_TYPE))
                                .execute()
                                .collect());

        assertThat(results)
                .hasToString(
                        "[+I[[1, 2, 3], [3, 4, 5], [3, 4, 5], [4, 5, 6], [4, 5, 6], [5.5, 6.6, 7.7], [5.5, 6.6, 7.7], [6.6, 7.7, 8.8], [6.6, 7.7, 8.8], [true, false, true], [a, b, c], [a, b, c], [b, c, d], [b  , c  , d  ], [b, c, d], [0:0:0:0:0:ffff:c0a8:64, 10.2.5.28, 127.0.0.6], [2016-06-22T19:10:25.123, 2019-06-22T11:22:33.987], null]]");
    }
}
