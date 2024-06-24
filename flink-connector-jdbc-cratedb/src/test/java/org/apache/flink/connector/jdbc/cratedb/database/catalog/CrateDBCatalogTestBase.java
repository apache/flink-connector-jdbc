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

package org.apache.flink.connector.jdbc.cratedb.database.catalog;

import org.apache.flink.connector.jdbc.cratedb.CrateDBTestBase;
import org.apache.flink.connector.jdbc.testutils.JdbcITCaseBase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static org.apache.flink.connector.jdbc.cratedb.testutils.CrateDBDatabase.CONTAINER;

/** Test base for {@link CrateDBCatalog}. */
class CrateDBCatalogTestBase implements JdbcITCaseBase, CrateDBTestBase {

    public static final Logger LOG = LoggerFactory.getLogger(CrateDBCatalogTestBase.class);

    protected static final String TEST_CATALOG_NAME = "mycratedb";
    protected static final String TEST_USERNAME = CONTAINER.getUsername();
    protected static final String TEST_PWD = CONTAINER.getPassword();
    protected static final String TEST_SCHEMA = "test_schema";
    protected static final String TABLE1 = "t1";
    protected static final String TABLE2 = "t2";
    protected static final String TABLE3 = "t3";
    protected static final String TABLE_PRIMITIVE_TYPE = "primitive_table";
    protected static final String TABLE_TARGET_PRIMITIVE = "target_primitive_table";
    protected static final String TABLE_ARRAY_TYPE = "array_table";

    protected static String baseUrl;
    protected static CrateDBCatalog catalog;

    @BeforeAll
    static void init() throws SQLException {
        // For deterministic timestamp field results
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        // jdbc:crate://localhost:50807/crate?user=crate
        String jdbcUrl = CONTAINER.getJdbcUrl();
        // jdbc:crate://localhost:50807/
        baseUrl = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"));

        catalog =
                new CrateDBCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        CrateDBCatalog.DEFAULT_DATABASE,
                        TEST_USERNAME,
                        TEST_PWD,
                        baseUrl);

        // create test tables
        // table: crate.doc.t1
        // table: crate.doc.t2
        createTable(CrateDBTablePath.fromFlinkTableName(TABLE1), getSimpleTable().crateDBSchemaSql);
        createTable(CrateDBTablePath.fromFlinkTableName(TABLE2), getSimpleTable().crateDBSchemaSql);

        // table: crate.test_schema.t3
        createTable(new CrateDBTablePath(TEST_SCHEMA, TABLE3), getSimpleTable().crateDBSchemaSql);
        createTable(
                CrateDBTablePath.fromFlinkTableName(TABLE_PRIMITIVE_TYPE),
                getPrimitiveTable().crateDBSchemaSql);
        createTable(
                CrateDBTablePath.fromFlinkTableName(TABLE_TARGET_PRIMITIVE),
                getTargetPrimitiveTable().crateDBSchemaSql);
        createTable(
                CrateDBTablePath.fromFlinkTableName(TABLE_ARRAY_TYPE),
                getArrayTable().crateDBSchemaSql);

        executeSQL(
                String.format("insert into doc.%s values (%s);", TABLE1, getSimpleTable().values));
        executeSQL(
                String.format(
                        "insert into doc.%s values (%s);",
                        TABLE_PRIMITIVE_TYPE, getPrimitiveTable().values));
        executeSQL(
                String.format(
                        "insert into doc.%s values (%s);",
                        TABLE_ARRAY_TYPE, getArrayTable().values));
    }

    @AfterAll
    static void clean() throws SQLException {
        List<String> tables =
                Arrays.asList(
                        CrateDBTablePath.fromFlinkTableName(TABLE1).getFullPath(),
                        CrateDBTablePath.fromFlinkTableName(TABLE2).getFullPath(),
                        new CrateDBTablePath(TEST_SCHEMA, TABLE3).getFullPath(),
                        CrateDBTablePath.fromFlinkTableName(TABLE_PRIMITIVE_TYPE).getFullPath(),
                        CrateDBTablePath.fromFlinkTableName(TABLE_TARGET_PRIMITIVE).getFullPath(),
                        CrateDBTablePath.fromFlinkTableName(TABLE_ARRAY_TYPE).getFullPath());
        for (String table : tables) {
            executeSQL(String.format("DROP TABLE %s", table));
        }
    }

    public static void createTable(CrateDBTablePath tablePath, String tableSchemaSql)
            throws SQLException {
        executeSQL(String.format("CREATE TABLE %s(%s);", tablePath.getFullPath(), tableSchemaSql));
        executeSQL(String.format("REFRESH TABLE %s", tablePath.getFullPath()));
    }

    public static void executeSQL(String sql) throws SQLException {
        try (Connection conn =
                        DriverManager.getConnection(
                                String.format("%s/%s", baseUrl, CrateDBCatalog.DEFAULT_DATABASE),
                                TEST_USERNAME,
                                TEST_PWD);
                Statement statement = conn.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    /** Object holding schema and corresponding sql. */
    public static class TestTable {
        Schema schema;
        String crateDBSchemaSql;
        String values;

        public TestTable(Schema schema, String crateDBSchemaSql, String values) {
            this.schema = schema;
            this.crateDBSchemaSql = crateDBSchemaSql;
            this.values = values;
        }
    }

    public static TestTable getSimpleTable() {
        return new TestTable(
                Schema.newBuilder().column("id", DataTypes.INT()).build(), "id integer", "1");
    }

    // TODO: add back timestamptz and time types.
    //  Flink currently doesn't support converting time's precision, with the following error
    //  TableException: Unsupported conversion from data type 'TIME(6)' (conversion class:
    // java.sql.Time)
    //  to type information. Only data types that originated from type information fully support a
    // reverse conversion.
    public static TestTable getPrimitiveTable() {
        return new TestTable(
                Schema.newBuilder()
                        .column("int", DataTypes.INT().notNull())
                        .column("short", DataTypes.SMALLINT().notNull())
                        .column("smallint", DataTypes.SMALLINT())
                        .column("long", DataTypes.BIGINT())
                        .column("bigint", DataTypes.BIGINT())
                        .column("float", DataTypes.FLOAT())
                        .column("real", DataTypes.FLOAT())
                        .column("double", DataTypes.DOUBLE())
                        .column("double_precision", DataTypes.DOUBLE())
                        .column("boolean", DataTypes.BOOLEAN())
                        .column("string", DataTypes.STRING())
                        .column("text", DataTypes.STRING())
                        .column("char", DataTypes.CHAR(1))
                        .column("character", DataTypes.CHAR(3))
                        .column("character_varying", DataTypes.VARCHAR(20))
                        .column("ip", DataTypes.STRING())
                        .column("timestamp", DataTypes.TIMESTAMP(6))
                        //  .column("timestamptz", DataTypes.TIMESTAMP_WITH_TIME_ZONE(6))
                        .primaryKeyNamed("primitive_table_pk", "int", "short")
                        .build(),
                "int integer, "
                        + "short short, "
                        + "smallint smallint, "
                        + "long long, "
                        + "bigint bigint, "
                        + "float float, "
                        + "real real, "
                        + "double double, "
                        + "double_precision double precision, "
                        + "boolean boolean, "
                        + "string string, "
                        + "text text, "
                        + "char char, "
                        + "character character(3), "
                        + "character_varying character varying(20), "
                        + "ip ip, "
                        + "timestamp timestamp, "
                        // + "timestamptz timestamptz, "
                        + "PRIMARY KEY (int, short)",
                // Values
                "1,"
                        + "3,"
                        + "3,"
                        + "4,"
                        + "4,"
                        + "5.5,"
                        + "5.5,"
                        + "6.6,"
                        + "6.6,"
                        + "true,"
                        + "'a',"
                        + "'b',"
                        + "'c',"
                        + "'d',"
                        + "'e',"
                        + "'0:0:0:0:0:ffff:c0a8:64',"
                        + "'2016-06-22 19:10:25.123456'");
        //  + "'2006-06-22 19:10:25.123456'");
    }

    // TODO: add back timestamptz once planner supports timestamp with timezone
    public static TestTable getArrayTable() {
        return new TestTable(
                Schema.newBuilder()
                        .column("int_arr", DataTypes.ARRAY(DataTypes.INT()))
                        .column("short_arr", DataTypes.ARRAY(DataTypes.SMALLINT()))
                        .column("smallint_arr", DataTypes.ARRAY(DataTypes.SMALLINT()))
                        .column("long_arr", DataTypes.ARRAY(DataTypes.BIGINT()))
                        .column("bigint_arr", DataTypes.ARRAY(DataTypes.BIGINT()))
                        .column("float_arr", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .column("real_arr", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .column("double_arr", DataTypes.ARRAY(DataTypes.DOUBLE()))
                        .column("double_precision_arr", DataTypes.ARRAY(DataTypes.DOUBLE()))
                        .column("boolean_arr", DataTypes.ARRAY(DataTypes.BOOLEAN()))
                        .column("string_arr", DataTypes.ARRAY(DataTypes.STRING()))
                        .column("text_arr", DataTypes.ARRAY(DataTypes.STRING()))
                        .column("char_arr", DataTypes.ARRAY(DataTypes.CHAR(1)))
                        .column("character_arr", DataTypes.ARRAY(DataTypes.CHAR(3)))
                        .column("character_varying_arr", DataTypes.ARRAY(DataTypes.VARCHAR(20)))
                        .column("ip", DataTypes.ARRAY(DataTypes.STRING()))
                        .column("timestamp_arr", DataTypes.ARRAY(DataTypes.TIMESTAMP(6)))
                        // .column("timestamptz_arr",
                        // DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(4)))
                        .column("null_text_arr", DataTypes.ARRAY(DataTypes.STRING()))
                        .build(),
                "int_arr integer[], "
                        + "short_arr short[],"
                        + "smallint_arr smallint[],"
                        + "long_arr long[], "
                        + "bigint_arr bigint[], "
                        + "float_arr float[], "
                        + "real_arr real[], "
                        + "double_arr double[], "
                        + "double_precision_arr double precision[], "
                        + "boolean_arr boolean[], "
                        + "string_arr string[], "
                        + "text_arr text[], "
                        + "char_arr char[], "
                        + "character_arr character(3)[], "
                        + "character_varying_arr character varying(20)[],"
                        + "ip string[],"
                        + "timestamp_arr timestamp[], "
                        // + "timestamptz_arr timestamptz[], "
                        + "null_text_arr text[]",
                // Values
                "[1,2,3],"
                        + "[3,4,5],"
                        + "[3,4,5],"
                        + "[4,5,6],"
                        + "[4,5,6],"
                        + "[5.5,6.6,7.7],"
                        + "[5.5,6.6,7.7],"
                        + "[6.6,7.7,8.8],"
                        + "[6.6,7.7,8.8],"
                        + "[true,false,true],"
                        + "['a','b','c'],"
                        + "['a','b','c'],"
                        + "['b','c','d'],"
                        + "['b','c','d'],"
                        + "['b','c','d'],"
                        + "['0:0:0:0:0:ffff:c0a8:64', '10.2.5.28', '127.0.0.6'],"
                        + "['2016-06-22 19:10:25.123456', '2019-06-22 11:22:33.987654'],"
                        // + "['2006-06-22 19:10:25.123456', '2019-06-22 11:22:33.987654'],"
                        + "NULL");
    }

    public static TestTable getTargetPrimitiveTable() {
        return new TestTable(
                Schema.newBuilder()
                        .column("int", DataTypes.INT().notNull())
                        .column("short", DataTypes.SMALLINT().notNull())
                        .column("long", DataTypes.BIGINT())
                        .column("real", DataTypes.FLOAT())
                        .column("double", DataTypes.DOUBLE())
                        .column("boolean", DataTypes.BOOLEAN())
                        .column("text", DataTypes.STRING())
                        .column("timestamp", DataTypes.TIMESTAMP(6))
                        .build(),
                "int integer, "
                        + "short short, "
                        + "long long, "
                        + "real real, "
                        + "double double, "
                        + "boolean boolean, "
                        + "text text, "
                        + "timestamp timestamp",
                // Values
                null);
    }

    protected static String sanitizeSchemaSQL(String schemaSQL) {
        return schemaSQL
                .replaceAll("CHAR\\(\\d+\\)", "CHAR(2147483647)")
                .replaceAll("VARCHAR\\(\\d+\\)", "STRING");
    }
}
