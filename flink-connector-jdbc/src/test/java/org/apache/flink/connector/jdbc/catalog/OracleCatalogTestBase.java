package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.connector.jdbc.databases.oracle.OracleDatabase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.DecimalType;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleCatalogTestBase implements OracleDatabase {

    public static final Logger LOG = LoggerFactory.getLogger(OracleCatalogTestBase.class);

    protected static final String TEST_CATALOG_NAME = "mypg";
    protected static final String TEST_USERNAME = CONTAINER.getUsername();
    protected static final String TEST_PWD = CONTAINER.getPassword();
    protected static final String TEST_DB = "test";
    protected static final String TEST_SCHEMA = "test_schema";
    protected static final String TABLE1 = "t1";
    protected static final String TABLE2 = "t2";
    protected static final String TABLE3 = "t3";
    protected static final String TABLE4 = "t4";
    protected static final String TABLE5 = "t5";
    protected static final String TABLE_PRIMITIVE_TYPE = "primitive_table";
    protected static final String TABLE_PRIMITIVE_TYPE2 = "primitive_table2";
    protected static final String TABLE_ARRAY_TYPE = "array_table";
    protected static final String TABLE_SERIAL_TYPE = "serial_table";

    protected static String baseUrl;
    protected static OracleCatalog catalog;


    @BeforeAll
    static void init() throws SQLException {
        // jdbc:oracle:thin:@//localhost:50807/helowin
        String jdbcUrl = CONTAINER.getJdbcUrl();
        // jdbc:oracle:thin:@//localhost:50807/
        baseUrl = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"));

        catalog =
            new OracleCatalog(
                Thread.currentThread().getContextClassLoader(),
                TEST_CATALOG_NAME,
                OracleCatalog.DEFAULT_DATABASE,
                TEST_USERNAME,
                TEST_PWD,
                baseUrl);

        // create test database and schema
        createSchema(TEST_DB, TEST_SCHEMA);

        // create test tables
        // table: helowin.public.t1
        // table: helowin.public.t4
        // table: helowin.public.t5
        createTable(PostgresTablePath.fromFlinkTableName(TABLE1), getSimpleTable().oracleSchemaSql);
        createTable(PostgresTablePath.fromFlinkTableName(TABLE4), getSimpleTable().oracleSchemaSql);
        createTable(PostgresTablePath.fromFlinkTableName(TABLE5), getSimpleTable().oracleSchemaSql);

        // table: test.public.t2
        // table: test.test_schema.t3
        // table: helowin.public.dt
        // table: helowin.public.dt2
        createTable(
            TEST_DB,
            PostgresTablePath.fromFlinkTableName(TABLE2),
            getSimpleTable().oracleSchemaSql);
        createTable(
            TEST_DB, new PostgresTablePath(TEST_SCHEMA, TABLE3), getSimpleTable().oracleSchemaSql);
        createTable(
            PostgresTablePath.fromFlinkTableName(TABLE_PRIMITIVE_TYPE),
            getPrimitiveTable().oracleSchemaSql);
        createTable(
            PostgresTablePath.fromFlinkTableName(TABLE_PRIMITIVE_TYPE2),
            getPrimitiveTable("test_pk2").oracleSchemaSql);
        createTable(
            PostgresTablePath.fromFlinkTableName(TABLE_ARRAY_TYPE),
            getArrayTable().oracleSchemaSql);
        createTable(
            PostgresTablePath.fromFlinkTableName(TABLE_SERIAL_TYPE),
            getSerialTable().oracleSchemaSql);

        executeSQL(
            OracleCatalog.DEFAULT_DATABASE,
            String.format(
                "insert into public.%s values (%s);", TABLE1, getSimpleTable().values));
        executeSQL(
            OracleCatalog.DEFAULT_DATABASE,
            String.format(
                "insert into %s values (%s);",
                TABLE_PRIMITIVE_TYPE, getPrimitiveTable().values));
        executeSQL(
            OracleCatalog.DEFAULT_DATABASE,
            String.format(
                "insert into %s values (%s);", TABLE_ARRAY_TYPE, getArrayTable().values));
        executeSQL(
            OracleCatalog.DEFAULT_DATABASE,
            String.format(
                "insert into %s values (%s);", TABLE_SERIAL_TYPE, getSerialTable().values));
    }

    public static void createTable(PostgresTablePath tablePath, String tableSchemaSql)
        throws SQLException {
        executeSQL(
            OracleCatalog.DEFAULT_DATABASE,
            String.format("CREATE TABLE %s(%s);", tablePath.getFullPath(), tableSchemaSql));
    }

    public static void createTable(String db, PostgresTablePath tablePath, String tableSchemaSql)
        throws SQLException {
        executeSQL(
            db, String.format("CREATE TABLE %s(%s);", tablePath.getFullPath(), tableSchemaSql));
    }

    public static void createSchema(String db, String schema) throws SQLException {
        executeSQL(db, String.format("CREATE SCHEMA %s", schema));
    }

    public static void createDatabase(String database) throws SQLException {
        executeSQL(String.format("CREATE DATABASE %s;", database));
    }

    public static void executeSQL(String sql) throws SQLException {
        executeSQL("", sql);
    }

    public static void executeSQL(String db, String sql) throws SQLException {
        try (Connection conn =
                 DriverManager.getConnection(
                     String.format("%s/%s", baseUrl, db), TEST_USERNAME, TEST_PWD);
             Statement statement = conn.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw e;
        }
    }


    /** Object holding schema and corresponding sql. */
    public static class TestTable {
        Schema schema;
        String oracleSchemaSql;
        String values;

        public TestTable(Schema schema, String oracleSchemaSql, String values) {
            this.schema = schema;
            this.oracleSchemaSql = oracleSchemaSql;
            this.values = values;
        }
    }

    public static OracleCatalogTestBase.TestTable getSimpleTable() {
        return new OracleCatalogTestBase.TestTable(
            Schema.newBuilder().column("id", DataTypes.INT()).build(), "id integer", "1");
    }

    // oracle doesn't support to use the same primary key name across different tables,
    // make the table parameterized to resolve this problem.
    public static OracleCatalogTestBase.TestTable getPrimitiveTable() {
        return getPrimitiveTable("test_pk");
    }

    // TODO: add back timestamptz and time types.
    //  Flink currently doesn't support converting time's precision, with the following error
    //  TableException: Unsupported conversion from data type 'TIME(6)' (conversion class:
    // java.sql.Time)
    //  to type information. Only data types that originated from type information fully support a
    // reverse conversion.
    public static OracleCatalogTestBase.TestTable getPrimitiveTable(String primaryKeyName) {
        return new OracleCatalogTestBase.TestTable(
            Schema.newBuilder()
                .column("int", DataTypes.INT().notNull())
                .column("bytea", DataTypes.BYTES())
                .column("short", DataTypes.SMALLINT().notNull())
                .column("long", DataTypes.BIGINT())
                .column("real", DataTypes.FLOAT())
                .column("double_precision", DataTypes.DOUBLE())
                .column("numeric", DataTypes.DECIMAL(10, 5))
                .column("decimal", DataTypes.DECIMAL(10, 1))
                .column("boolean", DataTypes.BOOLEAN())
                .column("text", DataTypes.STRING())
                .column("char", DataTypes.CHAR(1))
                .column("character", DataTypes.CHAR(3))
                .column("character_varying", DataTypes.VARCHAR(20))
                .column("timestamp", DataTypes.TIMESTAMP(5))
                //				.field("timestamptz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(4))
                .column("date", DataTypes.DATE())
                .column("time", DataTypes.TIME(0))
                .column("default_numeric", DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18))
                .primaryKeyNamed(primaryKeyName, "short", "int")
                .build(),
            "int integer, "
                + "bytea bytea, "
                + "short smallint, "
                + "long bigint, "
                + "real real, "
                + "double_precision double precision, "
                + "numeric numeric(10, 5), "
                + "decimal decimal(10, 1), "
                + "boolean boolean, "
                + "text text, "
                + "char char, "
                + "character character(3), "
                + "character_varying character varying(20), "
                + "timestamp timestamp(5), "
                +
                //				"timestamptz timestamptz(4), " +
                "date date,"
                + "time time(0), "
                + "default_numeric numeric, "
                + "CONSTRAINT "
                + primaryKeyName
                + " PRIMARY KEY (short, int)",
            "1,"
                + "'2',"
                + "3,"
                + "4,"
                + "5.5,"
                + "6.6,"
                + "7.7,"
                + "8.8,"
                + "true,"
                + "'a',"
                + "'b',"
                + "'c',"
                + "'d',"
                + "'2016-06-22 19:10:25',"
                +
                //				"'2006-06-22 19:10:25'," +
                "'2015-01-01',"
                + "'00:51:02.746572', "
                + "500");
    }

    // TODO: add back timestamptz once planner supports timestamp with timezone
    public static OracleCatalogTestBase.TestTable getArrayTable() {
        return new OracleCatalogTestBase.TestTable(
            Schema.newBuilder()
                .column("int_arr", DataTypes.ARRAY(DataTypes.INT()))
                .column("bytea_arr", DataTypes.ARRAY(DataTypes.BYTES()))
                .column("short_arr", DataTypes.ARRAY(DataTypes.SMALLINT()))
                .column("long_arr", DataTypes.ARRAY(DataTypes.BIGINT()))
                .column("real_arr", DataTypes.ARRAY(DataTypes.FLOAT()))
                .column("double_precision_arr", DataTypes.ARRAY(DataTypes.DOUBLE()))
                .column("numeric_arr", DataTypes.ARRAY(DataTypes.DECIMAL(10, 5)))
                .column(
                    "numeric_arr_default",
                    DataTypes.ARRAY(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18)))
                .column("decimal_arr", DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)))
                .column("boolean_arr", DataTypes.ARRAY(DataTypes.BOOLEAN()))
                .column("text_arr", DataTypes.ARRAY(DataTypes.STRING()))
                .column("char_arr", DataTypes.ARRAY(DataTypes.CHAR(1)))
                .column("character_arr", DataTypes.ARRAY(DataTypes.CHAR(3)))
                .column("character_varying_arr", DataTypes.ARRAY(DataTypes.VARCHAR(20)))
                .column("timestamp_arr", DataTypes.ARRAY(DataTypes.TIMESTAMP(5)))
                //				.field("timestamptz_arr",
                // DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(4)))
                .column("date_arr", DataTypes.ARRAY(DataTypes.DATE()))
                .column("time_arr", DataTypes.ARRAY(DataTypes.TIME(0)))
                .column("null_bytea_arr", DataTypes.ARRAY(DataTypes.BYTES()))
                .column("null_text_arr", DataTypes.ARRAY(DataTypes.STRING()))
                .build(),
            "int_arr integer[], "
                + "bytea_arr bytea[], "
                + "short_arr smallint[], "
                + "long_arr bigint[], "
                + "real_arr real[], "
                + "double_precision_arr double precision[], "
                + "numeric_arr numeric(10, 5)[], "
                + "numeric_arr_default numeric[], "
                + "decimal_arr decimal(10,2)[], "
                + "boolean_arr boolean[], "
                + "text_arr text[], "
                + "char_arr char[], "
                + "character_arr character(3)[], "
                + "character_varying_arr character varying(20)[], "
                + "timestamp_arr timestamp(5)[], "
                +
                //				"timestamptz_arr timestamptz(4)[], " +
                "date_arr date[], "
                + "time_arr time(0)[], "
                + "null_bytea_arr bytea[], "
                + "null_text_arr text[]",
            String.format(
                "'{1,2,3}',"
                    + "'{2,3,4}',"
                    + "'{3,4,5}',"
                    + "'{4,5,6}',"
                    + "'{5.5,6.6,7.7}',"
                    + "'{6.6,7.7,8.8}',"
                    + "'{7.7,8.8,9.9}',"
                    + "'{8.8,9.9,10.10}',"
                    + "'{9.9,10.10,11.11}',"
                    + "'{true,false,true}',"
                    + "'{a,b,c}',"
                    + "'{b,c,d}',"
                    + "'{b,c,d}',"
                    + "'{b,c,d}',"
                    + "'{\"2016-06-22 19:10:25\", \"2019-06-22 19:10:25\"}',"
                    +
                    //				"'{\"2006-06-22 19:10:25\", \"2009-06-22 19:10:25\"}'," +
                    "'{\"2015-01-01\", \"2020-01-01\"}',"
                    + "'{\"00:51:02.746572\", \"00:59:02.746572\"}',"
                    + "NULL,"
                    + "NULL"));
    }

    public static OracleCatalogTestBase.TestTable getSerialTable() {
        return new OracleCatalogTestBase.TestTable(
            Schema.newBuilder()
                // serial fields are returned as not null by ResultSetMetaData.columnNoNulls
                .column("f0", DataTypes.SMALLINT().notNull())
                .column("f1", DataTypes.INT().notNull())
                .column("f2", DataTypes.SMALLINT().notNull())
                .column("f3", DataTypes.INT().notNull())
                .column("f4", DataTypes.BIGINT().notNull())
                .column("f5", DataTypes.BIGINT().notNull())
                .build(),
            "f0 smallserial, "
                + "f1 serial, "
                + "f2 serial2, "
                + "f3 serial4, "
                + "f4 serial8, "
                + "f5 bigserial",
            "32767,"
                + "2147483647,"
                + "32767,"
                + "2147483647,"
                + "9223372036854775807,"
                + "9223372036854775807");
    }


}
