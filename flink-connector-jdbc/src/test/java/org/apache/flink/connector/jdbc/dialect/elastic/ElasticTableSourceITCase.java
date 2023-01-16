package org.apache.flink.connector.jdbc.dialect.elastic;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** The Table Source ITCase for {@link ElasticDialect}. */
public class ElasticTableSourceITCase extends AbstractTestBase {

    private static final ElasticsearchTestContainer container = new ElasticsearchTestContainer();
    private static final String INPUT_TABLE = "test_table_1";
    private static final String INPUT_DATA_PATH = "elastic-test-data/test-data.json";
    private static final String TEST_INDEX_DEFINITION_PATH = "elastic-test-data/test-index.json";

    private static final String USERNAME = "elastic";
    private static final String PASSWORD = "password";

    private TableEnvironment tEnv;

    @BeforeClass
    public static void beforeAll() throws Exception {
        container.withEnv("xpack.security.enabled", "true");
        container.withEnv("ELASTIC_PASSWORD", PASSWORD);
        container.withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g");
        container.start();
        Class.forName(container.getDriverClassName());
        enableTrial();
        // Elastic JDBC connector supports only reads, so test data has to be added via REST API.
        createTestIndex(INPUT_TABLE, TEST_INDEX_DEFINITION_PATH);
        addTestData(INPUT_TABLE, INPUT_DATA_PATH);
    }

    private static void enableTrial() throws Exception {
        Request request =
                new Request.Builder()
                        .url(
                                format(
                                        "http://%s:%d/_license/start_trial?acknowledge=true",
                                        container.getHost(), container.getElasticPort()))
                        .post(RequestBody.create(new byte[] {}))
                        .addHeader("Authorization", Credentials.basic(USERNAME, PASSWORD))
                        .build();
        executeRequest(request);
    }

    private static void createTestIndex(String inputTable, String indexPath) throws Exception {
        Request request =
                new Request.Builder()
                        .url(
                                format(
                                        "http://%s:%d/%s/",
                                        container.getHost(),
                                        container.getElasticPort(),
                                        inputTable))
                        .put(RequestBody.create(loadResource(indexPath)))
                        .addHeader("Content-Type", "application/json")
                        .addHeader("Authorization", Credentials.basic(USERNAME, PASSWORD))
                        .build();
        executeRequest(request);
    }

    private static void addTestData(String inputTable, String inputPath) throws Exception {
        Request request =
                new Request.Builder()
                        .url(
                                format(
                                        "http://%s:%d/%s/_bulk/",
                                        container.getHost(),
                                        container.getElasticPort(),
                                        inputTable))
                        .post(RequestBody.create(loadResource(inputPath)))
                        .addHeader("Content-Type", "application/json")
                        .addHeader("Authorization", Credentials.basic(USERNAME, PASSWORD))
                        .build();
        executeRequest(request);
    }

    private static void executeRequest(Request request) throws IOException {
        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
        }
    }

    private static byte[] loadResource(String path) throws IOException {
        return IOUtils.toByteArray(
                Objects.requireNonNull(
                        ElasticTableSourceITCase.class.getClassLoader().getResourceAsStream(path)));
    }

    @Before
    public void before() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @AfterClass
    public static void afterAll() {
        container.stop();
    }

    @Test
    public void shouldReadRecords() {
        createNonPartitionedTable(INPUT_TABLE);
        List<String> results = executeAndCollect("SELECT * FROM " + INPUT_TABLE);
        assertEquals(15, results.size());
    }

    @Test
    public void shouldReadRecordsFromPartitionedTable() {
        createPartitionedTable(INPUT_TABLE);
        List<String> results = executeAndCollect("SELECT * FROM " + INPUT_TABLE);
        assertEquals(15, results.size());
    }

    @Test
    public void shouldFilterByFloatColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual =
                executeAndCollect(
                        "SELECT * FROM "
                                + INPUT_TABLE
                                + " WHERE float_col > 5.0 AND float_col < 6.0");

        List<String> expected =
                singletonList(
                        "+I[5.1234, 5.123456787, 33, 8345, -200000, -900000000, 1200000000, 312.523, flink test 5, flink 1, flink_5, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-01T16:30:10, 192.168.1.5, 1.2.5, aaa bbb ccc ddd iii, false, mmm]");
        assertEquals(expected, actual);
    }

    @Test
    public void shouldFilterByDoubleColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual =
                executeAndCollect(
                        "SELECT * FROM "
                                + INPUT_TABLE
                                + " WHERE double_col > 5.0 AND double_col < 6.0");

        List<String> expected =
                singletonList(
                        "+I[5.1234, 5.123456787, 33, 8345, -200000, -900000000, 1200000000, 312.523, flink test 5, flink 1, flink_5, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-01T16:30:10, 192.168.1.5, 1.2.5, aaa bbb ccc ddd iii, false, mmm]");
        assertEquals(expected, actual);
    }

    @Test
    public void shouldFilterByByteColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual =
                executeAndCollect(
                        "SELECT * FROM " + INPUT_TABLE + " WHERE byte_col > 30 AND byte_col < 40");

        List<String> expected =
                singletonList(
                        "+I[5.1234, 5.123456787, 33, 8345, -200000, -900000000, 1200000000, 312.523, flink test 5, flink 1, flink_5, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-01T16:30:10, 192.168.1.5, 1.2.5, aaa bbb ccc ddd iii, false, mmm]");
        assertEquals(expected, actual);
    }

    @Test
    public void shouldFilterByShortColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual =
                executeAndCollect(
                        "SELECT * FROM "
                                + INPUT_TABLE
                                + " WHERE short_col > 15000 AND short_col < 16000");

        List<String> expected =
                singletonList(
                        "+I[8.1234, 8.123456787, 55, 15345, -80000, 0, 2100000000, 421.823, flink test 8, flink 1, flink_8, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-02T11:20:10, 192.168.2.1, 1.3.1, aaa bbb ccc jjj eee, true, www]");
        assertEquals(expected, actual);
    }

    @Test
    public void shouldFilterByIntegerColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual =
                executeAndCollect(
                        "SELECT * FROM "
                                + INPUT_TABLE
                                + " WHERE integer_col > 150000 AND integer_col < 250000");

        List<String> expected =
                singletonList(
                        "+I[12.1234, 12.123456787, 90, 23345, 200000, 1200000000, 3300000000, 856.835, flink test 12, flink 1, flink_12, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T11:30:10, 192.168.3.1, 1.3.5, aaa nnn ccc ddd eee, false, 6ad]");
        assertEquals(expected, actual);
    }

    @Test
    public void shouldFilterByLongColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual =
                executeAndCollect(
                        "SELECT * FROM "
                                + INPUT_TABLE
                                + " WHERE long_col > 1000000000 AND long_col < 1300000000");

        List<String> expected =
                singletonList(
                        "+I[12.1234, 12.123456787, 90, 23345, 200000, 1200000000, 3300000000, 856.835, flink test 12, flink 1, flink_12, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T11:30:10, 192.168.3.1, 1.3.5, aaa nnn ccc ddd eee, false, 6ad]");
        assertEquals(expected, actual);
    }

    @Test
    public void shouldFilterByUnsignedLongColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual =
                executeAndCollect(
                        "SELECT * FROM "
                                + INPUT_TABLE
                                + " WHERE unsigned_long_col > 1700000000 AND unsigned_long_col < 1900000000");

        List<String> expected =
                singletonList(
                        "+I[7.1234, 7.123456787, 48, 13345, -120000, -300000000, 1800000000, 412.723, flink test 7, flink 1, flink_7, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-02T08:50:10, 192.168.1.7, 1.3.0, aaa bbb ccc iii eee, true, sss]");
        assertEquals(expected, actual);
    }

    @Test
    public void shouldFilterByScaledFloatColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual =
                executeAndCollect(
                        "SELECT * FROM "
                                + INPUT_TABLE
                                + " WHERE scaled_float_col > 412.000 AND scaled_float_col < 412.500");

        List<String> expected =
                singletonList(
                        "+I[10.1234, 10.123456787, 72, 18345, 0, 600000000, 2700000000, 412.193, flink test 10, flink 1, flink_10, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T05:10:10, 192.168.2.3, 1.3.3, aaa lll ccc ddd eee, false, l5m]");
        assertEquals(expected, actual);
    }

    @Test
    public void shouldFilterByKeywordColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual =
                executeAndCollect(
                        "SELECT * FROM " + INPUT_TABLE + " WHERE keyword_col = 'flink test 14'");

        List<String> expected =
                singletonList(
                        "+I[14.1234, 14.123456787, 103, 29345, 400000, 1800000000, 3900000000, 698.024, flink test 14, flink 1, flink_14, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T17:50:10, 192.168.3.3, 1.4.0, ppp bbb ccc ddd eee, false, zxcvf]");
        assertEquals(expected, actual);
    }

    @Test
    public void shouldFilterByConstantKeywordColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> results =
                executeAndCollect(
                        "SELECT * FROM " + INPUT_TABLE + " WHERE constant_keyword_col = 'flink 1'");

        assertEquals(15, results.size());
    }

    @Test
    public void shouldFilterByWildcardColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> results =
                executeAndCollect(
                        "SELECT * FROM " + INPUT_TABLE + " WHERE wildcard_col LIKE 'flink_1%'");

        assertEquals(7, results.size());
    }

    @Test
    public void shouldFilterByIpColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> results =
                executeAndCollect(
                        "SELECT * FROM " + INPUT_TABLE + " WHERE ip_col LIKE '192.168.2.%'");

        assertEquals(4, results.size());
    }

    @Test
    public void shouldFilterByVersionColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> results =
                executeAndCollect(
                        "SELECT * FROM "
                                + INPUT_TABLE
                                + " WHERE version_col >= '1.3.4' AND version_col < '2.0.0'");

        assertEquals(4, results.size());
    }

    @Test
    public void shouldFilterByTextColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual =
                executeAndCollect(
                        "SELECT * FROM "
                                + INPUT_TABLE
                                + " WHERE text_col LIKE 'aaa nnn ccc ddd eee'");

        List<String> expected =
                singletonList(
                        "+I[12.1234, 12.123456787, 90, 23345, 200000, 1200000000, 3300000000, 856.835, flink test 12, flink 1, flink_12, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T11:30:10, 192.168.3.1, 1.3.5, aaa nnn ccc ddd eee, false, 6ad]");
        assertEquals(expected, actual);
    }

    @Test
    public void shouldFilterByBooleanColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> results =
                executeAndCollect("SELECT * FROM " + INPUT_TABLE + " WHERE boolean_col");

        assertEquals(7, results.size());
    }

    /*
     * Here is some description of the logic behind a multifield type.
     * If we select from a table with multifield column without a WHERE clause we will get a row
     * with the first value in multifield. However, we can query records with WHERE clause
     * specifying the value that should be looked for. Here is the document describing the logic in
     * more detail:
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-data-types.html#sql-multi-field
     */
    @Test
    public void shouldFilterByTextMultifieldColumn() {
        createNonPartitionedTable(INPUT_TABLE);

        List<String> actual1 =
                executeAndCollect(
                        "SELECT * FROM " + INPUT_TABLE + " WHERE text_multifield_col = 'asd'");

        List<String> actual2 =
                executeAndCollect(
                        "SELECT * FROM " + INPUT_TABLE + " WHERE text_multifield_col = 'dfg'");

        List<String> expected1 =
                singletonList(
                        "+I[11.1234, 11.123456787, 81, 21345, 100000, 900000000, 3000000000, 678.456, flink test 11, flink 1, flink_11, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T09:20:10, 192.168.2.4, 1.3.4, aaa mmm ccc ddd eee, true, asd]");
        List<String> expected2 =
                singletonList(
                        "+I[11.1234, 11.123456787, 81, 21345, 100000, 900000000, 3000000000, 678.456, flink test 11, flink 1, flink_11, U29tZSBiaW5hcnkgYmxvYg==, 2015-01-03T09:20:10, 192.168.2.4, 1.3.4, aaa mmm ccc ddd eee, true, dfg]");

        assertEquals(expected1, actual1);
        assertEquals(expected2, actual2);
    }

    private void createNonPartitionedTable(String tableName) {
        String createTableStatement =
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "float_col FLOAT,"
                        + "double_col DOUBLE,"
                        + "byte_col TINYINT,"
                        + "short_col SMALLINT,"
                        + "integer_col INT,"
                        + "long_col BIGINT,"
                        + "unsigned_long_col BIGINT,"
                        + "scaled_float_col DOUBLE,"
                        + "keyword_col VARCHAR,"
                        + "constant_keyword_col VARCHAR,"
                        + "wildcard_col VARCHAR,"
                        + "binary_col VARCHAR," // binary type has to be declared as varchar
                        + "date_col TIMESTAMP,"
                        + "ip_col VARCHAR,"
                        + "version_col VARCHAR,"
                        + "text_col VARCHAR,"
                        + "boolean_col BOOLEAN,"
                        + "text_multifield_col VARCHAR"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + container.getJdbcUrl()
                        + "',"
                        + "  'table-name'='"
                        + tableName
                        + "',"
                        + "'username'='"
                        + USERNAME
                        + "',"
                        + "'password'='"
                        + PASSWORD
                        + "')";

        tEnv.executeSql(createTableStatement);
    }

    private void createPartitionedTable(String tableName) {
        String sqlString =
                "CREATE TABLE "
                        + tableName
                        + "("
                        + "float_col FLOAT,"
                        + "double_col DOUBLE,"
                        + "byte_col TINYINT,"
                        + "short_col SMALLINT,"
                        + "integer_col INT,"
                        + "long_col BIGINT,"
                        + "unsigned_long_col BIGINT,"
                        + "scaled_float_col DOUBLE,"
                        + "keyword_col VARCHAR,"
                        + "constant_keyword_col VARCHAR,"
                        + "wildcard_col VARCHAR,"
                        + "binary_col VARCHAR," // binary type has to be declared as varchar
                        + "date_col TIMESTAMP,"
                        + "ip_col VARCHAR,"
                        + "version_col VARCHAR,"
                        + "text_col VARCHAR,"
                        + "boolean_col BOOLEAN,"
                        + "text_multifield_col VARCHAR"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + container.getJdbcUrl()
                        + "',"
                        + "  'table-name'='"
                        + tableName
                        + "',"
                        + "'username'='"
                        + USERNAME
                        + "',"
                        + "'password'='"
                        + PASSWORD
                        + "',"
                        + "  'scan.partition.column'='integer_col',"
                        + "  'scan.partition.num'='5',"
                        + "  'scan.partition.lower-bound'='-456781',"
                        + "  'scan.partition.upper-bound'='500000')";

        tEnv.executeSql(sqlString);
    }

    private List<String> executeAndCollect(String query) {
        return collectResults(tEnv.executeSql(query).collect());
    }

    private List<String> collectResults(Iterator<Row> queryResults) {
        return CollectionUtil.iteratorToList(queryResults).stream()
                .map(this::convertDateColToUtc)
                .map(Row::toString)
                .sorted()
                .collect(toList());
    }

    private Row convertDateColToUtc(Row element) {
        LocalDateTime ldt = (LocalDateTime) element.getField("date_col");
        ZonedDateTime utcZoned =
                ldt.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneId.of("UTC"));
        element.setField("date_col", utcZoned.toLocalDateTime());
        return element;
    }
}
