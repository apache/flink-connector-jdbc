package org.apache.flink.connector.jdbc.sink.writer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.databases.derby.DerbyTestBase;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.statements.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.datasource.statements.SimpleJdbcQueryStatement;
import org.apache.flink.connector.jdbc.internal.JdbcOutputSerializer;
import org.apache.flink.connector.jdbc.sink.committer.JdbcCommitable;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;

/** Base smoke tests for the {@link JdbcWriter} and the underlying classes. */
abstract class BaseJdbcWriterTest implements DerbyTestBase {

    private static final String JOBID = "6b64d8a9a951e2e8767ae952ad951706";
    private static final String GLOBAL_TID =
            String.format("%s000000000000000000000000000000000000", JOBID);
    protected static final BooksTable TEST_TABLE = new BooksTable("WriterTable");

    protected static final List<BooksTable.BookEntry> BOOKS =
            Arrays.stream(TEST_DATA)
                    .map(
                            book ->
                                    new BooksTable.BookEntry(
                                            book.id, book.title, book.author, book.price, book.qty))
                    .collect(Collectors.toList());
    protected JdbcWriter<BooksTable.BookEntry> sinkWriter;

    protected final TestWriterContext writerContext = new TestWriterContext();

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TEST_TABLE);
    }

    protected abstract JdbcExecutionOptions getExecutionOptions();

    protected abstract JdbcExactlyOnceOptions getExactlyOnceOptions();

    protected abstract DeliveryGuarantee getDeliveryGuarantee();

    protected abstract JdbcConnectionProvider getConnectionProvider();

    @BeforeEach
    void init() throws Exception {
        // We have to mock this because we have changes between 1.18 and 1.19
        Sink.InitContext sinkContext = Mockito.mock(Sink.InitContext.class);
        doReturn(JobID.fromHexString(JOBID)).when(sinkContext).getJobId();

        JdbcOutputSerializer<BooksTable.BookEntry> outputSerializer =
                JdbcOutputSerializer.of(
                        sinkContext.createInputSerializer(), sinkContext.isObjectReuseEnabled());

        JdbcQueryStatement<BooksTable.BookEntry> queryStatement =
                new SimpleJdbcQueryStatement<>(
                        TEST_TABLE.getInsertIntoQuery(), TEST_TABLE.getStatementBuilder());

        this.sinkWriter =
                new JdbcWriter<>(
                        getConnectionProvider(),
                        getExecutionOptions(),
                        getExactlyOnceOptions(),
                        queryStatement,
                        outputSerializer,
                        getDeliveryGuarantee(),
                        Collections.emptyList(),
                        sinkContext);
    }

    @AfterEach
    void finish() throws Exception {
        this.sinkWriter.close();
    }

    protected String withBranch(long checkpointId) {
        return String.format("00000000000000000000000%s00", checkpointId);
    }

    protected void checkCommitable(JdbcCommitable actual, String branchExpected) {
        assertThat(actual.getXid()).isNotNull();
        assertThat(StringUtils.byteToHexString(actual.getXid().getGlobalTransactionId()))
                .isEqualTo(GLOBAL_TID);
        assertThat(StringUtils.byteToHexString(actual.getXid().getBranchQualifier()))
                .isEqualTo(branchExpected);
    }

    protected void checkSnapshot(
            JdbcWriterState actual, List<String> prepared, List<String> hanging) {
        assertThat(actual.getPrepared().size()).isEqualTo(prepared.size());

        assertThat(
                        actual.getPrepared().stream()
                                .map(x -> StringUtils.byteToHexString(x.getGlobalTransactionId()))
                                .collect(Collectors.toList()))
                .isEqualTo(prepared);

        assertThat(actual.getHanging().size()).isEqualTo(hanging.size());

        assertThat(
                        actual.getHanging().stream()
                                .map(x -> StringUtils.byteToHexString(x.getGlobalTransactionId()))
                                .collect(Collectors.toList()))
                .isEqualTo(hanging.stream().map(h -> GLOBAL_TID).collect(Collectors.toList()));

        assertThat(
                        actual.getHanging().stream()
                                .map(x -> StringUtils.byteToHexString(x.getBranchQualifier()))
                                .collect(Collectors.toList()))
                .isEqualTo(hanging);
    }

    public static class TestWriterContext implements SinkWriter.Context {
        @Override
        public long currentWatermark() {
            return System.currentTimeMillis();
        }

        @Override
        public Long timestamp() {
            return System.currentTimeMillis();
        }
    }
}
