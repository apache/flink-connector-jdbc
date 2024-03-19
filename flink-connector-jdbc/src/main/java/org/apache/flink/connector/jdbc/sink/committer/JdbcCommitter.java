package org.apache.flink.connector.jdbc.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.XaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.XaTransaction;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.sink.writer.JdbcWriterState;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Collection;

/** The responsible for committing the {@link JdbcCommitable}. */
@Internal
public class JdbcCommitter implements Committer<JdbcCommitable> {

    private transient XaTransaction jdbcTransaction;

    private final DeliveryGuarantee deliveryGuarantee;

    public JdbcCommitter(
            DeliveryGuarantee deliveryGuarantee,
            JdbcConnectionProvider connectionProvider,
            JdbcExactlyOnceOptions exactlyOnceOptions)
            throws IOException {

        if (DeliveryGuarantee.EXACTLY_ONCE == deliveryGuarantee) {
            this.jdbcTransaction =
                    new XaTransaction(
                            exactlyOnceOptions,
                            TransactionId.empty(),
                            ((XaConnectionProvider) connectionProvider));
            this.jdbcTransaction.open(JdbcWriterState.empty());
        }

        this.deliveryGuarantee = deliveryGuarantee;
    }

    @Override
    public void commit(Collection<CommitRequest<JdbcCommitable>> committables)
            throws IOException, InterruptedException {

        if (DeliveryGuarantee.EXACTLY_ONCE != this.deliveryGuarantee && !committables.isEmpty()) {
            throw new FlinkRuntimeException("Non XA sink with commitables");
        }

        for (CommitRequest<JdbcCommitable> request : committables) {
            request.getCommittable().getTransaction().orElse(this.jdbcTransaction).commitTx();
        }
    }

    @Override
    public void close() throws Exception {
        if (this.jdbcTransaction != null) {
            this.jdbcTransaction.close();
        }
    }
}
