package org.apache.flink.connector.jdbc.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.XaTransaction;

import javax.annotation.Nullable;
import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.util.Optional;

/** A pair of Xid and transaction that can be committed. */
@Internal
public class JdbcCommitable implements Serializable {

    private final Xid xid;
    private final XaTransaction transaction;

    protected JdbcCommitable(Xid xid, @Nullable XaTransaction transaction) {
        this.xid = xid;
        this.transaction = transaction;
    }

    public static JdbcCommitable of(Xid xid) {
        return of(xid, null);
    }

    public static JdbcCommitable of(Xid xid, XaTransaction transaction) {
        return new JdbcCommitable(xid, transaction);
    }

    public Xid getXid() {
        return xid;
    }

    public Optional<XaTransaction> getTransaction() {
        return Optional.ofNullable(transaction);
    }
}
