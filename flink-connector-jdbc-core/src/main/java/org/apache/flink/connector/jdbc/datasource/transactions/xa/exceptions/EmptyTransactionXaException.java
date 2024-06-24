package org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.xa.XaFacade;
import org.apache.flink.util.FlinkRuntimeException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

/**
 * Thrown by {@link XaFacade} when RM responds with {@link javax.transaction.xa.XAResource#XA_RDONLY
 * XA_RDONLY} indicating that the transaction doesn't include any changes. When such a transaction
 * is committed RM may return an error (usually, {@link XAException#XAER_NOTA XAER_NOTA}).
 */
@PublicEvolving
public class EmptyTransactionXaException extends FlinkRuntimeException {
    private final Xid xid;

    public EmptyTransactionXaException(Xid xid) {
        super("end response XA_RDONLY, xid: " + xid);
        this.xid = xid;
    }

    public Xid getXid() {
        return xid;
    }
}
