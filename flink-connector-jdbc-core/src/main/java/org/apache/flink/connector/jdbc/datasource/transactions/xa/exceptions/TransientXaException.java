package org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.FlinkRuntimeException;

import javax.transaction.xa.XAException;

/**
 * Indicates a transient or unknown failure from the resource manager (see {@link
 * XAException#XA_RBTRANSIENT XA_RBTRANSIENT}, {@link XAException#XAER_RMFAIL XAER_RMFAIL}).
 */
@PublicEvolving
public class TransientXaException extends FlinkRuntimeException {
    public TransientXaException(XAException cause) {
        super(cause);
    }
}
