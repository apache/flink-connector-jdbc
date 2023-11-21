package org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions;

import org.apache.flink.util.FlinkRuntimeException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** */
public class XaException {
    private static final Set<Integer> TRANSIENT_ERR_CODES =
            new HashSet<>(Arrays.asList(XAException.XA_RBTRANSIENT, XAException.XAER_RMFAIL));

    private static final Set<Integer> HEUR_ERR_CODES =
            new HashSet<>(
                    Arrays.asList(
                            XAException.XA_HEURRB,
                            XAException.XA_HEURCOM,
                            XAException.XA_HEURHAZ,
                            XAException.XA_HEURMIX));

    public static boolean isHeurErrorCode(int errorCode) {
        return HEUR_ERR_CODES.contains(errorCode);
    }

    public static FlinkRuntimeException wrapException(String action, Exception ex) {
        return wrapException(action, null, ex);
    }

    public static FlinkRuntimeException wrapException(String action, Xid xid, Exception ex) {
        if (ex instanceof XAException) {
            XAException xa = (XAException) ex;
            if (TRANSIENT_ERR_CODES.contains(xa.errorCode)) {
                throw new TransientXaException(xa);
            } else {
                throw new FlinkRuntimeException(
                        XaErrorMessage.errorMessage(action, xid, xa.errorCode, xa.getMessage()));
            }
        } else {
            throw new FlinkRuntimeException(
                    XaErrorMessage.errorMessage(action, xid, ex.getMessage()), ex);
        }
    }
}
