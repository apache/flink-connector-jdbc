package org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.FlinkRuntimeException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** Utility to wrap and transform XA errors and messages. */
@Internal
public class XaError {

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
                        XaError.errorMessage(action, xid, xa.errorCode, xa.getMessage()));
            }
        } else {
            throw new FlinkRuntimeException(XaError.errorMessage(action, xid, ex.getMessage()), ex);
        }
    }

    public static Optional<String> buildCommitErrorDesc(XAException err, boolean ignoreUnknown) {
        if (err.errorCode == XAException.XA_HEURCOM) {
            return Optional.of("transaction was heuristically committed earlier");
        } else if (ignoreUnknown && err.errorCode == XAException.XAER_NOTA) {
            return Optional.of("transaction is unknown to RM (ignoring)");
        } else {
            return Optional.empty();
        }
    }

    public static Optional<String> buildRollbackErrorDesc(XAException err) {
        if (err.errorCode == XAException.XA_HEURRB) {
            return Optional.of("transaction was already heuristically rolled back");
        } else if (err.errorCode >= XAException.XA_RBBASE) {
            return Optional.of("transaction was already marked for rollback");
        } else {
            return Optional.empty();
        }
    }

    public static String errorMessage(String action, Xid xid, String... more) {
        return errorMessage(action, xid, null, more);
    }

    public static String errorMessage(String action, Xid xid, Integer code, String... more) {
        String message =
                String.join(
                        ", ",
                        actionErrorMessage(action),
                        xidErrorMessage(xid),
                        codeErrorMessage(code));

        return String.join(". ", message, moreErrorMessage(more));
    }

    private static String actionErrorMessage(String action) {
        if (action == null) {
            return "";
        }
        return String.format("Unable to %s", action);
    }

    private static String xidErrorMessage(Xid xid) {
        if (xid == null) {
            return "";
        }
        return String.format("XA transaction with xid %s", xid);
    }

    private static String codeErrorMessage(Integer errorCode) {
        if (errorCode == null) {
            return "";
        }
        return String.format("Error %d: %s", errorCode, translateCode(errorCode));
    }

    private static String moreErrorMessage(String... more) {
        if (more == null || more.length == 0) {
            return "";
        }
        return Arrays.toString(more);
    }

    /** @return error description from {@link XAException} javadoc from to ease debug. */
    private static String translateCode(int code) {
        switch (code) {
            case XAException.XA_HEURCOM:
                return "heuristic commit decision was made";
            case XAException.XA_HEURHAZ:
                return "heuristic decision may have been made";
            case XAException.XA_HEURMIX:
                return "heuristic mixed decision was made";
            case XAException.XA_HEURRB:
                return "heuristic rollback decision was made";
            case XAException.XA_NOMIGRATE:
                return "the transaction resumption must happen where the suspension occurred";
            case XAException.XA_RBCOMMFAIL:
                return "rollback happened due to a communications failure";
            case XAException.XA_RBDEADLOCK:
                return "rollback happened because deadlock was detected";
            case XAException.XA_RBINTEGRITY:
                return "rollback happened because an internal integrity check failed";
            case XAException.XA_RBOTHER:
                return "rollback happened for some reason not fitting any of the other rollback error codes";
            case XAException.XA_RBPROTO:
                return "rollback happened due to a protocol error in the resource manager";
            case XAException.XA_RBROLLBACK:
                return "rollback happened for an unspecified reason";
            case XAException.XA_RBTIMEOUT:
                return "rollback happened because of a timeout";
            case XAException.XA_RBTRANSIENT:
                return "rollback happened due to a transient failure";
            case XAException.XA_RDONLY:
                return "the transaction branch was read-only, and has already been committed";
            case XAException.XA_RETRY:
                return "the method invoked returned without having any effect, and that it may be invoked again";
            case XAException.XAER_ASYNC:
                return "an asynchronous operation is outstanding";
            case XAException.XAER_DUPID:
                return "Xid given as an argument is already known to the resource manager";
            case XAException.XAER_INVAL:
                return "invalid arguments were passed";
            case XAException.XAER_NOTA:
                return "Xid is not valid";
            case XAException.XAER_OUTSIDE:
                return "the resource manager is doing work outside the global transaction";
            case XAException.XAER_PROTO:
                return "protocol error";
            case XAException.XAER_RMERR:
                return "resource manager error has occurred";
            case XAException.XAER_RMFAIL:
                return "the resource manager has failed and is not available";
            default:
                return "";
        }
    }
}
