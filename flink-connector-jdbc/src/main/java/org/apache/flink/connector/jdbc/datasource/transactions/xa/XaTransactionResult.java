package org.apache.flink.connector.jdbc.datasource.transactions.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.TransientXaException;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** The result of the XA transaction. */
@Internal
public class XaTransactionResult<T> {
    private final List<T> succeeded = new ArrayList<>();
    private final List<T> failed = new ArrayList<>();
    private final List<T> toRetry = new ArrayList<>();
    private Optional<Exception> failure = Optional.empty();
    private Optional<Exception> transientFailure = Optional.empty();

    void failedTransiently(T x, TransientXaException e) {
        toRetry.add(x);
        transientFailure =
                getTransientFailure().isPresent() ? getTransientFailure() : Optional.of(e);
    }

    void failed(T x, Exception e) {
        failed.add(x);
        failure = failure.isPresent() ? failure : Optional.of(e);
    }

    void succeeded(T x) {
        succeeded.add(x);
    }

    private FlinkRuntimeException wrapFailure(
            Exception error, String formatWithCounts, int errCount) {
        return new FlinkRuntimeException(String.format(formatWithCounts, errCount, total()), error);
    }

    private int total() {
        return succeeded.size() + failed.size() + toRetry.size();
    }

    public List<T> getForRetry() {
        return toRetry;
    }

    Optional<Exception> getTransientFailure() {
        return transientFailure;
    }

    boolean hasNoFailures() {
        return !failure.isPresent() && !transientFailure.isPresent();
    }

    void throwIfAnyFailed(String action) {
        failure.map(
                        f ->
                                wrapFailure(
                                        f,
                                        "failed to " + action + " %d transactions out of %d",
                                        toRetry.size() + failed.size()))
                .ifPresent(
                        f -> {
                            throw f;
                        });
    }
}
