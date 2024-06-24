package org.apache.flink.connector.jdbc.datasource.connections.xa;

import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.XaError;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

class XaCommand<T> {

    private static final Logger LOG = LoggerFactory.getLogger(XaCommand.class);
    private final String name;
    private final Xid xid;
    private final Callable<T> callable;
    private final Function<XAException, Optional<T>> recover;

    public XaCommand(String name, Xid xid, Callable<T> callable) {
        this(name, xid, callable, e -> Optional.empty());
    }

    private XaCommand(
            String name,
            Xid xid,
            Callable<T> callable,
            Function<XAException, Optional<T>> recover) {
        this.name = name;
        this.xid = xid;
        this.callable = callable;
        this.recover = recover;
    }

    static XaCommand<Object> fromRunnable(
            String action, Xid xid, ThrowingRunnable<XAException> runnable) {
        return fromRunnable(
                action,
                xid,
                runnable,
                e -> {
                    throw XaError.wrapException(action, xid, e);
                });
    }

    static XaCommand<Object> fromRunnableRecoverByWarn(
            String action,
            Xid xid,
            ThrowingRunnable<XAException> runnable,
            Function<XAException, Optional<String>> err2msg) {
        return fromRunnable(
                action,
                xid,
                runnable,
                e ->
                        LOG.warn(
                                XaError.errorMessage(
                                        action,
                                        xid,
                                        e.errorCode,
                                        err2msg.apply(e)
                                                .orElseThrow(
                                                        () ->
                                                                XaError.wrapException(
                                                                        action, xid, e)))));
    }

    public static XaCommand<Object> fromRunnable(
            String action,
            Xid xid,
            ThrowingRunnable<XAException> runnable,
            Consumer<XAException> recover) {
        return new XaCommand<>(
                action,
                xid,
                () -> {
                    runnable.run();
                    return null;
                },
                e -> {
                    recover.accept(e);
                    return Optional.of("");
                });
    }

    public Optional<Xid> getXid() {
        return Optional.ofNullable(xid);
    }

    public String getName() {
        return name;
    }

    public T execute() throws Exception {
        LOG.debug("{}, xid={}", this.name, this.xid);
        T result = this.callable.call();
        LOG.trace("{} succeeded , xid={}", this.name, this.xid);
        return result;
    }

    public T recover(XAException e) {
        return this.recover
                .apply(e)
                .orElseThrow(() -> XaError.wrapException(this.name, this.xid, e));
    }
}
