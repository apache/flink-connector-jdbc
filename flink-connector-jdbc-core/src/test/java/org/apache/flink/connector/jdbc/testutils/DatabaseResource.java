package org.apache.flink.connector.jdbc.testutils;

import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;

/** Database resource for testing. */
public interface DatabaseResource extends CloseableResource {

    void start();

    void stop();

    default void close() throws Throwable {
        stop();
    }
}
