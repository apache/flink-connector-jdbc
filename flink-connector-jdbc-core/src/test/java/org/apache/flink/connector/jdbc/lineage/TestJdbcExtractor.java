package org.apache.flink.connector.jdbc.lineage;

import io.openlineage.client.utils.jdbc.GenericJdbcExtractor;
import io.openlineage.client.utils.jdbc.JdbcLocation;
import io.openlineage.client.utils.jdbc.OverridingJdbcExtractor;

import java.net.URISyntaxException;
import java.util.Properties;

/** Implementation of {@link io.openlineage.client.utils.jdbc.JdbcExtractor} for test purpose. */
public class TestJdbcExtractor extends GenericJdbcExtractor {
    private OverridingJdbcExtractor delegate;

    public TestJdbcExtractor() {
        this.delegate = new OverridingJdbcExtractor("test", "10051");
    }

    public boolean isDefinedAt(String jdbcUri) {
        return this.delegate.isDefinedAt(jdbcUri);
    }

    public JdbcLocation extract(String rawUri, Properties properties) throws URISyntaxException {
        return this.delegate.extract(rawUri, properties);
    }
}
