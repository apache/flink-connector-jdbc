package org.apache.flink.connector.jdbc.databases.clickhouse;

import org.apache.flink.connector.jdbc.databases.DatabaseMetadata;

import org.testcontainers.containers.ClickHouseContainer;

import javax.sql.XADataSource;

/** clickhouse metadata */
public class ClickHouseMetadata implements DatabaseMetadata {

    private final String username;
    private final String password;
    private final String url;
    private final String driver;
    private final String version;
    private final boolean xaEnabled;

    public ClickHouseMetadata(ClickHouseContainer container) {
        this(container, false);
    }

    public ClickHouseMetadata(ClickHouseContainer container, boolean hasXaEnabled) {
        this.username = container.getUsername();
        this.password = container.getPassword();
        this.url = container.getJdbcUrl();
        this.driver = container.getDriverClassName();
        this.version = container.getDockerImageName();
        this.xaEnabled = hasXaEnabled;
    }

    @Override
    public String getJdbcUrl() {
        return this.url;
    }

    @Override
    public String getJdbcUrlWithCredentials() {
        return String.format("%s?user=%s&password=%s", getJdbcUrl(), getUsername(), getPassword());
    }

    @Override
    public String getUsername() {
        return this.username;
    }

    @Override
    public String getPassword() {
        return this.password;
    }

    @Override
    public XADataSource buildXaDataSource() {
        return null;
    }

    @Override
    public String getDriverClass() {
        return this.driver;
    }

    @Override
    public String getVersion() {
        return this.version;
    }
}
