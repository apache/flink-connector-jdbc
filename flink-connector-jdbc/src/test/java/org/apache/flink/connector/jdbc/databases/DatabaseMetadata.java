package org.apache.flink.connector.jdbc.databases;

import org.apache.flink.connector.jdbc.DbMetadata;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import javax.sql.XADataSource;

import java.io.Serializable;

public interface DatabaseMetadata extends Serializable, DbMetadata {

    default String getUrl(){
        return getJdbcUrl();
    }

    default String getUser() {
        return getUsername();
    }

    String getJdbcUrl();

    String getUsername();

    String getPassword();

    XADataSource buildXaDataSource();

    String getDriverClass();

    String getVersion();

    default JdbcConnectionOptions toConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(getDriverClass())
                .withUrl(getJdbcUrl())
                .withUsername(getUsername())
                .withPassword(getPassword())
                .build();
    }
}