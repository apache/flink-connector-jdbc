package org.apache.flink.connector.jdbc;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.mysql.MySqlDialectFactory;
import org.apache.flink.connector.jdbc.dialect.oracle.OracleDialectFactory;
import org.apache.flink.connector.jdbc.dialect.psql.PostgresDialectFactory;

import java.util.HashMap;
import java.util.Map;

public class RdbDialects {

    private static final Map<String, JdbcDialect> jdbcDialectMap = new HashMap<String, JdbcDialect>() {
        {
            put("jdbc:oracle", new OracleDialectFactory().create());
            put("jdbc:mysql", new MySqlDialectFactory().create());
            put("jdbc:postgres", new PostgresDialectFactory().create());
        }
    };

    public static JdbcDialect get(String url) throws RuntimeException {
        for (Map.Entry<String, JdbcDialect> item : jdbcDialectMap.entrySet()) {
            String key = item.getKey();
            if (url.contains(key)) {
                return jdbcDialectMap.get(url);
            }
        }
        throw new RuntimeException("当前数据源不支持分库分表!");
    }
}
