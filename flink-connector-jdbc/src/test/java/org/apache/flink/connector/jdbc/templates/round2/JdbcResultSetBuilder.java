package org.apache.flink.connector.jdbc.templates.round2;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/** ResultSet builder. * */
@FunctionalInterface
public interface JdbcResultSetBuilder<T> extends Serializable {
    List<T> accept(ResultSet rs) throws SQLException;
}
