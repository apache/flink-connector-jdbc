package org.apache.flink.connector.jdbc.dialect.clickhouse;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.internal.converter.ClickhouseRowConvert;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/** */
public class ClickhouseDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to Mysql docs:
    // https://dev.mysql.com/doc/refman/8.0/en/fractional-seconds.html
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    // Define MAX/MIN precision of DECIMAL type according to Mysql docs:
    // https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
    private static final int MAX_DECIMAL_PRECISION = 65;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public AbstractJdbcRowConverter getRowConverter(RowType rowType) {
        return new ClickhouseRowConvert(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.clickhouse.jdbc.ClickHouseDriver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    @Override
    public String dialectName() {
        return "Clickhouse";
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        // LegacyTypeInfoDataTypeConverter.
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
    }
}
