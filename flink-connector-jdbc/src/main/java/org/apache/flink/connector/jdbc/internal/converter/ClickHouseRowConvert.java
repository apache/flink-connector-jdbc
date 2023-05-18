package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedShort;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * ClickHouse.
 */
public class ClickHouseRowConvert extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return "ClickHouse";
    }

    public ClickHouseRowConvert(RowType rowType) {
        super(rowType);
    }

    @Override
    protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return val -> val;
            case TINYINT:
                return val -> ((Byte) val).byteValue();
            case SMALLINT:
                return val ->
                        val instanceof UnsignedByte
                                ? ((UnsignedByte) val).shortValue()
                                : ((Short) val).shortValue();
            case INTEGER:
                return val ->
                        val instanceof UnsignedShort
                                ? ((UnsignedShort) val).intValue()
                                : ((Integer) val).intValue();
            case BIGINT:
                return jdbcField -> {
                    if (jdbcField instanceof UnsignedInteger) {
                        return ((UnsignedInteger) jdbcField).longValue();
                    } else if (jdbcField instanceof Long) {
                        return ((Long) jdbcField).longValue();
                    }
                    // UINT64 is not supported,the uint64 range exceeds the long range
                    throw new UnsupportedOperationException("Unsupported type:" + type);
                };
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString((String) val);
            case DATE:
                return val -> Long.valueOf(((LocalDate) val).toEpochDay()).intValue();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val -> TimestampData.fromLocalDateTime((LocalDateTime) val);
            default:
                return super.createInternalConverter(type);
        }
    }
}
