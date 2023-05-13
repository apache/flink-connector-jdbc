package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

/** */
public class ClickhouseRowConvert extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "Clickhouse";
    }

    public ClickhouseRowConvert(RowType rowType) {
        super(rowType);
    }
}
