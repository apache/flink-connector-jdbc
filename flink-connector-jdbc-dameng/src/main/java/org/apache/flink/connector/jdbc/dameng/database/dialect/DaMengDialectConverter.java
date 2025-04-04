/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.dameng.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.sql.Clob;
import java.sql.SQLException;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * MySQL.
 */
@Internal
public class DaMengDialectConverter
        extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "DaMeng";
    }

    public DaMengDialectConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        if (type.getTypeRoot() == LogicalTypeRoot.CHAR
                || type.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
            return val -> {
                if (val == null) {
                    return null;
                } else if (val instanceof String) {
                    String str = (String) val;
                    return StringData.fromString(str.trim());
                } else if (val instanceof Clob || val.getClass().getName().contains("Clob")) {
                    try {
                        Clob clob = (Clob) val;
                        long length = clob.length();
                        if (length > Integer.MAX_VALUE) {
                            // Dealing with very large CLOBs, which may require segmented reads
                            return StringData.fromString(clob.getSubString(1, Integer.MAX_VALUE));
                        }
                        return StringData.fromString(clob.getSubString(1, (int) length));
                    } catch (SQLException e) {
                        throw new RuntimeException("Failed to convert DmdbClob/NClob to String", e);
                    }
                } else {
                    // For other unknown types, try using the toString method
                    return StringData.fromString(val.toString().trim());
                }
            };
        } else if (type.getTypeRoot() == LogicalTypeRoot.TINYINT) {
            return val -> {
                if (val == null) {
                    return null;
                } else if (val instanceof Byte) {
                    return ((Byte) val).byteValue();
                } else if (val instanceof Integer) {
                    return ((Integer) val).byteValue();
                } else {
                    return Byte.parseByte(val.toString());
                }
            };
        } else if (type.getTypeRoot() == LogicalTypeRoot.FLOAT) {
            return val -> {
                if (val == null) {
                    return null;
                } else if (val instanceof Float) {
                    return ((Float) val).floatValue();
                } else if (val instanceof Double) {
                    return ((Double) val).floatValue();
                } else {
                    return Float.parseFloat(val.toString());
                }
            };
        } else {
            return super.createInternalConverter(type);
        }
    }
}
