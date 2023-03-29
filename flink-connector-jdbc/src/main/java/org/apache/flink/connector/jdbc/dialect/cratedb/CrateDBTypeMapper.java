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

package org.apache.flink.connector.jdbc.dialect.cratedb;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.psql.PostgresTypeMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/** CrateDBTypeMapper util class. */
@Internal
public class CrateDBTypeMapper extends PostgresTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(CrateDBTypeMapper.class);

    // CrateDB jdbc driver uses very similar mapping
    // to PostgreSQL driver, and adds some extras:
    private static final String PG_STRING = "string";
    private static final String PG_STRING_ARRAY = "_string";

    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String pgType = metadata.getColumnTypeName(colIndex);

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        DataType dataType = getMapping(pgType, precision, scale);
        if (dataType == null) {
            throw new UnsupportedOperationException(
                    String.format("Doesn't support CrateDB type '%s' yet", pgType));
        }
        return dataType;
    }

    @Override
    protected DataType getMapping(String pgType, int precision, int scale) {
        switch (pgType) {
            case PG_BIGSERIAL:
                return null;
            case PG_STRING:
                return DataTypes.STRING();
            case PG_STRING_ARRAY:
                return DataTypes.ARRAY(DataTypes.STRING());
            default:
                return super.getMapping(pgType, precision, scale);
        }
    }
}
