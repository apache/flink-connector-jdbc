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

package org.apache.flink.connector.jdbc.oceanbase.database.catalog;

import org.apache.flink.connector.jdbc.oceanbase.OceanBaseOracleTestBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Disabled;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.oceanbase.OceanBaseOracleTestBase.tableRow;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.dbType;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.field;
import static org.apache.flink.connector.jdbc.testutils.tables.TableBuilder.pkField;

/** E2E test for {@link OceanBaseCatalog} with OceanBase Oracle mode. */
@Disabled("OceanBase Oracle mode can only be tested locally.")
public class OceanBaseOracleCatalogITCase extends OceanBaseCatalogITCaseBase
        implements OceanBaseOracleTestBase {

    private static final String SCHEMA = "SYS";

    private static final TableRow TABLE_ALL_TYPES = createTableAllTypeTable("T_ALL_TYPES");
    private static final TableRow TABLE_ALL_TYPES_SINK =
            createTableAllTypeTable("T_ALL_TYPES_SINK");

    private static final List<Row> TABLE_ALL_TYPES_ROWS =
            Arrays.asList(
                    Row.of(
                            1L,
                            BigDecimal.valueOf(100.1234),
                            "1.12345",
                            1.175E-10F,
                            1.79769E+40D,
                            "a",
                            "abc",
                            "abcdef",
                            LocalDate.parse("1997-01-01"),
                            LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                            LocalDateTime.parse("2020-01-01T15:35:00.123456789"),
                            "Hello World"),
                    Row.of(
                            2L,
                            BigDecimal.valueOf(101.1234),
                            "1.12345",
                            1.175E-10F,
                            1.79769E+40D,
                            "a",
                            "abc",
                            "abcdef",
                            LocalDate.parse("1997-01-02"),
                            LocalDateTime.parse("2020-01-01T15:36:01.123456"),
                            LocalDateTime.parse("2020-01-01T15:36:01.123456789"),
                            "Hey Leonard"));

    private static TableRow createTableAllTypeTable(String tableName) {
        return tableRow(
                tableName,
                pkField("ID", dbType("NUMBER(18,0)"), DataTypes.BIGINT().notNull()),
                field("DECIMAL_COL", dbType("NUMBER(10,4)"), DataTypes.DECIMAL(10, 4)),
                field("FLOAT_COL", dbType("FLOAT"), DataTypes.STRING()),
                field("BINARY_FLOAT_COL", dbType("BINARY_FLOAT"), DataTypes.FLOAT()),
                field("BINARY_DOUBLE_COL", dbType("BINARY_DOUBLE"), DataTypes.DOUBLE()),
                field("CHAR_COL", dbType("CHAR"), DataTypes.CHAR(1)),
                field("NCHAR_COL", dbType("NCHAR(3)"), DataTypes.CHAR(3)),
                field("VARCHAR2_COL", dbType("VARCHAR2(30)"), DataTypes.VARCHAR(30)),
                field("DATE_COL", dbType("DATE"), DataTypes.DATE()),
                field("TIMESTAMP6_COL", dbType("TIMESTAMP"), DataTypes.TIMESTAMP(6)),
                field("TIMESTAMP9_COL", dbType("TIMESTAMP(9)"), DataTypes.TIMESTAMP(9)),
                field("CLOB_COL", dbType("CLOB"), DataTypes.STRING()));
    }

    public OceanBaseOracleCatalogITCase() {
        super("oceanbase_oracle_catalog", "oracle", SCHEMA);
    }

    @Override
    protected TableRow allTypesSourceTable() {
        return TABLE_ALL_TYPES;
    }

    @Override
    protected TableRow allTypesSinkTable() {
        return TABLE_ALL_TYPES_SINK;
    }

    @Override
    protected List<Row> allTypesTableRows() {
        return TABLE_ALL_TYPES_ROWS;
    }

    @Override
    public List<TableManaged> getManagedTables() {
        return Arrays.asList(TABLE_ALL_TYPES, TABLE_ALL_TYPES_SINK);
    }
}
