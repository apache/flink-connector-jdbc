/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.testutils.databases.sqlserver;

import org.testcontainers.utility.DockerImageName;

/** SqlServer docker images. */
public interface SqlServerImages {
    // When running MSSQL tests on Mac M1/M2s, use MSSQL_AZURE_SQL_EDGE instead of MSSQL_SERVER_2019
    DockerImageName MSSQL_AZURE_SQL_EDGE =
            DockerImageName.parse("mcr.microsoft.com/azure-sql-edge")
                    .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server");

    String MSSQL_SERVER_2019 = "mcr.microsoft.com/mssql/server:2019-CU20-ubuntu-20.04";
}
