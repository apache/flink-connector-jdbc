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

package org.apache.flink.connector.jdbc.databases;

/**
 * Utility class for defining the image names and versions of Docker containers used during the Java
 * tests. The names/versions are centralised here in order to make testing version updates easier,
 * as well as to provide a central file to use as a key when caching testing Docker files.
 */
public class DockerImageVersions {

    public static final String MSSQL_SERVER_2019 =
            "mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04";

    public static final String MSSQL_SERVER = MSSQL_SERVER_2019;

    public static final String MYSQL_5_6 = "mysql:5.6.51";
    public static final String MYSQL_5_7 = "mysql:5.7.41";
    public static final String MYSQL_8_0 = "mysql:8.0.32";
    public static final String MYSQL = MYSQL_8_0;

    public static final String ORACLE_18 = "gvenzl/oracle-xe/18.4.0-slim-faststart";
    public static final String ORACLE_21 = "gvenzl/oracle-xe:21.3.0-slim-faststart";
    public static final String ORACLE = ORACLE_21;

    public static final String POSTGRES_9 = "postgres:9.6.24";
    public static final String POSTGRES_15 = "postgres:15.1";
    public static final String POSTGRES = POSTGRES_15;
}
