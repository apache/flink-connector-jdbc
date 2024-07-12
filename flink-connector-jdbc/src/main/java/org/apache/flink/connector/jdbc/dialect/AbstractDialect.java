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

package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;

/**
 * Base class for {@link JdbcDialect JdbcDialects} that implements basic data type validation and
 * the construction of basic {@code INSERT}, {@code UPDATE}, {@code DELETE}, and {@code SELECT}
 * statements.
 *
 * <p>Implementors should be careful to check the default SQL statements are performant for their
 * specific dialect and override them if necessary.
 *
 * @deprecated Use {@link org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect}
 */
@Deprecated
@PublicEvolving
public abstract class AbstractDialect
        extends org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect
        implements org.apache.flink.connector.jdbc.dialect.JdbcDialect {}
