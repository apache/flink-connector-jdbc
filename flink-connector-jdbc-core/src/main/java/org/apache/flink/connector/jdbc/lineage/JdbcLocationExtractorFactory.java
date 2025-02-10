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

package org.apache.flink.connector.jdbc.lineage;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A factory to create a specific {@link JdbcLocationExtractor}. This factory is used with Java's
 * Service Provider Interfaces (SPI) for discovering.
 *
 * <p>Classes that implement this interface can be added to the
 * "META_INF/services/org.apache.flink.connector.jdbc.core.lineage.JdbcLocationExtractorFactory"
 * file of a JAR file in the current classpath to be found.
 *
 * @see JdbcLocationExtractor
 */
@PublicEvolving
public interface JdbcLocationExtractorFactory {

    JdbcLocationExtractor createExtractor();
}
