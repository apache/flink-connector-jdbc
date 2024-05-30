package org.apache.flink.connector.jdbc.core.database.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.Catalog;

import java.io.Serializable;

/** Catalogs for relational databases via JDBC. */
@PublicEvolving
public interface JdbcCatalog extends Catalog, Serializable {}
