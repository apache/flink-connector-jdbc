---
title: JDBC
weight: 10
type: docs
aliases:
  - /dev/connectors/jdbc.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# JDBC Connector

This connector provides a source that read data from a JDBC database and
provides a sink that writes data to a JDBC database.

Since version 4.0 the connector is no longer published as a single artifact. It is split into
`flink-connector-jdbc-core`, which contains the source and the sink, and one artifact per supported
database, which contains the dialect and the catalog for that database. Add the artifact for the
database you are connecting to (along with your JDBC driver); it pulls in
`flink-connector-jdbc-core` transitively. For example, for PostgreSQL:

{{< connector_artifact flink-connector-jdbc-postgres jdbc >}}

The available database artifacts are `flink-connector-jdbc-mysql`, `flink-connector-jdbc-oracle`,
`flink-connector-jdbc-postgres`, `flink-connector-jdbc-sqlserver`, `flink-connector-jdbc-cratedb`,
`flink-connector-jdbc-db2`, `flink-connector-jdbc-trino` and `flink-connector-jdbc-oceanbase`. The
Derby dialect ships in `flink-connector-jdbc-core`.

The `JdbcSource` and `JdbcSink` themselves live in `flink-connector-jdbc-core` and work against any
JDBC driver, so a database artifact is only required for the Table/SQL API, where the dialect is used
to generate statements.

Note that the streaming connectors are currently __NOT__ part of the binary distribution.
See how to link with them for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).
A driver dependency is also required to connect to a specified database.
Please consult your database documentation on how to add the corresponding driver.

## JDBC Source

### Usage

```java
JdbcSource<OUT> source = JdbcSource.<OUT>builder()
        // Required
        .setSplitter(...)
        .setResultExtractor(...)
        .setTypeInformation(...)
        .setDBUrl(...)
        .setDriverName(...)
        .setUsername(...)
        .setPassword(...)

        // Optional
        .setDeliveryGuarantee(...)
        .setConnectionCheckTimeoutSeconds(...)

        // The extended JDBC connection property passing
        .setConnectionProperty("key", "value")

        // other attributes
        .setSplitReaderFetchBatchSize(...)
        .setResultSetType(...)
        .setResultSetConcurrency(...)
        .setAutoCommit(...)
        .setResultSetFetchSize(...)
        .setConnectionProvider(...)
        .build();
```

`setSplitter` describes the query and how it is divided into splits, and is the only required way to
define what the source reads. The older `setSql` / `setJdbcParameterValuesProvider` pair is
deprecated in favour of it; see [Deprecated query API](#deprecated-query-api). Setting both a
splitter and a query fails at `build()` time.

### SplitterEnumerator

A `SplitterEnumerator` produces the `JdbcSourceSplit`s that the readers execute, and decides whether
the source is bounded or continuously unbounded.

#### PreparedSplitterEnumerator

`PreparedSplitterEnumerator` builds bounded splits from a query template. With no parameters the
query becomes a single split:

```java
PreparedSplitterEnumerator.of("select * from books");
```

To read in parallel, provide a parameterized query template (i.e. a valid
[JDBC prepared statement](https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/PreparedStatement.html))
together with the binding values. One split is generated per row of the parameter array:

```java
String query = "select * from books where author = ?";
Serializable[][] queryParameters = new String[2][1];
queryParameters[0] = new String[]{"Kumar"};
queryParameters[1] = new String[]{"Tan Ah Teck"};

PreparedSplitterEnumerator.of(query, queryParameters);
```

For a numeric range there are convenience overloads that generate the parameter pairs for you. The
template must take a lower and an upper bound, and the range is divided either into splits of a given
size or into a given number of splits:

```java
// splits of at most 1000 values each
PreparedSplitterEnumerator.of("select * from books where id between ? and ?", 1L, 10_000L, 1000L);

// exactly 10 splits
PreparedSplitterEnumerator.of("select * from books where id between ? and ?", 1L, 10_000L, 10);
```

Note that the two overloads differ only in whether the last argument is a `long` (batch size) or an
`int` (number of batches). The same can be expressed explicitly with
`PreparedSplitterNumericParameters`:

```java
PreparedSplitterEnumerator.of(
        "select * from books where id between ? and ?",
        new PreparedSplitterNumericParameters(1L, 10_000L).withBatchSize(1000L));
```

#### SlideTimingSplitterEnumerator

`SlideTimingSplitterEnumerator` generates splits continuously from a sliding window over a timestamp
column, which makes the source unbounded. The query template takes the window start and end:

```java
SlideTimingSplitterEnumerator.builder()
        .setSqlTemplate("select * from books where ts >= ? and ts < ?")
        .setStartMillis(startMillis)
        .setSlideSpanMillis(1000L)
        .setSlideStepMillis(1000L)
        .setSplitGenerateDelayMillis(100L)
        .build();
```

`setSplitGenerateDelayMillis` holds a window back for the given duration before it is emitted as a
split, which gives late-arriving rows time to land.

### Delivery guarantee

The JDBC source provides `at-least-once`/`at-most-once(default)`/`exactly-once` guarantee.
The `JdbcSource` supports `Delivery guarantee` semantic based on `Concur` of `ResultSet`.

**NOTE:** Here's a few disadvantage. It only makes sense for corresponding semantic
that the `ResultSet` corresponding to this SQL(`JdbcSourceSplit`)
remains unchanged in the whole lifecycle of `JdbcSourceSplit` processing.
Unfortunately, this condition is not met in most databases and data scenarios.
See [FLIP-239](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=217386271) for more details.

Using `DeliveryGuarantee.EXACTLY_ONCE` requires `setResultSetType` to be either
`ResultSet.TYPE_SCROLL_INSENSITIVE` or `ResultSet.CONCUR_READ_ONLY`; other values fail at `build()`
time.

### ResultExtractor

An `Extractor` to extract a record from `ResultSet` executed by a sql.

```java
import org.apache.flink.connector.jdbc.core.datastream.source.reader.extractor.ResultExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;

class Book {
    public Book(Long id, String title) {
        this.id = id;
        this.title = title;
    }

    final Long id;
    final String title;
};

ResultExtractor<Book> resultExtractor = new ResultExtractor<Book>() {
    @Override
    public Book extract(ResultSet resultSet) throws SQLException {
        return new Book(resultSet.getLong("id"), resultSet.getString("title"));
    }
};
```

### Full example

```java
public class JdbcSourceExample {

    static class Book {
        public Book(Long id, String title) {
            this.id = id;
            this.title = title;
        }

        final Long id;
        final String title;
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JdbcSource<Book> jdbcSource =
                JdbcSource.<Book>builder()
                        .setTypeInformation(TypeInformation.of(Book.class))
                        .setSplitter(
                                PreparedSplitterEnumerator.of(
                                        "select * from books where id < ?",
                                        new Serializable[][] {{1001L}}))
                        .setDBUrl(...)
                        .setDriverName(...)
                        .setResultExtractor(resultSet ->
                            new Book(
                                resultSet.getLong("id"),
                                resultSet.getString("title")))
                        .build();
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "TestSource")
                .sinkTo(new DiscardingSink<>());
        env.execute();
    }
}
```

### Deprecated query API

Before `SplitterEnumerator` was introduced, the query was defined with `setSql` and the splits with a
`JdbcParameterValuesProvider`. Both methods are deprecated and are equivalent to a
`PreparedSplitterEnumerator`:

```java
// deprecated
JdbcSource.<TestEntry>builder()
        .setSql("select * from testing_table where id >= ? and id <= ?")
        .setJdbcParameterValuesProvider(
                new JdbcGenericParameterValuesProvider(
                        new Serializable[][] {{1001, 1005}, {1006, 1010}}))
        ...

// current
JdbcSource.<TestEntry>builder()
        .setSplitter(
                PreparedSplitterEnumerator.of(
                        "select * from testing_table where id >= ? and id <= ?",
                        new Serializable[][] {{1001, 1005}, {1006, 1010}}))
        ...
```

On this deprecated path, continuous unbounded reads are configured with
`setContinuousUnBoundingSettings` and a `JdbcSlideTimingParameterProvider`, which must be set
together:

```java
// deprecated
JdbcSource.<TestEntry>builder()
        .setSql("select * from testing_table where ts >= ? and ts < ?")
        .setContinuousUnBoundingSettings(
                new ContinuousUnBoundingSettings(Duration.ofMillis(10L), Duration.ofSeconds(1L)))
        .setJdbcParameterValuesProvider(
                new JdbcSlideTimingParameterProvider(0L, 1000L, 1000L, 100L))
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE);
```

Use [`SlideTimingSplitterEnumerator`](#slidetimingsplitterenumerator) instead.
`setContinuousUnBoundingSettings` has no effect when a splitter is set — a splitter declares its own
boundedness.

## JDBC Sink

The JDBC sink is built with `JdbcSink.builder()`. It provides an at-least-once and an exactly-once
variant, selected by which `build` method you call. Effectively though, exactly-once can also be
achieved on the at-least-once path by crafting upsert SQL statements or idempotent SQL updates.

```java
JdbcSink<IN> sink = JdbcSink.<IN>builder()
        .withQueryStatement(sqlDmlStatement, jdbcStatementBuilder)   // mandatory
        .withExecutionOptions(jdbcExecutionOptions)                  // optional
        .buildAtLeastOnce(jdbcConnectionOptions);                    // mandatory
```

The sink is attached to a stream with `sinkTo`:

```java
stream.sinkTo(sink);
```

### SQL DML statement and JDBC statement builder

The sink builds one [JDBC prepared statement](https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/PreparedStatement.html) from a user-provider SQL string, e.g.:

```sql
INSERT INTO some_table field1, field2 values (?, ?)
```

It then repeatedly calls a user-provided function to update that prepared statement with each value of the stream, e.g.:

```
(preparedStatement, someRecord) -> { ... update here the preparedStatement with values from someRecord ... }
```

The two are passed together to `withQueryStatement`. A `JdbcQueryStatement` can also be supplied
directly if the query itself has to be derived from the record.

### JDBC execution options

The SQL DML statements are executed in batches, which can optionally be configured with the following instance:

```java
JdbcExecutionOptions.builder()
        .withBatchIntervalMs(200)             // optional: default = 0, meaning no time-based execution is done
        .withBatchSize(1000)                  // optional: default = 5000 values
        .withMaxRetries(5)                    // optional: default = 3
        .build();
```

A JDBC batch is executed as soon as one of the following conditions is true:

* the configured batch interval time is elapsed
* the maximum batch size is reached
* a Flink checkpoint has started

### JDBC connection parameters

The connection to the database is configured with a `JdbcConnectionOptions` instance, which is passed
to `buildAtLeastOnce`. A `JdbcConnectionProvider` can be passed instead to reuse an existing
connection strategy.

### Full example

```java
public class JdbcSinkExample {

    static class Book {
        public Book(Long id, String title, String authors, Integer year) {
            this.id = id;
            this.title = title;
            this.authors = authors;
            this.year = year;
        }
        final Long id;
        final String title;
        final String authors;
        final Integer year;
    }

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromData(
                new Book(101L, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
                new Book(102L, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
                new Book(103L, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
                new Book(104L, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
        ).sinkTo(
                JdbcSink.<Book>builder()
                        .withQueryStatement(
                                "insert into books (id, title, authors, year) values (?, ?, ?, ?)",
                                (statement, book) -> {
                                    statement.setLong(1, book.id);
                                    statement.setString(2, book.title);
                                    statement.setString(3, book.authors);
                                    statement.setInt(4, book.year);
                                })
                        .withExecutionOptions(
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(1000)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(5)
                                        .build())
                        .buildAtLeastOnce(
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl("jdbc:postgresql://dbhost:5432/postgresdb")
                                        .withDriverName("org.postgresql.Driver")
                                        .withUsername("someUser")
                                        .withPassword("somePassword")
                                        .build()));

        env.execute();
    }
}
```

### Exactly-once sink

The exactly-once implementation relies on the JDBC driver support of XA
[standard](https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf).
Most drivers support XA if the database also supports XA (so the driver is usually the same).

To use it, call `buildExactlyOnce()` instead of `buildAtLeastOnce()` and provide:
- `JdbcExactlyOnceOptions`
- an [XA DataSource](https://docs.oracle.com/javase/8/docs/api/javax/sql/XADataSource.html) Supplier

For example:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env
        .fromData(...)
        .sinkTo(JdbcSink.<Book>builder()
                .withQueryStatement(
                        "insert into books (id, title, author, price, qty) values (?,?,?,?,?)",
                        (ps, t) -> {
                            ps.setInt(1, t.id);
                            ps.setString(2, t.title);
                            ps.setString(3, t.author);
                            ps.setDouble(4, t.price);
                            ps.setInt(5, t.qty);
                        })
                .withExecutionOptions(
                        JdbcExecutionOptions.builder()
                                .withMaxRetries(0)
                                .build())
                .buildExactlyOnce(
                        JdbcExactlyOnceOptions.defaults(),
                        () -> {
                            // create a driver-specific XA DataSource
                            PGXADataSource ds = new PGXADataSource();
                            ds.setUrl("jdbc:postgresql://localhost:5432/postgres");
                            ds.setUser(username);
                            ds.setPassword(password);
                            return ds;
                        }));
env.execute();
```

An `XaConnectionProvider` can be passed instead of the supplier when the connection strategy has to
be controlled directly.

**NOTE:** Some databases only allow a single XA transaction per connection (e.g. PostgreSQL, MySQL).
In such cases, please use the following API to construct `JdbcExactlyOnceOptions`:

```java
JdbcExactlyOnceOptions.builder()
    .withTransactionPerConnection(true)
    .build();
```

This will make Flink use a separate connection for every XA transaction. This may require adjusting connection limits.
For PostgreSQL and MySQL, this can be done by increasing `max_connections`.

Furthermore, XA needs to be enabled and/or configured in some databases.
For PostgreSQL, you should set `max_prepared_transactions` to some value greater than zero.
For MySQL v8+, you should grant `XA_RECOVER_ADMIN` to Flink DB user.

**ATTENTION:** Currently, the exactly-once sink can ensure exactly once semantics
with `JdbcExecutionOptions.maxRetries == 0`; otherwise, duplicated results maybe produced.

### `XADataSource` examples
PostgreSQL `XADataSource` example:
```java
PGXADataSource xaDataSource = new org.postgresql.xa.PGXADataSource();
xaDataSource.setUrl("jdbc:postgresql://localhost:5432/postgres");
xaDataSource.setUser(username);
xaDataSource.setPassword(password);
```

MySQL `XADataSource` example:
```java
MysqlXADataSource xaDataSource = new com.mysql.cj.jdbc.MysqlXADataSource();
xaDataSource.setUrl("jdbc:mysql://localhost:3306/");
xaDataSource.setUser(username);
xaDataSource.setPassword(password);
```

Oracle `XADataSource` example:
```java
OracleXADataSource xaDataSource = new oracle.jdbc.xa.OracleXADataSource();
xaDataSource.setURL("jdbc:oracle:oci8:@");
xaDataSource.setUser("scott");
xaDataSource.setPassword("tiger");
```

Please also take Oracle connection pooling into account.

## Lineage

`JdbcSource` and `JdbcSink` expose lineage information as described in
[FLIP-314](https://cwiki.apache.org/confluence/display/FLINK/FLIP-314%3A+Support+Customized+Job+Lineage+Listener),
as do the Table API source and lookup function. No configuration is needed on the connector side; the
information is delivered to any `JobStatusChangedListener` registered in the cluster.

The dataset namespace is derived from the JDBC URL and the dataset name from the queries the job
executes.

{{< top >}}
