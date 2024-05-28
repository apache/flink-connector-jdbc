---
title: JDBC
weight: 10
type: docs
aliases:
  - /zh/dev/connectors/jdbc.html
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

To use it, add the following dependency to your project (along with your JDBC driver):

{{< connector_artifact flink-connector-jdbc jdbc >}}

Note that the streaming connectors are currently __NOT__ part of the binary distribution.
See how to link with them for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).
A driver dependency is also required to connect to a specified database.
Please consult your database documentation on how to add the corresponding driver.

## Source of JDBC Connector

Configuration goes as follow (see also {{< javadoc file="org/apache/flink/connector/jdbc/source/JdbcSource.html" name="JdbcSource javadoc" >}}
and {{< javadoc file="org/apache/flink/connector/jdbc/source/JdbcSourceBuilder.html" name="JdbcSourceBuilder javadoc" >}}).

### `JdbcSource.builder`

{{< tabs "4ab65f13-607a-411a-8d24-e709f701cd41" >}}
{{< tab "Java" >}}
```java
JdbcSource source = JdbcSourceBuilder.builder()
        // Required
        .setSql(...)
        .setResultExtractor(...)
        .setUsername(...)
        .setPassword(...)
        .setDriverName(...)
        .setDBUrl(...)
        .setTypeInformation(...)
        
         // Optional
        .setContinuousUnBoundingSettings(...)
        .setJdbcParameterValuesProvider(...)
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
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

#### Delivery guarantee

The JDBC source provides `at-least-once`/`at-most-once(default)`/`exactly-once` guarantee.
The `JdbcSource` supports `Delivery guarantee` semantic based on `Concur` of `ResultSet`.

**NOTE:** Here's a few disadvantage. It only makes sense for corresponding semantic
that the `ResultSet` corresponding to this SQL(`JdbcSourceSplit`)
remains unchanged in the whole lifecycle of `JdbcSourceSplit` processing.
Unfortunately, this condition is not met in most databases and data scenarios.
See [FLIP-239](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=217386271) for more details.

#### ResultExtractor

An `Extractor` to extract a record from `ResultSet` executed by a sql.

```java
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;

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

ResultExtractor resultExtractor = new ResultExtractor() {
    @Override
    public Object extract(ResultSet resultSet) throws SQLException {
        return new Book(resultSet.getLong("id"), resultSet.getString("titile"));
    }
};

```

#### JdbcParameterValuesProvider

A provider to provide parameters in sql to fulfill actual value in the corresponding placeholders, which is in the form of two-dimension array.
See {{< javadoc file="org/apache/flink/connector/jdbc/split/JdbcParameterValuesProvider.html" name="JdbcParameterValuesProvider javadoc" >}} for more details.

```java

class TestEntry {
    ...
};

ResultSetExtractor extractor = ...;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

JdbcSource<TestEntry> jdbcSource =
        JdbcSource.<TestEntry>builder()
                .setTypeInformation(TypeInformation.of(TestEntry.class))
                .setSql("select * from testing_table where id >= ? and id <= ?")
                .setJdbcParameterValuesProvider(
                        new JdbcGenericParameterValuesProvider(
                                new Serializable[][] {{1001, 1005}, {1006, 1010}}))
                ...
                .build();
env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "TestSource")
        .addSink(new DiscardSink());
env.execute();

```

#### Minimalist Streaming Semantic and ContinuousUnBoundingSettings
If you want to generate continuous milliseconds parameters based on sliding-window,
please have a try on setting the followed attributes of `JdbcSource`:

```java

jdbcSourceBuilder =
        JdbcSource.<TestEntry>builder()
        .setSql("select * from testing_table where ts >= ? and ts < ?")
        
        // Required for streaming related semantic.
        .setContinuousUnBoundingSettings(new ContinuousUnBoundingSettings(Duration.ofMillis(10L), Duration.ofSeconds(1L)))
        .setJdbcParameterValuesProvider(new JdbcSlideTimingParameterProvider(0L, 1000L, 1000L, 100L))
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE);

        // other attributes
        ...
        
JdbcSource source = jdbcSourceBuilder.build();
```

See {{< javadoc file="org/apache/flink/connector/jdbc/utils/ContinuousUnBoundingSettings.html" name="ContinuousUnBoundingSettings javadoc" >}} for more details.

#### Full example

{{< tabs "4ab65f13-608a-411a-8d24-e303f348ds81" >}}
{{< tab "Java" >}}

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
                        .setSql(sql)
                        .setDBUrl("select * from testing_table where id < ?")
                        .setJdbcParameterValuesProvider(
                                new JdbcGenericParameterValuesProvider(
                                        new Serializable[][] {{1001L}}))
                        .setDriverName(...)
                        .setResultExtractor(resultSet ->
                            new Book(
                                resultSet.getLong("id"),
                                resultSet.getString("title")))
                .build();
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "TestSource")
                .addSink(new DiscardingSink());
        env.execute();
    }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

## Sink of JDBC Connector

### `JdbcSink.sink`

The JDBC sink provides at-least-once guarantee.
Effectively though, exactly-once can be achieved by crafting upsert SQL statements or idempotent SQL updates.
Configuration goes as follow (see also {{< javadoc file="org/apache/flink/connector/jdbc/JdbcSink.html" name="JdbcSink javadoc" >}}).

{{< tabs "4ab65f13-607a-411a-8d24-e709f701cd4c" >}}
{{< tab "Java" >}}
```java
JdbcSink.sink(
      	sqlDmlStatement,                       // mandatory
      	jdbcStatementBuilder,                  // mandatory   	
      	jdbcExecutionOptions,                  // optional
      	jdbcConnectionOptions                  // mandatory
);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
JdbcSink.sink(
    sql_dml_statement,          # mandatory
    type_info,                  # mandatory
    jdbc_connection_options,    # mandatory
    jdbc_execution_options      # optional
)
```
{{< /tab >}}
{{< /tabs >}}

#### SQL DML statement and JDBC statement builder

The sink builds one [JDBC prepared statement](https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/PreparedStatement.html) from a user-provider SQL string, e.g.:

```sql
INSERT INTO some_table field1, field2 values (?, ?)
```

It then repeatedly calls a user-provided function to update that prepared statement with each value of the stream, e.g.:

```
(preparedStatement, someRecord) -> { ... update here the preparedStatement with values from someRecord ... }
```

#### JDBC execution options

The SQL DML statements are executed in batches, which can optionally be configured with the following instance (see also {{< javadoc name="JdbcExecutionOptions javadoc" file="org/apache/flink/connector/jdbc/JdbcExecutionOptions.html" >}})

{{< tabs "4ab65f13-607a-411a-8d24-e709f512ed6k" >}}
{{< tab "Java" >}}
```java
JdbcExecutionOptions.builder()
        .withBatchIntervalMs(200)             // optional: default = 0, meaning no time-based execution is done
        .withBatchSize(1000)                  // optional: default = 5000 values
        .withMaxRetries(5)                    // optional: default = 3 
.build();
```
{{< /tab >}}
{{< tab "Python" >}}
```python
JdbcExecutionOptions.builder() \
    .with_batch_interval_ms(2000) \
    .with_batch_size(100) \
    .with_max_retries(5) \
    .build()
```
{{< /tab >}}
{{< /tabs >}}

A JDBC batch is executed as soon as one of the following conditions is true:

* the configured batch interval time is elapsed
* the maximum batch size is reached
* a Flink checkpoint has started

#### JDBC connection parameters

The connection to the database is configured with a `JdbcConnectionOptions` instance.
Please see {{< javadoc name="JdbcConnectionOptions javadoc" file="org/apache/flink/connector/jdbc/JdbcConnectionOptions.html" >}} for details

#### Full example

{{< tabs "4ab65f13-608a-411a-8d24-e303f348ds8d" >}}
{{< tab "Java" >}}
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

        env.fromElements(
                new Book(101L, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
                new Book(102L, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
                new Book(103L, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
                new Book(104L, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
        ).addSink(
                JdbcSink.sink(
                        "insert into books (id, title, authors, year) values (?, ?, ?, ?)",
                        (statement, book) -> {
                            statement.setLong(1, book.id);
                            statement.setString(2, book.title);
                            statement.setString(3, book.authors);
                            statement.setInt(4, book.year);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:postgresql://dbhost:5432/postgresdb")
                                .withDriverName("org.postgresql.Driver")
                                .withUsername("someUser")
                                .withPassword("somePassword")
                                .build()
                ));
                
        env.execute();
    }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
type_info = Types.ROW([Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])
env.from_collection(
    [(101, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
     (102, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
     (103, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
     (104, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
     ], type_info=type_info) \
    .add_sink(
    JdbcSink.sink(
        "insert into books (id, title, authors, year) values (?, ?, ?, ?)",
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url('jdbc:postgresql://dbhost:5432/postgresdb')
            .with_driver_name('org.postgresql.Driver')
            .with_user_name('someUser')
            .with_password('somePassword')
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_interval_ms(1000)
            .with_batch_size(200)
            .with_max_retries(5)
            .build()
    ))

env.execute()
```
{{< /tab >}}
{{< /tabs >}}

### `JdbcSink.exactlyOnceSink`

Since 1.13, Flink JDBC sink supports exactly-once mode.
The implementation relies on the JDBC driver support of XA
[standard](https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf).
Most drivers support XA if the database also supports XA (so the driver is usually the same).

To use it, create a sink using `exactlyOnceSink()` method as above and additionally provide:
- {{< javadoc name="exactly-once options" file="org/apache/flink/connector/jdbc/JdbcExactlyOnceOptions.html" >}}
- {{< javadoc name="execution options" file="org/apache/flink/connector/jdbc/JdbcExecutionOptions.html" >}}
- [XA DataSource](https://docs.oracle.com/javase/8/docs/api/javax/sql/XADataSource.html) Supplier

For example:

{{< tabs "4ab65f13-608a-411a-8d24-e304f627ac8f" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env
        .fromElements(...)
        .addSink(JdbcSink.exactlyOnceSink(
                "insert into books (id, title, author, price, qty) values (?,?,?,?,?)",
                (ps, t) -> {
                    ps.setInt(1, t.id);
                    ps.setString(2, t.title);
                    ps.setString(3, t.author);
                    ps.setDouble(4, t.price);
                    ps.setInt(5, t.qty);
                },
                JdbcExecutionOptions.builder()
                    .withMaxRetries(0)
                    .build(),
                JdbcExactlyOnceOptions.defaults(),
                () -> {
                    // create a driver-specific XA DataSource
                    // The following example is for derby 
                    EmbeddedXADataSource ds = new EmbeddedXADataSource();
                    ds.setDatabaseName("my_db");
                    return ds;
                });
env.execute();
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

**NOTE:** Some databases only allow a single XA transaction per connection (e.g. PostgreSQL, MySQL).
In such cases, please use the following API to construct `JdbcExactlyOnceOptions`:

{{< tabs "4ab65f13-608a-411a-8d24-e304f627cd4e" >}}
{{< tab "Java" >}}
```java
JdbcExactlyOnceOptions.builder()
.withTransactionPerConnection(true)
.build();
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

This will make Flink use a separate connection for every XA transaction. This may require adjusting connection limits.
For PostgreSQL and MySQL, this can be done by increasing `max_connections`.

Furthermore, XA needs to be enabled and/or configured in some databases.
For PostgreSQL, you should set `max_prepared_transactions` to some value greater than zero.
For MySQL v8+, you should grant `XA_RECOVER_ADMIN` to Flink DB user.

**ATTENTION:** Currently, `JdbcSink.exactlyOnceSink` can ensure exactly once semantics
with `JdbcExecutionOptions.maxRetries == 0`; otherwise, duplicated results maybe produced.

#### `XADataSource` examples
PostgreSQL `XADataSource` example:
{{< tabs "4ab65f13-608a-411a-8d24-e304f323ab3a" >}}
{{< tab "Java" >}}
```java
PGXADataSource xaDataSource = new org.postgresql.xa.PGXADataSource();
xaDataSource.setUrl("jdbc:postgresql://localhost:5432/postgres");
xaDataSource.setUser(username);
xaDataSource.setPassword(password);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

MySQL `XADataSource` example:
{{< tabs "4ab65f13-608a-411a-8d24-b213f323ca3c" >}}
{{< tab "Java" >}}
```java
MysqlXADataSource xaDataSource = new com.mysql.cj.jdbc.MysqlXADataSource();
xaDataSource.setUrl("jdbc:mysql://localhost:3306/");
xaDataSource.setUser(username);
xaDataSource.setPassword(password);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

Oracle `XADataSource` example:
{{< tabs "4ab65f13-608a-411a-8d24-b213f337ad5f" >}}
{{< tab "Java" >}}
```java
OracleXADataSource xaDataSource = new oracle.jdbc.xa.OracleXADataSource();
xaDataSource.setURL("jdbc:oracle:oci8:@");
xaDataSource.setUser("scott");
xaDataSource.setPassword("tiger");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

Please also take Oracle connection pooling into account.

Please refer to the `JdbcXaSinkFunction` documentation for more details.
