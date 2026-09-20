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

# Flink JDBC Connector AI Agent Instructions

This file provides guidance for AI coding agents working with the Apache Flink JDBC connector codebase.

## Prerequisites

- Java 11, 17, or 21. Java 11 is the source baseline: the build compiles with source level 11, so Java 11 syntax must be used everywhere. PR CI runs on JDK 17 and 21 (`.github/workflows/push_pr.yml`); the weekly build additionally covers JDK 8 and 11 across the release branches (`.github/workflows/weekly.yml`).
- Maven 3.8.6. There is no Maven wrapper in this repository; use a system `mvn`.
- Git
- Docker (every `*ITCase` and `*E2eTest` starts its database through Testcontainers)
- Unix-like environment (Linux, macOS, WSL)
- The connector builds against `flink.version` in the root `pom.xml`. A change must also work against every Flink version in `.github/workflows/push_pr.yml` and the `main` row of `.github/workflows/weekly.yml`.

## Commands

### Build

- Build without tests: `mvn clean package -DskipTests`
- Full build with tests: `mvn clean verify`
- Build against another Flink version (what CI does): `mvn clean install -DskipTests -Dflink.version=<version>`
- Single module: `mvn clean install -DskipTests -pl flink-connector-jdbc-core`
- A database module depends on `flink-connector-jdbc-core`; build it with its dependency using `-pl flink-connector-jdbc-mysql -am`.
- Dependency convergence (CI runs this on PRs): `mvn clean install -DskipTests -Pcheck-convergence -Dflink.convergence.phase=install`

### Testing

- `*Test` classes are unit tests; `*ITCase` classes are integration tests that start a real database via Testcontainers (Docker required). `flink-connector-jdbc-core` also runs against in-memory Derby and H2, so its unit tests need no Docker.
- Single unit test class: `mvn test -pl flink-connector-jdbc-core -Dtest=JdbcOutputFormatTest`
- Single test method: `mvn test -pl flink-connector-jdbc-core -Dtest=JdbcOutputFormatTest#testX`
- Single ITCase in a database module: `mvn test -pl flink-connector-jdbc-postgres -am -Dtest=PostgresDialectITCase`
- ArchUnit rules live in `flink-connector-jdbc-architecture`; violation stores are frozen (see Testing Standards).
- The standalone `flink-connector-jdbc-backward-compatibility` module is not part of the root reactor; CI builds it separately (`.github/workflows/backwards_compatibility.yml`) to check that current code reads savepoints written by older connector versions.
- CI for PRs and the weekly build is defined by `apache/flink-connector-shared-utils` (`.github/workflows/ci.yml@ci_utils`); its Maven command line, including the license check, is the reference when a local run differs from CI.

### Code Quality

- Format code: `mvn spotless:apply` (the `java21` profile sets `spotless.skip=true`, so run it on JDK 11 or 17; google-java-format does not run on JDK 21)
- Check formatting: `mvn spotless:check`
- Checkstyle: `mvn checkstyle:check` (config in `tools/maven/checkstyle.xml`, suppressions in `tools/maven/suppressions.xml`)
- License headers: `mvn apache-rat:check`
- japicmp compares against `japicmp.referenceVersion` from the root `pom.xml` and only checks stable API.

### Documentation

- There is no docs build in this repository. The Flink docs build in `apache/flink` clones the release branch of this repository and renders `docs/content` and `docs/content.zh`.
- Documentation exists in English (`docs/content`) and Chinese (`docs/content.zh`); the two carry the same file set.

## Repository Structure

### Modules

The root `pom.xml` reactor contains:

- `flink-connector-jdbc-architecture` — ArchUnit tests and their frozen violation stores (`archunit-violations/`).
- `flink-connector-jdbc-core` — The connector: the FLIP-27 `JdbcSource`, the Sink V2 `JdbcSink`, the legacy `JdbcInputFormat` / `JdbcRowOutputFormat`, the Table/SQL factory (identifier `jdbc`), the dialect SPI, the XA/exactly-once machinery, and the Derby + H2 in-memory test support. Everything database-agnostic lives here.
- `flink-connector-jdbc-cratedb`, `-db2`, `-mysql`, `-oceanbase`, `-oracle`, `-postgres`, `-sqlserver`, `-trino` — one module per database. Each contributes a `JdbcDialect`, a `JdbcFactory` (dialect SPI), usually a `JdbcCatalog` + type mapper, and a lineage location extractor, discovered via `META-INF/services`.

Not in the reactor:

- `flink-connector-jdbc-backward-compatibility` — standalone module, built by its own CI workflow, that verifies savepoint/state compatibility with older connector versions.

### Supporting directories

- `docs/content/docs/connectors/` and `docs/content.zh/docs/connectors/` — DataStream and Table docs, English and Chinese, same file set.
- `tools/maven/` — checkstyle config and suppressions.
- `tools/ci/` — the log4j config CI passes to Maven.
- `.github/workflows/` — `push_pr.yml` (PR CI), `weekly.yml` (release branches and Flink snapshots), `backwards_compatibility.yml` (the standalone module).

### Key packages in `flink-connector-jdbc-core/src/main/java`

- `org.apache.flink.connector.jdbc` — top-level user-facing option/builder types: `JdbcConnectionOptions`, `JdbcExecutionOptions`, `JdbcExactlyOnceOptions`, `JdbcStatementBuilder`, and the legacy `JdbcInputFormat` / `JdbcRowOutputFormat`.
- `org.apache.flink.connector.jdbc.core.datastream.source` — FLIP-27 source. `@PublicEvolving`: `JdbcSource`, `JdbcSourceBuilder`, `JdbcSourceOptions`. `@Internal`: `enumerator/`, `reader/`, `split/`.
- `org.apache.flink.connector.jdbc.core.datastream.sink` — Sink V2 sink. `@PublicEvolving`: `JdbcSink`, `JdbcSinkBuilder`. `writer/` and `committer/` are `@Internal`.
- `org.apache.flink.connector.jdbc.core.table` — Table/SQL layer. `JdbcDynamicTableFactory` (identifier `jdbc`, registered in `META-INF/services/org.apache.flink.table.factories.Factory`), `JdbcConnectorOptions` (`@PublicEvolving`), and the `@Internal` `JdbcDynamicTableSource` / `JdbcDynamicTableSink`.
- `org.apache.flink.connector.jdbc.core.database` — the pluggable-database SPI: `JdbcFactory` and `JdbcDialect` (both `@PublicEvolving`), `dialect/` (statement building, converters), and `catalog/` (`JdbcCatalog`, the catalog factory).
- `org.apache.flink.connector.jdbc.datasource` — connection and transaction management. `connections/` holds `JdbcConnectionProvider` / `SimpleJdbcConnectionProvider`; `connections/xa/` and `transactions/xa/` hold the XA two-phase-commit path used for exactly-once.
- `org.apache.flink.connector.jdbc.internal` — `JdbcOutputFormat`, the DML/insert/read option holders (`internal/options/`), and the connection/statement execution glue. `@Internal`.
- `org.apache.flink.connector.jdbc.lineage` — OpenLineage facet extraction and the `JdbcLocationExtractorFactory` SPI.
- `org.apache.flink.connector.jdbc.split` — input-split parameter providers for the legacy input format.

## Architecture Boundaries

1. **Source (FLIP-27).** `JdbcSourceEnumerator` runs on the coordinator thread and hands `JdbcSourceSplit`s to `JdbcSourceReader`; `JdbcSourceSplitReader` runs on the fetcher thread and owns the JDBC `Connection`/`ResultSet`. A JDBC connection is not thread-safe — access it only from the reader thread.
2. **Sink (Sink V2).** `JdbcSink` builds on `JdbcOutputFormat`, which batches rows and flushes on a size/time trigger and on checkpoint. The at-least-once path uses a plain `JdbcConnectionProvider`; per-record failures surface on the task thread at flush time.
3. **Exactly-once (XA).** The exactly-once sink uses distributed (XA) transactions via `connections/xa` (`XaConnectionProvider`, `PoolingXaConnectionProvider`) and `transactions/xa` (Xid generation, per-checkpoint transaction lifecycle). XA driver support varies by database; this is the most driver-sensitive path — re-verify it on any driver bump. Not every dialect supports it.
4. **Dialect SPI.** A database plugs in through `JdbcFactory` (`acceptsURL` decides which JDBC URLs it claims; `createDialect` / `createDialect(compatibleMode)` builds the `JdbcDialect`), discovered via `META-INF/services/org.apache.flink.connector.jdbc.core.database.JdbcFactory`. The `JdbcDialect` owns quoting, LIMIT, upsert/UPSERT-or-MERGE statement construction, and the `JdbcDialectConverter` type mapping. Keep database-specific behaviour inside the dialect, not in core.
5. **Table layer.** `JdbcDynamicTableSource` / `JdbcDynamicTableSink` wrap the DataStream connectors; all Table options live in `JdbcConnectorOptions`.
6. **Connector vs Flink.** Production code may depend only on stable (`@Public` / `@PublicEvolving`) Flink API outside connector and util packages (enforced by ArchUnit). Every Flink API used must exist with the same annotation in `flink.version`, because the connector is released for several Flink minor versions.

## Common Change Patterns

### Adding a new database

1. Create a `flink-connector-jdbc-<db>` module mirroring an existing one (e.g. `flink-connector-jdbc-postgres`), with the DB module as a sibling of `flink-connector-jdbc-core` and added to the root `pom.xml` `<modules>`.
2. Implement `JdbcDialect` + `JdbcDialectConverter`, and a `JdbcFactory` whose `acceptsURL` matches the driver's JDBC sub-protocol; register the factory in `META-INF/services/org.apache.flink.connector.jdbc.core.database.JdbcFactory`.
3. If the catalog is supported, add a `JdbcCatalog` + type mapper; add a `JdbcLocationExtractorFactory` for lineage.
4. Pin the JDBC driver version as a `<db>.version` property in the module `pom.xml` and add the driver dependency (test or provided scope, as the sibling modules do).
5. Add a Testcontainers `*Database` helper under `testutils/`, a `*TestBase`, and dialect/catalog `*ITCase`s.
6. Document the dialect in `docs/content` and `docs/content.zh`.

### Adding a Table/SQL option

1. Define the `ConfigOption<T>` in `JdbcConnectorOptions`
2. Register and validate it in `JdbcDynamicTableFactory` (`optionalOptions()` / validation)
3. Add a factory test and, when behaviour changes, an `ITCase`
4. Add the option row to `docs/content/docs/connectors/table/*.md` and the same file under `docs/content.zh/`
5. Fill in the Release Notes field on the JIRA ticket

### Adding a DataStream builder option

1. Add it to `JdbcSourceBuilder` or `JdbcSinkBuilder` with validation in `build()`
2. Add a builder unit test and, when behaviour changes, an `ITCase`
3. Document it under `docs/content/docs/connectors/datastream/` and the `.zh` copy

### Changing checkpointed state

State is written by `SimpleVersionedSerializer` implementations. Current versions: `JdbcSourceSplitSerializer` 0, `JdbcSourceEnumStateSerializer` 0, `JdbcWriterStateSerializer` 2, `JdbcCommitableSerializer` 1.

1. Bump the version and keep a read path for every older version
2. Add a serializer test that decodes bytes of the previous version
3. Cover the change with the `flink-connector-jdbc-backward-compatibility` module

### Bumping a database driver version

1. Change the `<db>.version` property in the module `pom.xml`
2. If the change affects exactly-once, re-verify the XA path against that database
3. Verify: `dependency:tree` for new transitive dependencies, and the module's ITCase suite

### Bumping `flink.version` or changing the CI matrix

1. Get consensus on the JIRA ticket first; this changes which Flink versions the branch supports
2. Update `push_pr.yml` and `weekly.yml`; keep one JDK per Flink version and stay under the ASF limit of 20 concurrent jobs
3. Verify: the build against every Flink version in the matrix

### Fixing a flaky test

1. Name the race or ordering that fails, with the CI log excerpt
2. Wait on the condition (`CommonTestUtils.waitUtil`), never on time; for a database, wait for readiness through the Testcontainers wait strategy
3. Do not add `Thread.sleep`, larger timeouts, retries or `@Disabled`
4. Verify: run the test repeatedly and state the number of runs in the PR

## Coding Standards

- **Format Java files with Spotless after editing:** `mvn spotless:apply` (google-java-format, AOSP style). It is auto-skipped on JDK 21, so format on 11 or 17.
- **Checkstyle:** `tools/maven/checkstyle.xml`. Fix the code rather than suppressing rules.
- **Apache License 2.0 header** required on all new files (enforced by Apache Rat). Use an HTML comment for markdown files.
- **API stability annotations:** Every user-facing API class and method must have one. `@Public` (stable across minor releases), `@PublicEvolving` (may change in minor releases), `@Experimental` (may change at any time), `@Internal` (no guarantees; do not depend on).
- **JDBC resources:** close `Connection`, `Statement`, and `ResultSet` deterministically (try-with-resources). A `Connection` is single-threaded; do not share it across threads.
- **Logging:** parameterized SLF4J statements (`{}` placeholders), never string concatenation.
- **No Java serialization** for new state; use `SimpleVersionedSerializer`.
- **Use `final`** for variables and fields where applicable.
- **Comments:** do not restate what the code does; explain the "why" where it is non-obvious.
- **Keep database-specific logic in the dialect,** not in `flink-connector-jdbc-core`.
- Full code style guide: https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/

## Testing Standards

- Add tests for new behavior, covering success, failure, and edge cases.
- Use **JUnit 5** + **AssertJ** assertions.
- **Integration tests:** name classes with the `ITCase` suffix; they start their database through Testcontainers. Reuse the module's `testutils/*Database` helper and `*TestBase`; do not hand-roll container setup.
- **Core tests** use in-memory Derby and H2 (`flink-connector-jdbc-core`), so they run without Docker; prefer these for database-agnostic behaviour.
- **Red-green verification:** for bug fixes, confirm the new test fails without the fix before it passes with it.
- **ArchUnit:** violation stores under `flink-connector-jdbc-architecture/archunit-violations/` (and `archunit-violations/` in modules that have them) are frozen. A local run removes lines when violations disappear; commit removed lines with the change, never add lines.
- **Backward compatibility:** when a serializer version changes, cover it in `flink-connector-jdbc-backward-compatibility`.
- Follow the testing conventions at https://flink.apache.org/how-to-contribute/code-style-and-quality-common/#7-testing

## Commits and PRs

### Commit message format

- `[FLINK-XXXX][component] Description` where FLINK-XXXX is the JIRA issue number
- `[hotfix][component] Description` for typo/doc/CI-config fixes without JIRA
- Each commit must have a meaningful message including the JIRA ID. If you don't know the ticket number, ask.
- Separate cleanup/refactoring from functional changes into distinct commits
- When AI tools were used: add a `Generated-by: <Tool Name and Version>` trailer per [ASF generative tooling guidance](https://www.apache.org/legal/generative-tooling.html)

### Pull request conventions

- Title format: `[FLINK-XXXX][component] Title of the pull request`
- A corresponding JIRA issue is required (except hotfixes for typos, docs, or CI config)
- Fill out the PR template completely but concisely: purpose, change log, testing approach, impact assessment
- Each PR should address exactly one issue
- Ensure `mvn clean verify` passes before opening a PR
- Always push to your fork, not directly to `apache/flink-connector-jdbc`
- Rebase onto the latest target branch before submitting
- Branches: `main` is the next connector version; `v<major>.<minor>` are release branches. Artifacts are versioned `<connector version>-<flink major.minor>`.
- For user-visible behaviour changes, breaking changes, or new options: fill in the **Release Notes** field on the JIRA ticket.

### AI-assisted contributions

- Disclose AI usage by checking the AI disclosure checkbox and filling in the `Generated-by` line in the PR template
- Add `Generated-by: <Tool Name and Version>` to commit messages
- Never add `Co-Authored-By` with an AI agent as co-author; agents are assistants, not authors
- You must be able to explain the design, code, and tests, debug them, and respond to review feedback substantively
- Reviewer-ready quality bar: the author owns PR quality. PRs that look AI-generated without author refinement (walls of unreviewed prose, scaffolding without behaviour, tests that do not exercise the change, padded commit messages) will be closed without review

## Boundaries

### Ask first

- Adding or changing `@Public`, `@PublicEvolving`, or `@Experimental` API, including Table options and the `JdbcDialect` / `JdbcFactory` SPI. Discuss on the JIRA ticket; connector-wide semantic changes may require a FLIP.
- Changes to checkpointed state or its serializers
- Changes to the exactly-once (XA) path
- Bumping `flink.version` or changing the CI matrix
- New dependencies
- Changes on the per-record path (split reader, `JdbcOutputFormat`, dialect statement building)

### Never

- Commit secrets, credentials, or tokens
- Push directly to `apache/flink-connector-jdbc`; always work from your fork
- Mix unrelated changes into one PR
- Use `@Internal` Flink classes, or Flink API that does not exist in `flink.version`
- Add lines to any `archunit-violations/` store, `tools/maven/suppressions.xml`, or the RAT excludes
- Put database-specific logic in `flink-connector-jdbc-core`
- Add `Co-Authored-By` with an AI agent as co-author in commit messages; use `Generated-by: <Tool Name and Version>` instead
- Use destructive git operations unless explicitly requested

## References

- [README.md](README.md) — Build instructions and project overview
- [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) — PR checklist
- [JDBC connector documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/jdbc/) — User-facing docs
- [Externalized Connector development](https://cwiki.apache.org/confluence/display/FLINK/Externalized+Connector+development) — Versioning, branching, Flink compatibility, and common review issues for connector repositories
- [Code Style Guide](https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/) — Detailed coding guidelines
- [Flink Improvement Proposals](https://cwiki.apache.org/confluence/display/FLINK/Flink+Improvement+Proposals) — When a FLIP is required
- [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html) — AI tooling policy
