# bellwether-source-sql

Work package WP20 (ADR 027). SQL tables as Bellwether sources: a table is a `Source`, so reconcile, `why`, and `fix` work over it unchanged, through the facade, the CLI, and the MCP server. The headline is reconciling a table against the folder of files it was loaded from (or exported to): a Certificate whose provenance names both sides' identities.

```text
bellwether reconcile 'sql://duckdb/main.trips?db=taxi.duckdb&range=tpep_pickup_datetime&units=12' ./taxi/ \
    --key VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,PULocationID,DOLocationID \
    --totals fare_amount:dec(2) --index ./ledger
```

## The root

`sql://<engine>/<schema>.<table>[,<schema>.<table>...]?db=<connection>[&driver=<library>][&range=<column>&units=<n>][&version=<column>][&snapshot=transaction|reread]`

| Engine | `db=` | Driver |
|---|---|---|
| `sqlite` | the database file (opened read-only) | rusqlite, bundled SQLite (feature `sqlite`, default) |
| `duckdb` | the database file | ADBC: libduckdb, its own ADBC driver (feature `adbc`, default) |
| `postgres` | a `postgresql://` URI | ADBC: `adbc_driver_postgresql` |
| `flightsql` | a `grpc://` URI | ADBC: `adbc_driver_flightsql` |
| `mssql` | an ODBC connection string | ODBC through `arrow-odbc` (feature `odbc`) |

`db=env:NAME` reads the connection from the environment variable `NAME`, so a password never appears on a command line. Nothing after `?` reaches a record: records name `sql://<engine>/<tables>` only. An ADBC driver library is found at `driver=`, else `BELLWETHER_DUCKDB_DRIVER`, `BELLWETHER_ADBC_POSTGRESQL_DRIVER`, or `BELLWETHER_ADBC_FLIGHTSQL_DRIVER`, else (DuckDB) `libduckdb` in `/opt/homebrew/lib`, `/usr/local/lib`, `/usr/lib`, else the ADBC driver manager's search by name. A table root has no folder to hold an index, so the CLI and the MCP server need `--index` (the ledger's location); index builds and lookups refuse a table (`not_a_folder`).

### Why these drivers

- **ADBC through `adbc_driver_manager`** (Apache Arrow ADBC, Apache-2.0, the reference Rust driver manager): DuckDB, Postgres, and Flight SQL return Arrow batches in the workspace's own Arrow version (adbc 0.24 takes arrow 58 or 59), so nothing is converted between Arrow copies. The driver library is loaded at run time, so no C or C++ is compiled into the build. DuckDB's ADBC driver is libduckdb itself (`duckdb_adbc_init`), which also means no DuckDB Rust crate: none is built on Arrow 59.
- **rusqlite with the bundled SQLite** (MIT) for SQLite: in process, always available, no driver to install. The ADBC SQLite driver is a C library not distributed through crates.io, and it types each column from the first values it reads, where this source must read the declared decimal scale and time unit.
- **`arrow-odbc` over `odbc-api`** (MIT, actively maintained, bulk fetches into Arrow) for SQL Server. It links the system ODBC driver manager, so it is behind the `odbc` feature (`--features bellwether/odbc` in the facade, `bellwether-cli/odbc`, `bellwether-mcp/odbc`).

`cargo deny --all-features check` passes with every driver crate: Apache-2.0 (adbc), MIT (rusqlite, libsqlite3-sys, arrow-odbc, odbc-api, odbc-sys), and the permissive licences already allowed.

## Units, identities, statistics

Each table is cut into units, each a "file" of one row group: its declared partitions (Postgres), else at most `units=<n>` key ranges of `range=<column>` plus one unit for its nulls, else the whole table. Range boundaries are the column's own values at evenly spaced ranks, in the engine's own order, so every row lands in exactly one unit; discovery checks that the units' row counts sum to the table's `COUNT(*)` and refuses the cut otherwise. A unit's fingerprint path is `sql://<engine>/<schema>.<table>/<unit>`, its size the row count, its etag the identity (`v1;rows=<n>;version=<hex>` with `version=<column>`, else `v1;rows=<n>;stats=<hex>`), and `tail_blake3` hashes the identity string. `docs/format.md`, "SQL sources", is the definition.

### Which numbers are counts, bounds, or measured

| Quantity | What it is |
|---|---|
| `FileFingerprint.size`, `RowGroupMeta.rows` | a count: `COUNT(*)` over the unit |
| `ColumnStats.null_count` | a count: `COUNT(*) - COUNT(col)` |
| `ColumnStats.min`, `max` | bounds attained by the data: `MIN`, `MAX` over the unit in byte order (`COLLATE "C"`, `BINARY`, `"binary"`), encoded as the dataset type; `None` for SQL Server and Flight SQL strings, whose order is not byte order |
| `ColumnStats.bytes`, `RowGroupMeta.bytes` | **measured**: logical bytes, the non-null count times the type's width, or the engine's octet length summed for strings and binaries; not storage bytes |
| `ColumnStats.distinct_count`, `has_bloom` | `None`, `false` |
| unit counts, units read, rows read in `why`, `fix`, and reconcile | counts |
| bytes in `why` and `fix` over a table, the dataset's bytes | **measured** (`exact: false`, `exactness: Measured { model }`, model `measured: logical bytes from per-unit aggregates`) |
| `MeasuredScore` | **measured**: wall-clock nanoseconds (`measured: wall time`), or Postgres buffers / DuckDB rows scanned from `EXPLAIN ANALYZE` (`measured: engine plan statistics`), with the noise as a bound |

Bounds are reported only where the engine's `MIN`/`MAX` order is the order of the values read: SQLite times, dates, and decimals only when every value has one storage class and (for text) the one canonical shape (`YYYY-MM-DD HH:MM:SS[.f]`, `YYYY-MM-DD`); none for Postgres `bytea` (no `MIN`) or `character(n)` (compared without trailing blanks, read padded); strings in byte order under any column collation.

SQLite itself stores a `DECIMAL(p,s)` value with more than 15 significant digits as an 8-byte float or integer and loses its last digits on insert (`'12345678901234567.89'` becomes `12345678901234568`); the reader reads what SQLite stored, so declare such columns `TEXT` and give the decimal type in the dataset schema.

Planner statistics (`pg_stats`, `sqlite_stat4`, BRIN summaries, DuckDB zone maps, columnstore segment metadata) are never used as bounds: they are samples, or cover block ranges and row groups that are not the unit, and a bound that excludes a row would break invariant 1.

The identity's limit: the statistics checksum sees a change to the row count or to any column's minimum, maximum, null count, or bytes; an in-place update that keeps all of them is not seen. Name a version column that increases on every write (`version=updated_at`) where that matters, or read inside a snapshot.

## Snapshot discipline

A run (a reconcile, a `why`, a `fix`) holds one read transaction where the engine has a snapshot: Postgres `REPEATABLE READ READ ONLY`, DuckDB's transaction (snapshot isolation), a SQLite read transaction, SQL Server `SNAPSHOT` isolation when the database allows it. The transaction is rolled back when the run ends. Where there is none (`snapshot=reread`, Flight SQL, SQL Server without snapshot isolation), every unit's identity and the partition list are read again before anything is recorded, and a change is refused (`Error::SourceChanged`, MCP `source_changed`): never a partial answer. A unit read that returns another row count than its identity says is refused either way. In SQLite's rollback-journal mode a read transaction makes writers wait for the run; `snapshot=reread` avoids that. The human output names which discipline each table got.

A DuckDB source opens its database read-only, so other processes may read it too (a process that already holds the file read-write in this process is shared instead). Errors never print the connection: a driver's message has it replaced by `<connection>`, and the CLI prints a root as `db=<connection>` unless it names an environment variable. The source identity covers a hash of the connection with its credentials removed, so two databases holding a table of one name are two identities. A unit holds at most `max_unit_rows` rows and `max_unit_bytes` logical bytes (16 Mi rows, 1 GiB); `read_columns` may hold a second copy while it joins the engine's batches, so twice that at most.

Every value is read at the dataset type and converted exactly or refused, never rounded: a decimal at another scale is rescaled exactly, a timestamp in another unit converted exactly, an integer narrowed only if it fits. When a table is reconciled against a folder, the table is read at the folder's types where the two differ only in scale, unit, or width (`--schema` for the data root wins).

## Measured costs and attest-only apply

Over a table every cost is measured, never exact (ADR 027). `why` and `fix` compute the same replay as over files (runs no spec serves are priced as a raw scan), mark every cost `exact: false` and `exactness: Measured { model }` with the model `measured: logical bytes from per-unit aggregates`, and report it; the ledger keeps a Replay only when its current costs are exact (ADR 005), so these are never recorded, and nothing measured is ever stored as exact. The `MeasuredScorer` trait (`TimingScorer`, `ExplainScorer`) has the shape of evolve's `Scorer` (`docs/evolve.md` §10) and is for evolve once it lands; nothing in the engine calls it, since it runs SQL text and a Proposal's text is an agent's.

Apply is attest-only. The engine creates no index inside a database and deploys no job, so `fix` over a table scores, verifies, writes the Certificate, applies the same gain gate as over files (`nothing_to_gain`, `--allow-no-gain`), and refuses with `attest_only`: for a key it names the `CREATE INDEX "bw_<table>_<cols>" ON ...` per table a human runs (`run`); for a rewrite it returns the agent's `after` text as `proposed_rewrite`, which the engine never ran, for a human to review. No Receipt is written: a Receipt attests an apply the engine made against `manifest_before`, and here the engine made none.

## Deliberate-break checks

Each was made once in a scratch copy and the named test failed (the PR lists the runs):

| Break | Test that fails |
|---|---|
| skip the identity re-read at the end of a run without a snapshot | `tests/breaks.rs::a_write_mid_run_without_a_snapshot_is_refused` |
| trust a partial unit (accept fewer rows than the identity says) | `tests/breaks.rs::a_partial_unit_is_refused` |
| skip the check that key-range units partition the table | `tests/breaks.rs::units_that_do_not_partition_the_table_are_refused` |
| mark a measured cost exact | `bellwether/tests/sql_tables.rs::why_over_a_table_never_reports_or_stores_a_measured_cost_as_exact` |

## Tests

| File | Covers |
|---|---|
| `tests/engines.rs` | SQLite and DuckDB in process, every path: declared scale and unit; unit statistics against an oracle over the rows read, for one unit and key ranges on an integer, a timestamp, and a string; the two engines report identical statistics for the same rows; projected reads, nulls, an empty projection; decimals at two scales and timestamps in two units under one dataset type, and the refusals when a value does not fit; a snapshot hides a write committed mid-run; identities follow the data (statistics checksum and version column); refusals naming the fix (no declared type, no scale, an integer in a bare `TIMESTAMP`, text in an integer column, a missing table or file); the unit row cap; several tables of one shape |
| `tests/breaks.rs` | the deliberate-break checks above, on both engines, with and without a snapshot |
| `tests/corrupt.rs` | the corrupt-response sweep: every response of an honest run (catalog, counts, boundary scan, aggregates, reads) corrupted ten ways in turn (columns dropped, added, replaced by text, negated, nulled, duplicated, emptied, halved, failed, floats for integers); never a panic, most refused, and any answer complete |
| `tests/measured.rs` | the scorers: never exact, model named, noise bound, one statement only |
| `tests/servers.rs` | Postgres when `BELLWETHER_PG_URL` is set (declared partitions as units, statistics against the oracle with byte-order strings, `REPEATABLE READ`, a partition added mid-run refused, key ranges); SQL Server through ODBC when built with `odbc` and `BELLWETHER_MSSQL_URL` is set. Each prints `SKIPPED:` and why otherwise |
| `bellwether/tests/sql_tables.rs` | through the facade: a table reconciled against its Parquet files at another scale and unit, both directions, a planted difference found; a scale the files cannot hold refused; `why` measured and unrecorded; `fix` attest-only with no Receipt, including the apply tier and its dry run; folder-only operations refused |

DuckDB tests need libduckdb (CI downloads it and sets `BELLWETHER_DUCKDB_DRIVER`; locally a Homebrew `duckdb` provides it). Without it they print `SKIPPED:` and pass, except in CI (`CI` set), where they fail.

## Demo

`scripts/sql-demo.sh` loads the NYC taxi months the real-data run uses into DuckDB and SQLite and runs reconcile and `why` against the same Parquet files; its measured output is in `docs/measured/results/sql.json`.
