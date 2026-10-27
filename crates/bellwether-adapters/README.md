# bellwether-adapters

Bellwether plans executed inside query engines (WP16; ADR 010's second half: a plan is the same plan in every surface, including an engine). The input is the `ScanPlan` that `locate`, `latest`, `asof`, and `why` already emit, and the dataset schema it was planned against; the output is an artifact the user's engine runs. The plan a receipt cites is the plan the engine runs.

```rust
pub trait Adapter {
    fn engine(&self) -> &'static str;
    fn emit(&self, plan: &ScanPlan, schema: &[ColumnDef], opts: &EmitOptions) -> Result<Artifact>;
}

pub enum Artifact {
    DataFusion(datafusion::ScanConfig), // executable with the `datafusion` feature; JSON when printed
    Sql(String),                        // DuckDB
    Manifest(serde_json::Value),        // Spark, for shims/spark/bellwether_plan.py
}
```

`adapter("datafusion" | "duckdb" | "spark")` returns the adapter. Adapters never open an index, read a manifest, or see a `Store`. What a plan does not carry comes in `EmitOptions`, which `bellwether locate|latest|asof --emit` fills from the manifest at the plan's own sequence:

| Option | What it is |
|---|---|
| `row_offsets` | rows per row group of each planned file (manifest storage version 2's `rowgroup_rows`); DuckDB and Spark restrict a strict subset of a file's row groups by them |
| `partitions` | the partition columns and each planned file's values, from the partitioning the index records (Hive or a path template), typed by the source's exact cast |
| `keys_file` | a Parquet file of the queried keys: the engine semi-joins against it instead of an inline list. The adapter reads it once and refuses it unless its key set is exactly the plan's `In` list at the dataset types |
| `residual` | `full` (default: exactly the lookup's rows), `keys_only` (only the `In` membership; every row of a queried key in the planned row groups), or `none` (every row of the planned row groups). The two partial modes are labeled in every artifact |
| `footer_hint` | DataFusion: read each planned file's footer trailer once, set `metadata_size_hint` to the footer's exact length, and check the plan's row groups against the footer |
| `engine_pruning` | DataFusion: turn its statistics, page-index, and bloom-filter pruning back on; realized bytes are then at most the plan's |
| `ordered` | return rows in the plan's read order (`ScanPlan.order`, else entry order with row groups ascending), row by row |
| `relocate` | where the engine reads the plan's files (a prefix rewrite: the folder served over HTTP, or mounted elsewhere); the plan is unchanged |

## The residual: translated, never dropped

Every canonical value in the residual is decoded to a typed value at its column's dataset type with `bellwether_core::encoding::decode` (decimals at the dataset scale, timestamps in the dataset unit; `decode` is exact: `encode(decode(b)) == b` or a typed error), so no engine re-interprets a key through its own cast. The predicate is flattened into stages kept in the plan's order: row filters, then `NotOlderThan`, then `LatestPerKey`, then `Live`. `LatestPerKey` keeps every row at a key's newest timestamp (ties are all winners: `rank() = 1`, never `row_number() = 1`), and a null key or timestamp never wins. A residual an engine cannot evaluate exactly is a typed `AdapterError::Unsupported` naming the predicate, the column, why, and which engine can; never a looser query.

| Residual | DataFusion (`logical_plan`) | DuckDB (one SQL statement, one CTE per stage) | Spark (`bellwether_plan.read`) |
|---|---|---|---|
| `In` | `in_list` of typed literals; over 1,024 keys, a composite key, or `keys_file`: a left semi join against a `VALUES` table or the keys file | `IN (...)` of typed literals; over 1,024 keys or composite: semi join against a `VALUES` CTE, or `read_parquet('<keys_file>')` | `isin` of typed literals; long or composite: semi join against a typed relation or the keys file; a queried NaN matched with `isnan` |
| `Between` | inclusive range on typed literals | `BETWEEN` on typed literals | `between` on typed literals |
| `NotOlderThan` | a `VALUES` table of per-key bounds left-joined on the key; a row of a listed key is dropped when its timestamp is null or older | the same as a `VALUES` CTE and `LEFT JOIN` | the same as a typed relation and left join |
| `LatestPerKey` | `rank() over (partition by key order by ts desc) = 1`, null keys and timestamps dropped first | `QUALIFY rank() OVER (PARTITION BY key ORDER BY ts DESC) = 1` | a `rank()` window `= 1` |
| `Live` | tombstone column not null and not the tombstone value | `IS NOT NULL AND <> tombstone` | `isNotNull & !=` |
| `Text` | a UDF with Bellwether's own case folding and UAX #29 words | refused | refused |
| `ExprIn` | a UDF recomputing Bellwether's expression language bit for bit | refused | refused |
| `ContainsAny`, `ContainsAll` | a UDF over canonical element encodings (a map by its keys) | `list_has_any` over typed elements; `ContainsAll` one `list_has_any` per group, conjoined | `arrays_overlap` over typed elements; `ContainsAll` one per group, conjoined |
| `SemiJoin`, `Join`, `WindowPartition` | refused by every adapter: they belong to a consumer plan priced by relational replay, not to a lookup's scan | refused | refused |

Float keys are compared as their canonical bytes order them. DataFusion's `=`, `IN`, and hash join compare `-0.0` equal to `+0.0`, and the plan does not; the DataFusion adapter compares every float key through a function mapping the float to its canonical 8 bytes (`In`, `Between`, `Live`, the `NotOlderThan` join, and the `LatestPerKey` partition and order). DuckDB and Spark have no such function, so they run a float key only where SQL's equality gives the plan's answer and refuse the rest.

## What each engine can pin, and what it refuses

| | DataFusion 55 | DuckDB 2.0 | Spark 4.2 |
|---|---|---|---|
| Files | exactly the planned files | exactly the planned files (`read_parquet([...])`) | exactly the planned files (`spark.read.parquet(*files)`) |
| Row groups | pinned: one `ParquetAccessPlan` per file, planned row groups `Scan`, the rest `Skip`, with DataFusion's own pruning off and the scan behind a leaf no optimizer pushes a filter into | not pinned: DuckDB cannot be told which row groups to read. A strict subset of a file's row groups is a `file_row_number BETWEEN` filter on the planned row groups' rows (rows are exact); DuckDB may or may not use it to skip row groups, so its bytes are measured | not pinned: the shim keeps only the planned row groups' rows (rows are exact) but reads at the file level, and Spark's reader prunes row groups by its own statistics. Only planned-file access is claimed |
| Bytes | equal to the plan's (engine pruning off); at most the plan's (on) | measured at an HTTP range server: an upper bound of the plan's; never asserted equal | not claimed: only that no unplanned file is opened |
| Output order (`ordered`) | one file group in read order, rows sorted by read position and row number | `ORDER BY` read position and `file_row_number` | sorted by read position and row index |

Refusals, each typed and naming the next step (`emit for DataFusion`, unless stated):

- **DuckDB, a queried NaN.** DuckDB 2.0 skips a Parquet row group by min/max statistics, which by the Parquet specification leave NaN out, so `f IN (-1.0, NaN)` silently loses the NaN row (reproduced, also through a semi-join). The DuckDB adapter therefore refuses any `In` key list holding a NaN. This is a safety refusal: DuckDB does **not** support a NaN key through this adapter; DataFusion and Spark (whose shim matches a queried NaN with `isnan`, which Spark never pushes into the Parquet reader) return the row.
- **DuckDB and Spark, float zeros.** A key list, a `ContainsAny`/`ContainsAll` value set, or a `Live` tombstone holding one float zero in one sign only (both engines compare `-0.0` equal to `+0.0`, so the other sign would match), `NotOlderThan` bounds that differ between a key's two zeros, and a `Between` on a float column bounded by a zero or NaN (both engines order them differently from the plan).
- **DuckDB and Spark, `Text` and `ExprIn`.** Their case folding, word boundaries, and expression functions are not Bellwether's.
- **DuckDB, types.** A decimal over 38 digits (DuckDB reads it as a `DOUBLE`), a negative scale, a date before year 1, a `Date64` value that is not a whole day, a column name with a NUL, a column named like a `read_parquet` virtual column the statement needs (`filename`, `file_row_number`).
- **Spark, types.** A decimal over 38 digits or with a negative scale.
- **DataFusion.** A plan over two object stores (relocate the files to one); a column type with no exact text form in Arrow (the printed description could not be loaded back).
- **Every engine.** A column using the `__bellwether_` prefix of the helper columns; a strict subset of a file's row groups without row counts (`RowOffsetsNeeded`; a storage version 1 manifest records none, and the CLI then reads them from the footers); a relational residual.

A plan that does not belong to its inputs is a typed `AdapterError`, never a panic or an engine's error string: an unknown file (or one whose size changed), an out-of-range row group, a residual over a column not in the schema, canonical bytes at the wrong width, a malformed read order (`tests/corrupt.rs`).

## DataFusion (feature `datafusion`)

`datafusion::scan_config` builds the scan description; it is always built. With the feature, `ScanConfig::file_scan_config` builds the executable `FileScanConfig` (one `PartitionedFile` per file with `extensions = ParquetAccessPlan`, `metadata_size_hint` when the footer was read, partition values synthesized from the index's reading of each path), `logical_plan` the residual over it, and `execute` runs it. `CountingStore` wraps any `ObjectStore` and records, per object, the ranges DataFusion asked for (`requests`) and the ranges the store served (`fetches`), with an optional coalescing gap like a remote store's; `measure::classify` splits them against each file's own layout into footer bytes, column-chunk (data) bytes by row group, and other bytes (page indexes, bloom filters, coalesced gaps), so a caller measures its own run and never reads a profiler total.

`--emit datafusion` prints the description as JSON (`format: bellwether.datafusion/1`). It carries the plan, the dataset schema (Arrow type spellings), and the options it was built from; `datafusion::from_json` rebuilds the scan from them and refuses (`AdapterError::Description`) a description that differs from the rebuilt one in any field, so a description edited after it was printed never runs.

## DuckDB

`duckdb::sql` returns one statement (shape in the module docs): typed literals (`DECIMAL(p,s)`, `TIMESTAMP` in the unit, escaped strings, blobs), identifiers quoted, partition values from the index (DuckDB's own Hive inference types `007` as a number and cannot read path templates). Tests run it in the external `duckdb` CLI exactly as `bellwether-bench` does (`-no-agent -init /dev/null`, `AI_AGENT` stripped, any stderr an error).

## Spark

`spark::manifest` returns `{format: bellwether.spark/1, files: [{path, row_groups, rows}], residual, schema, ...}`; `shims/spark/bellwether_plan.py` has one function, `read(spark, manifest_path) -> DataFrame`. No Rust dependency on Spark; no cluster is started in CI (a local session runs the tests).

## CLI

```
bellwether locate|latest|asof <root> --keys K ... --emit datafusion|duckdb|spark
    [--keys-file-in-query] [--residual full|keys-only|none] [--ordered]
    [--footer-hint] [--engine-pruning] [--relocate FROM TO] [--out PATH]
```

The artifact is printed after the plan line (human or `--json`), or written to `--out`. Nothing is executed by the CLI: the artifact is text for the user's engine (threat model: no surface executes a query string). `--ordered` is refused, with the plan's own reason, when the plan has no read order; `--footer-hint` and `--engine-pruning` are refused for DuckDB and Spark; `--keys-file-in-query` is refused for a text lookup, which has no key file.

`--keys-file-in-query` with `latest` or `asof` takes a file of the keys that have a version: those lookups list only such keys, and a file holding another key is refused with that reason.

## Measured: planned versus realized bytes

Release build, 2026-10-27, Apple M3 Pro (12 cores, 36 GiB), macOS 26.4, local SSD; DuckDB CLI v2.0.0 through `httpfs` at the test's HTTP range server. `cargo test --release -p bellwether-cli --features datafusion --test emit -- --ignored --nocapture measured`: a bench corpus (10 files × 8 row groups × 20,000 rows, 256-byte payload, 200,000 entities), indexed by the CLI; each lookup emitted by the CLI, run as written, its rows equal to the oracle's, and every byte classified at the storage boundary against the files' own layouts. *Data* is column-chunk bytes; footers are counted apart; *other* is page indexes, bloom filters, and bytes between requested ranges.

| Lookup | Rows | Planned row groups / bytes | DataFusion data (asked = served) | DataFusion row groups touched | DataFusion other | DuckDB data | DuckDB row groups touched | DuckDB other |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `locate`, 10 keys | 208 | 56 / 313,912,797 | 313,912,797 | 56 | 0 | 313,912,797 | 56 | 973,889 |
| `locate`, 100 keys | 2,032 | 80 / 448,447,926 | 448,447,926 | 80 | 0 | 448,447,926 | 80 | 1,391,270 |
| `locate`, 1,000 keys | 19,872 | 80 / 448,447,926 | 448,447,926 | 80 | 0 | 448,447,926 | 80 | 1,391,270 |
| `latest`, 100 keys | 100 | 10 / 56,056,124 | 56,056,124 | 10 | 0 | 448,447,926 (8.0×) | 80 | 1,391,270 |
| `asof`, 100 keys | 66 | 12 / 67,267,426 | 67,267,426 | 12 | 0 | 319,545,895 (4.8×) | 64 | 1,113,012 |

DataFusion's data bytes equal the plan's in every run, also behind a store that coalesces ranges up to 1 MiB apart (identical figures; what it asked for stays the plan's). DuckDB reads every planned row group. For `locate` it read exactly the plan's data. For `latest` and `asof` it read row groups the plan skipped: those plans drop the row groups holding only a key's older versions, which the key's own statistics cannot rule out, and DuckDB did not skip them on the `file_row_number` restriction. Rows are exact either way. Spark has no byte figure: only that it opened no unplanned file. Footers: 52,430 to 74,900 bytes per run (one per planned file).

## Tests

| Test | What it proves |
|---|---|
| `tests/datafusion.rs` (feature) | every lookup shape of a corpus with every key type, composite prefixes, both float zeros and NaN, decimals at two scales, two timestamp units, null timestamps, ties, an all-null row group, Hive partition columns, a list and a text column, run from the description as printed (`from_json`): rows equal the reference evaluator's; data bytes equal the plan's, the planned row groups touched and no other, footers counted apart, nothing else read; engine pruning at most the plan's; a 100k-key keys file; a coalescing store's gap bytes counted apart from the requested data; partial residual modes |
| `tests/duckdb.rs` | the same shapes in the `duckdb` CLI (rows exact; the refused shapes refused by name); ordered output; bytes at the range server (planned row groups all touched, no unplanned file's data, the delta printed) |
| `tests/spark.rs`, `shims/spark/test_bellwether_plan.py` | the same shapes in a local Spark session with every unplanned file unreadable during the run (and a control run that denies a planned file and fails); ordered output; NaN keys inline and composite; a 20k-key keys file |
| `tests/residual.rs` | a reference evaluator over canonical bytes agrees with DataFusion's and DuckDB's typed translation on random rows |
| `tests/corrupt.rs` | typed refusals for plans that do not belong to their inputs; every flipped byte of a residual value refused exactly when it does not decode, and otherwise translated as the value it re-encodes to; an edited DataFusion description refused |
| `bellwether-cli/tests/emit.rs` | the CLI end to end: a bench corpus indexed, `locate`, `latest`, `asof`, `--live`, `--keys-file-in-query`, `--columns`, `--residual keys-only`, `--ordered`, `--footer-hint`, and `--relocate` emitted for each engine, each artifact as written executed, and its rows and exact `amount` total equal to the bench oracle's (`harness::oracle`) and to a scan of every corpus row |

Without an engine a test prints `SKIPPED:` and the reason; with `BELLWETHER_REQUIRE_DATAFUSION`, `BELLWETHER_REQUIRE_DUCKDB`, or `BELLWETHER_REQUIRE_SPARK` set it fails instead. In CI the `check` job installs the DuckDB CLI v2.0.0 (pinned by checksum) and requires DataFusion and DuckDB; the `spark` job installs Java 17 and PySpark 4.2.0 and requires Spark; both fail if any `SKIPPED:` is printed. `BELLWETHER_DUCKDB` and `BELLWETHER_SPARK_PYTHON` name the binaries.
