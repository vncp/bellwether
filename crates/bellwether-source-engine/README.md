# bellwether-source-engine

Work package WP20b (ADR 033). Query engines that own their storage (a warehouse or an analytics database) as Bellwether sources. The diagnosis of a slow engine query is about the data's shape, so it runs on the engine's lake twin (a folder of Parquet files that copies its tables) with `why --twin-of`; the cost is about the engine, so the engine measures it. This crate is the engine side: an adapter trait of three primitives, a `Source` over the engine's storage units with an identity, and two adapters.

```text
bellwether why ./orders/ --workload <workload> --twin-of 'clickhouse://reader@ch:8123/sales/orders?password=env:CH_PASSWORD' --gate in:100000
bellwether fix ./orders/ --twin-of "$ENGINE" --proposal <rewrite proposal> --scratch ./orders/_scratch
```

## The trait: three primitives, and only three

```rust
pub trait MeasuredEngine: Send + Sync {
    fn root(&self) -> &str;                    // the recorded root: no user, no parameters
    fn model(&self) -> &'static str;           // "<engine>.query_log"
    fn submit(&self, query: &str, tag: &Tag) -> Result<ResultHandle>;
    fn export(&self, query: &str, tag: &Tag, dir: &Path) -> Result<ResultHandle>;
    fn stats(&self, run: &ResultHandle) -> Result<QueryStats>;
}
pub trait EngineAdapter: Source + MeasuredEngine {}
```

- **`submit(query, tag)`** runs one query under a tag Bellwether chose (`bw-<run>-<role>`, `Tag`), read-only where the engine has the setting, with every cache a request can turn off turned off, and discards its rows.
- **`export(query, tag, dir)`** runs one query the same way and writes **that run's** rows as Parquet into a fresh folder, or refuses with `Error::ExportDenied` naming the permission to grant (the engine's export right, or write access to the scratch folder). The export is the measured run: there is never a second run of the same query under another tag, so a query that tells its runs apart (by its query id, the `bw-` prefix, or its output format) shows the measurement and the verification the same result.
- **`stats(run)`** reads that run's entry from the engine's **own** query log (`QueryStats`: rows and bytes read, peak memory, CPU, duration, result rows, units read of units total), waiting a bounded time for the log to record it. It fails when the query failed or the log has no entry, and refuses (`Error::MeasurementRefused`, with a stable `reason`) an entry that is not the run's: another text or user (`not_the_query_submitted`), more than one finished entry under the tag (`ambiguous_log_entry`), a run served from a cache or that turned one on (`cache_served`), or a missing statistic (`statistics_missing`, never read as 0). The facade also refuses statistics naming another tag (`tag_mismatch`) and a run whose logged result rows are not the rows it exported (`result_rows_differ`).

Nothing else crosses. No cost is computed here, none is accepted from a caller (there is no `measure import` and no field to carry one), and every number in a `QueryStats` is one the engine logged for a query Bellwether submitted under its own tag. Costs are `measured`, never `exact`, and the model names the log. An adapter creates nothing inside the engine: no table, view, index, user, or job. It runs the text it is given and its own fixed metadata statements, whose names are passed as parameters, never spliced.

**Least privilege.** The agent's text runs with every read privilege of the user Bellwether connects as, network table functions included (ClickHouse `url()` works under `readonly=2`). Connect as a user that may `SELECT` the twin's tables and read the query log, and nothing else.

The `Source` half gives the engine an identity: each storage unit plays a file of one row group, and `identity(engine)` is BLAKE3 over the root and every unit's path and identity string in path order (`docs/format.md`, "Query engine sources", with conformance vectors). `why --twin-of` records it beside the lake's manifest sequence; `fix` reads it before the runs and again after them on a fresh connection, and refuses a change (`source_changed`); the apply tier reads it again after its own runs and refuses a change (`data_changed`). `EngineScorer` implements WP20's `MeasuredScorer` over any adapter (peak memory per side, one sample, for evolve).

## Roots

```text
clickhouse://[user@]host[:port]/<database>[/<table>[,<table>...]][?secure=true&password=env:NAME&timeout=<s>]
kusto://<cluster>/<database>[/<table>[,<table>...]][?scratch=<storage folder URL>&endpoint=<url>&auth=default|none&timeout=<s>]
```

A secret is read from the environment only (`password=env:NAME`); a literal password is refused. Records and messages print the root without the user and the parameters. Database and table names are ASCII letters, digits, `_`, and `-`.

## The adapters

| | ClickHouse (reference) | Kusto (Azure Data Explorer) |
|---|---|---|
| Transport | HTTP interface, `reqwest` (the workspace's, rustls) | REST: `/v2/rest/query`, `/v1/rest/mgmt` |
| submit | the text as the body; `query_id` = tag, `readonly=2`, `default_format=Null`, `use_query_cache=0` (on every request), `use_query_condition_cache=0` (where the server has it) | `x-ms-client-request-id: bellwether;<tag>`; `request_readonly`, `notruncation`; the body read and dropped, the completion frame kept |
| stats | `system.query_log` by `query_id` (after `SYSTEM FLUSH LOGS` where allowed, else the flush interval): `read_rows`, `read_bytes`, `memory_usage`, `query_duration_ms`, `result_rows`, CPU and selected parts from `ProfileEvents`; the one finished entry must hold the exact text and the user; `QueryCacheHits`, `QueryConditionCacheHits`, or `Settings['use_query_cache']` on is `cache_served`. A query longer than the log keeps (`log_queries_cut_to_length`) is refused | `.show commands-and-queries` by `ClientActivityId`: `ScannedExtentsStatistics`, `CacheStatistics`, `MemoryPeak`, `TotalCpu`, `Duration`, `ResultSetStatistics`, every scan and cache statistic required; the one entry must hold the exact text (`Text` keeps 64 KB). Unverified against a cluster: whether the log's result rows for an `.export` are the query's; if not, `fix` refuses (`result_rows_differ`) rather than certifies |
| export | the same request with `FORMAT Parquet`, one run, streamed into `<dir>/part-0.parquet` and checked to be whole Parquet; access denied (code 497) is `ExportDenied` naming the grant | `.export to parquet (h@"<scratch>/<dir>;impersonate") <\| <query>` under the run's own request id, written by the cluster into the storage folder `scratch=` names, which the local scratch folder must mount; 401/403 or a permission message is `ExportDenied` |
| units, identity | one per partition of each MergeTree table (active parts in `system.parts`): `v1;rows=<n>;blocks=<lo>-<hi>;mutation=<v>`, changed by an insert, a delete, or a mutation and not by a background merge | one per extent (`.show table extents`): `v1;rows=<n>;extent=<id>`, changed by ingestion, deletion, and a merge |
| credentials | the user in the root, `password=env:NAME` | the Azure Identity chain in order: service principal secret (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`), workload identity, managed identity, Azure CLI / Azure Developer CLI; `auth=none` for a local emulator |
| tests | `tests/clickhouse.rs` against a server: CI starts `clickhouse/clickhouse-server` (pinned by digest) and the tests must run there; locally they print `SKIPPED:` without `BELLWETHER_CLICKHOUSE_URL` | unit tests over canned responses (`src/kusto.rs`); no cluster test (none is available to CI) |

The Kusto adapter is behind the default feature `kusto` (`azure_identity`, `azure_core`, `tokio`; MIT).

## Which numbers are counts, measured, or exact

| Quantity | What it is |
|---|---|
| unit rows, unit count, engine rows in the twin's freshness line | counts from the engine's own listing |
| everything in `QueryStats` | **measured**: the engine's log for one run; CPU, duration, and memory vary between runs of the same query |
| `RunCost.bytes` on an engine replay | the log's `read_bytes`, **measured** (`exact: false`, model `<engine>.query_log`) |
| the twin's prediction (`predicted_rows`, `predicted_row_groups`) | **exact** for the lake copy, from its footers; says nothing exact about the engine |
| prediction ratios | per pair: that pair's read rows / predicted rows, three places, rounded half to even; never summed across pairs |

## Scratch governance

Exports land only under the scratch folder the operator names (`--scratch`, or the MCP server's flag; no tool argument names a folder). It must resolve, on its canonical path (symlinks followed), to a hidden folder (a name starting with `_` or `.`, not the index folder) under the lake root, so the exports stay under the lake's account and are never read as lake data; otherwise `invalid_argument` on `scratch`, naming one that would do. Missing folders on the way, and each run's fresh folder, are created with the lake root's permissions. The run's folder is deleted when the run ends, whatever its outcome, unless `--keep`, and then a refusal or error says where the files are. Records keep hashes and counts, never rows. A scratch folder Bellwether cannot write is `export_denied` naming write access to it.

## Another engine is one file

An adapter is one file implementing `MeasuredEngine` and `Source`; `connect` gains one match arm and `EngineKind` one variant. A library caller can also pass its own adapter without touching this crate (`EngineOptions::engine_adapter`). Three sketches follow (documentation, not code in this crate; ADR 033 defers further adapters until a user asks). Each keeps the rules above: the tag is Bellwether's, the numbers are the engine's log entry for that tag, and nothing is created in the engine except where the engine's export itself requires a target.

### BigQuery

```rust
//! bigquery.rs: REST v2 (jobs.insert, jobs.get), INFORMATION_SCHEMA.JOBS, EXPORT DATA.
pub struct BigQuery { root: String, project: String, dataset: String, tables: Vec<String>,
                      location: String, scratch_uri: String /* gs://bucket/prefix, from scratch= */,
                      client: Http, token: TokenSource /* Google ADC: env, workload identity, gcloud */ }

impl MeasuredEngine for BigQuery {
    fn model(&self) -> &'static str { "bigquery.jobs" }
    fn submit(&self, q: &str, tag: &Tag) -> Result<ResultHandle> {
        // jobs.insert with jobReference.jobId = tag, configuration.query = { query: q,
        // useLegacySql: false, useQueryCache: false }, dryRun: false; poll jobs.get until DONE;
        // the result rows are never fetched. A DML or DDL statement is refused before submit:
        // the job's statementType from a dryRun must be SELECT.
    }
    fn stats(&self, run: &ResultHandle) -> Result<QueryStats> {
        // SELECT total_bytes_processed, total_slot_ms, TIMESTAMP_DIFF(end_time, start_time, ...),
        //   query_info.performance_insights, job_stages[...].records_read, error_result
        // FROM `region-<location>`.INFORMATION_SCHEMA.JOBS WHERE job_id = @tag
        // read_rows: summed records_read of the input stages; read_bytes: total_bytes_processed;
        // peak memory: not logged, so 0 and stated; cpu_ns: total_slot_ms * 1e6.
    }
    fn export(&self, q: &str, tag: &Tag, dir: &Path) -> Result<ResultHandle> {
        // One job: EXPORT DATA OPTIONS(uri = '<scratch_uri>/<dir>/part-*.parquet',
        // format = 'PARQUET') AS <q>, jobId = tag, so stats(run) reads this very job;
        // accessDenied on the bucket or bigquery.jobs.create is ExportDenied naming the IAM
        // role; then list `dir`, the local mount of scratch_uri, as Kusto does.
    }
}
impl Source for BigQuery {
    // units: INFORMATION_SCHEMA.PARTITIONS (one per partition_id; an unpartitioned table is one
    // unit); identity "v1;rows=<total_rows>;modified=<last_modified_time>".
}
```

### Snowflake

```rust
//! snowflake.rs: SQL API v2 (/api/v2/statements), QUERY_HISTORY, COPY INTO a stage.
pub struct Snowflake { root: String, account: String, database: String, schema: String,
                       tables: Vec<String>, warehouse: String, stage: String /* from scratch= */,
                       client: Http, auth: KeyPairJwt /* private key path from env:NAME */ }

impl MeasuredEngine for Snowflake {
    fn model(&self) -> &'static str { "snowflake.query_history" }
    fn submit(&self, q: &str, tag: &Tag) -> Result<ResultHandle> {
        // POST /api/v2/statements { statement: q, parameters: { QUERY_TAG: tag,
        // USE_CACHED_RESULT: "FALSE", MULTI_STATEMENT_COUNT: "1" } } under a role granted SELECT
        // only (the read-only setting Snowflake has); keep the statementHandle, fetch no
        // partitions of the result.
    }
    fn stats(&self, run: &ResultHandle) -> Result<QueryStats> {
        // SELECT rows_produced, bytes_scanned, partitions_scanned, partitions_total,
        //   total_elapsed_time, execution_status, error_message
        // FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER()) WHERE query_tag = :tag
        // (ACCOUNT_USAGE lags by up to 45 minutes; the table function does not).
        // units_read / units_total: partitions_scanned / partitions_total (micro-partitions).
    }
    fn export(&self, q: &str, tag: &Tag, dir: &Path) -> Result<ResultHandle> {
        // One statement: COPY INTO @<stage>/<dir>/part FROM (<q>) FILE_FORMAT = (TYPE = PARQUET)
        // HEADER = TRUE, QUERY_TAG = tag, so stats(run) reads this very statement;
        // "Insufficient privileges ... stage" is ExportDenied naming USAGE/WRITE on the stage;
        // then list `dir`, the stage's local mount.
    }
}
impl Source for Snowflake {
    // Snowflake does not list micro-partitions by id, so a table is one unit with identity
    // "v1;rows=<ROW_COUNT>;altered=<LAST_ALTERED>;bytes=<BYTES>" from INFORMATION_SCHEMA.TABLES.
}
```

### Postgres

```rust
//! postgres.rs: EXPLAIN (ANALYZE, BUFFERS) as the log entry, COPY for the export. Reuses
//! bellwether-source-sql for the connection (ADBC) and for units and identity.
pub struct Postgres { root: String, sql: bellwether_source_sql::SqlSource, log: Mutex<HashMap<Tag, Value>> }

impl MeasuredEngine for Postgres {
    fn model(&self) -> &'static str { "postgres.explain_analyze" }
    fn submit(&self, q: &str, tag: &Tag) -> Result<ResultHandle> {
        // BEGIN READ ONLY; SET LOCAL application_name = tag; then
        // EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON) <q>; ROLLBACK. Postgres keeps no
        // per-execution log (pg_stat_statements aggregates), so the engine's own report for
        // this run, the EXPLAIN ANALYZE JSON, is kept under the tag.
    }
    fn stats(&self, run: &ResultHandle) -> Result<QueryStats> {
        // From the kept plan: read_rows = summed "Actual Rows" x "Actual Loops" of the scan
        // nodes; read_bytes = (Shared Hit + Read Blocks) x block_size; duration = Execution
        // Time; peak memory = the largest "Peak Memory Usage" of sort and hash nodes;
        // units_read = partitions scanned of partitions total.
    }
    fn export(&self, q: &str, tag: &Tag, dir: &Path) -> Result<ResultHandle> {
        // One run: EXPLAIN (ANALYZE, BUFFERS) cannot return rows, so the export runs <q> once
        // through the source's ADBC connection (inside BEGIN READ ONLY), writes its rows as
        // Parquet into `dir`, and keeps pg_stat_statements' delta for this one call as its log
        // entry under the tag. Permission denied on a table is ExportDenied naming GRANT SELECT.
    }
}
// impl Source: delegate to the SqlSource (declared partitions or key ranges as units, the
// statistics checksum or a version column as identity; docs/format.md "SQL sources").
```

## Tests

- `tests/clickhouse.rs`: submit is read-only and the log holds the statistics of that run; a write is refused by the engine's own read-only setting; export is the measured run (one log entry under its tag, its result rows the rows exported), writes whole Parquet, refuses a non-empty folder, and is `ExportDenied` for an unwritable scratch folder and for a user without SELECT; a run that turns the query cache on or is served from it is refused (`cache_served`, the review's probe), and a user profile's caches never reach a run; another client's entry under the tag is refused (`ambiguous_log_entry`, `not_the_query_submitted`); the identity follows inserts and mutations and ignores merges; units read back exactly at their declared types.
- `tests/conformance.rs`: the `format.md` vectors (tags, ratios, an identity, the canonical bytes and hashes of the ADR 033 record fields) and records of every kind written before ADR 033 reading and writing back to the same bytes.
- `src/kusto.rs` and `src/uri.rs` unit tests: response parsing over canned frames, the `.show queries` row, the export refusal, root parsing and redaction, `password` only from `env:`.
- The loop through the facade with an in-process adapter: `crates/bellwether/tests/engine_twin.rs`; CLI and MCP byte identity: `crates/bellwether-mcp/tests/engine_tools.rs` (in process) and `crates/bellwether-cli/tests/engine_tools.rs` (the binary against ClickHouse).

## Deliberate-break checks

Each was made once in a scratch copy with a private target directory; the named test failed (the PR lists the runs).

| Break | Test that fails |
|---|---|
| sort units by etag instead of path in `identity` | `tests/conformance.rs::an_engine_identity_hashes_the_root_and_the_sorted_unit_list` |
| serialize `Receipt.attest_only` when false | `tests/conformance.rs::records_written_before_adr_033_keep_their_bytes_and_hashes` |
| drop `readonly=2` from `submit` | `tests/clickhouse.rs::submit_is_read_only_and_the_log_holds_its_statistics_by_tag` |
| fold merges into the identity (use part names, not block ranges) | `tests/clickhouse.rs::the_identity_follows_inserts_and_mutations_and_ignores_merges` |
| map access denied (497) to a plain source error | `tests/clickhouse.rs::export_is_the_measured_run_and_is_refused_without_the_permission` |
| drop the cache refusal from `stats` | `tests/clickhouse.rs::a_run_that_turns_a_cache_on_or_is_served_from_one_is_refused` |
| drop `use_query_cache=0` from the requests | `tests/clickhouse.rs::a_user_profile_s_caches_never_reach_a_measured_run` |
| drop the text-and-user match or the one-entry check | `tests/clickhouse.rs::a_log_entry_another_client_wrote_under_the_tag_is_refused` |
