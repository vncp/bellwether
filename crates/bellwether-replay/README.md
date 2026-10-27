# bellwether-replay

Work package WP6. This crate replays recorded runs to answer "what would these runs have read" under:

- the current index
- a hypothetical key spec
- a baseline without an index
- a proposal

Every cost is an exact sum of row-group statistics under a stated plan, never an estimate (ADR 005). The crate owns **invariant 3**: replay is deterministic.

Replay reads very little. The current index costs no data reads at all. A hypothetical spec reads only its key columns, one row group at a time, to build transient segments. Everything else comes from `Source::row_groups`:

- **Parquet:** that is the footers.
- **CSV and JSON lines:** the first `row_groups` call per file scans each chunk once to count rows and compute statistics, which reads the whole file. Its result is cached by the source.

## What it provides

| Module | Item | Purpose |
|---|---|---|
| `workload` | `WorkloadRecorder`, `KeyInput`, `Workload`, `record_bench_workload` | Records each run's key set in the cache under its BLAKE3, plus a `workload.json` manifest. Reads `bellwether-bench`'s workload format directly. |
| `keys` | `RunKeys`, `RunKeysBuilder` | One run's distinct, non-null key tuples, canonical at the **dataset** types, and their cache payload. |
| `catalog` | `Catalog` | Every live row group of one manifest version, under the dataset schema. Sizes and statistics come from `Source::row_groups`. Its digest names the data. |
| `plan` | `PlanSource`, `SegmentPlanner`, `servable`, `text_residual` | The plan-building seam, and this crate's implementation of it over core segments. A text run (B5; query kinds `contains`, `has_token`, `has_prefix`, its key set the words, substrings, or prefixes over one string column) plans through `PlanSource::plan_text` in both planners, so a priced text key plans what the built one does (`tests/text.rs`); `Replayer::text_stats` counts a text key's distinct tokens exactly. |
| `build_plan` | `impl PlanSource for bellwether_build::Planner` | The build crate's planner, the one the index answers queries with, behind the same seam. |
| `transient` | `TransientIndex`, `TransientCache`, `TransientLimits` | A hypothetical spec's segments, built over key reads, and their cache. An `expr` key's index derives its virtual column per row group from the expression's input columns (bounded like any transient build); `BuildStats::null_rows` and `TransientIndex::distinct_keys` give `why`'s `expr` column. |
| `baseline` | `Baseline::{RawScan, ParquetStats}` | Costs without a Bellwether index. A text run reads every row group under both: no statistic says which words a string holds. |
| `layout` | `LayoutModel`, `replay_layout` | Layout proposals under a stated model, `exact: false`. |
| `sketch` | `TransientSketch` | What a `sketch` sidecar would hold, computed now: reads only the sketch's item columns, one row group at a time in catalog order, through core's `FileSketcher` (the code a build writes segments with), so its registers equal a committed sketch's over the same files (the facade's `tests/sketch.rs` checks predicted equals built). It reports the files, row groups, and rows it read. **Approximate**, like every sketch, and never handed to a planner. Memory: one row group's item columns, one file's open sketch, and the running totals (`2^p` registers and `k` slots per item). |
| `replay` | `Replayer`, `Scored`, `write_replay` | Pins one manifest, scores proposals, and writes `Replay` records through the ledger. |
| `report` | `report`, `TemplateSummary` | Per-run rows. Per-template p50/p95/max of keys, files, row groups and bytes. |
| `attribution` | `Replayer::replace_node`, `Replayer::interaction` | Counterfactual hooks for WP13 (ADR 024). |
| `error` | `ReplayError` | The refusals a caller must tell apart (below). |
| `slo` | `REPLAY_RUN_P50_BUDGET` | 5 ms for the p50 run under one cached candidate (`docs/evolve.md` section 12). |

## Workloads

`record(run_id, KeyInput::Batch(&batch) | KeyInput::Parquet(path))` treats every column of the input as a key column.

**Encoding.** Each value is converted to the dataset schema's type and canonically encoded (ADR 016). A `UInt32` file column under a dataset `Int64` encodes with the Int tag. A value that does not convert fails the run, and nothing from it is kept.

**Nulls.** A run tuple that contains a null in any key column is dropped, under ADR 014: a probe containing a null matches nothing, by SQL three-valued logic. The drop is counted in the run's `null_tuples`. The build crate's planner applies the same rule.

**Sorting.** Tuples are sorted and deduplicated.

**Templates.** A run's `template` groups it in reports. Without one it is the run's key-column set, `columns(<key columns>)`. That is a grouping by columns, not by query; a recorder that knows the query or pipeline step a run came from passes it as the template.

**Query kind.** Every run records a `query_kind` (`locate`, `latest`, `asof`, or `window`; `asof` runs also carry `asof`, the instant in the timestamp column's dataset unit, omitted for the other kinds so existing hashes stay stable). A `latest` or `asof` run may also carry `lookback`, a window length in the same unit (`RunQuery::Lookback`, contract v3 backlog #38). A `latest` lookback asks for every version of each key in its own window `[newest − lookback, newest]`, `newest` being the key's newest live version, planned through `Lookup::Lookback`; an `asof` lookback asks for the rows of the keys in `[asof − lookback, asof]`, planned as a window run is. Both are measured on the column the lookup verbs take (`planned_time_column`): the serving temporal spec's timestamp, else, through the build planner, a `latest` lookback's complete per-key bounds sidecar (`PlanSource::lookback_column`) and an `asof` lookback's default window column (`PlanSource::window_column`: per-key bounds, range keys, partition columns); with none, the run reads every row group. `Replayer::planned_time_column` reports that column, so `why` refuses a `--latest-by` on another one. The run does not record that column (a record version 3 limit; a new field waits for record version 4): a lookback is taken to be measured on the column the current configuration plans the run on, and a candidate that plans it on another column gets the window converted outward to that column's unit (`converted_lookback_window`), in replay and in `fix`'s verify alike; with no time column planned now, the candidate's own column is used. A `window` run (`RunQuery::Window`, recorded by `why --window`) carries `window`, a time column and inclusive bounds, and is planned through `Lookup::Window`: the key specs, the specs bounding the time column, a temporal spec's version chains on it, and path values all prune it. The build planner and `SegmentPlanner` plan every kind alike (`tests/wp4_planner.rs` checks both against each other); `SegmentPlanner` has no sidecars, so it plans a lookback through temporal specs only. `lookback` and `window` are omitted when absent, so existing hashes stay stable; a `locate` run with a lookback, a negative one, or a window beside any other kind is refused. A workload naming any other kind is refused with `Error::UnknownKind` (contract v3 backlog #14).

**Bitemporal runs (M5).** A run with `query_kind` `bitemporal` (`RunQuery::Bitemporal`, recorded by `why --valid-by --known-by`) asks per key for the version valid at `valid` as known at `known`, each optional, in their columns' dataset units. It is planned through a bitemporal spec (`temporal2:<keys>@<valid>,<known>`, committed or transient); a configuration with none prices it as a scan of every live row group. `Replay.configuration` lists the identities of the specs that formed the current configuration, and a proposal's `sidecars` are priced together with its spec.

**Cache entries.** Each run's payload goes to `cache/<blake3>.bin`. The payload is `BWKEYSET`, a version, a JSON header with columns, key types and encoding version, then length-prefixed elements per tuple. A reader refuses all of these:

- a truncated payload
- out-of-order or duplicate tuples
- malformed or null elements
- trailing bytes
- a newer payload or encoding version (`Error::UnsupportedVersion`)

Every read checks the BLAKE3 of the bytes it got against the hash it asked for, whatever `KeySetSource` served them.

**Workload hash.** The hash is the BLAKE3 of the canonical JSON of the `WorkloadManifest`. The manifest holds the format, the encoding version, and for each run sorted by id:

- id
- template
- query kind (and `asof` instant when `query_kind == "asof"`, `lookback` for a lookback run, and `window` for a window run)
- columns
- key types
- key count
- dropped-null count
- key-set hash

The same keys therefore hash the same whether they arrive as a batch or a Parquet file, and in any run order. The manifest bytes are stored in the cache under that hash, so a `Replay`'s `workload` hash resolves to everything needed to re-run it.

**`workload.json`.** This is the manifest plus its hash and key-set p50/p95/max. Reading it back recomputes both and refuses a file that disagrees.

**Bench format.** `record_bench_workload` reads `bellwether-bench`'s `workload.json` (`bellwether-bench-workload/1`) in two passes:

1. The first pass writes nothing. It verifies:
   - every run's hash: BLAKE3 over its sorted `u64` keys as 8 big-endian bytes
   - every run's key count
   - every run id's uniqueness
   - every run file's location inside the corpus
   - the workload hash: BLAKE3 over the run hashes in order
   - the corpus id, when `corpus.json` is beside the workload file
2. The second pass records each run, re-verifying its hash as it reads it again.

A mismatch is a `ReplayError::BenchMismatch` that names the run. A mismatch found in the first pass, even on the last run, leaves the recorder and the key-set sink untouched.

The bench hashes are `u64`-specific. The replay hashes cover canonical encodings of any key type. Both are checked, and they differ by design.

## Plans

A run keyed on an expression (its one key column named by an expression over dataset columns, recorded normalized) is served only by an `expr` key over that expression, and only as a point lookup; its residual is `ExprIn`. `tests/expr.rs` checks that the replay's prediction equals the build planner's plan per run (files, row groups, bytes) for exact and bloom `expr` keys.


`PlanSource` is the surface replay plans through. The build crate's `Planner` implements it (`build_plan`), and so does this crate's `SegmentPlanner`; see "The two planners" below.

```rust
pub trait PlanSource: Send + Sync {
    fn manifest_version(&self) -> String;  // sequence string (ADR 020)
    fn specs(&self) -> Vec<KeySpec>;
    fn plan(&self, specs: &[KeySpec], keys: &RunKeys) -> Result<ScanPlan> {
        self.plan_query(specs, keys, &RunQuery::Locate)
    }
    fn plan_query(&self, specs: &[KeySpec], keys: &RunKeys, query: &RunQuery) -> Result<ScanPlan>;
    fn with_transient(&self, t: &[Arc<TransientIndex>]) -> Result<Box<dyn PlanSource + '_>>;
}
```

Both planners follow the same rules, and `tests/wp4_planner.rs` holds them to identical plans:

- **Serving.** A spec over columns `P` serves a run keyed on columns `C` in two cases, and no other:
  - (a) Every column of `P` is in `C`. Each tuple is projected onto `P` in `P`'s order and probed. This applies to exact, temporal and bloom specs, and to range specs, where `P` is one column.
  - (b) `C` is a strict prefix of `P`, and the spec is exact or temporal. Each tuple becomes a prefix lookup.
- **Float zero.** A float-zero element expands to both the `+0.0` and `-0.0` encodings.
- **Intersection.** Specs are intersected per key tuple, then the results are unioned over tuples.
- **Spec lists.** An empty spec list means a raw scan. A non-empty list in which nothing serves the run is an error.
- **Committed specs.** A committed spec plans only if it is declared exactly as given and complete at the pinned sequence (ADR 012), with live segments whose row-group ranges cover every live row group. A plan that lists a spec failing this is refused with `Error::KeySpec`, whether or not the spec serves the run; a plan that does not list it is unaffected. Neither planner refuses at construction.
- **Transient specs.** A transient index covers every live row group by construction. Each of its segments records the row groups it processed, keys or not, in its own bytes (core's `SegmentWriter::set_rowgroup_range`). So a row group whose key columns are all null is covered without holding a key, and the build planner checks coverage from those ranges before taking the segments.
- **Types.** Keys are refused when recorded under another type than the dataset declares. So is a transient index whose segments record other key or timestamp types. A probe at another decimal scale or time unit would silently miss.
- **Residual** (ADR 011). Every plan carries `In` over the run's key tuples, with float zeros in both encodings. An empty key set plans zero row groups with an empty `In`.
- **Row-group ids.**
  - Tombstoned ids are dropped.
  - An id that is neither live nor tombstoned is an error.
- **Committed reads.** Committed segments are read by range through the pinned `Snapshot` (invariant 4, ADR 018).

Replay checks every plan against the catalog before costing it, so a misreported plan is an error, never a wrong number:

- the manifest version matches
- every file is in the catalog and appears once
- every row group is live
- each entry's bytes are the exact sum of its row groups' compressed sizes
- the totals agree

A run's cost has these parts:

- `files`: the plan's entries
- `row_groups`: the selected row groups
- `bytes`: the sum of `RowGroupMeta.bytes` (Parquet `compressed_size()`)
- `keys`: the distinct tuple count

**Catalog digest.** `Catalog` computes it with `bellwether_source::catalog::digest`, the one definition a certificate cites too (`docs/format.md`, "Catalog digest"). The digest covers everything replay reads about the data:

- the manifest version
- the dataset schema
- every file's fingerprint and row-group ids, with each row group's footer report (rows, bytes, byte range, column statistics)
- the tombstoned ids

It keys the transient cache together with the full spec and its key and timestamp types. An unindexed folder has manifest version `"0"`, so its digest, not the version, names its data.

### Membership runs (B4)

A run recorded with `RunQuery::ContainsAny` or `ContainsAll` (`query_kind` `contains_any` / `contains_all`) looks up one list column; its values are encoded at the column's element type. Recording refuses a membership run on a flat column or on several columns, a list column in any other kind of run, and a `contains_all` run with a null value (no list holds a null, ADR 014). Both planners plan it by the build planner's `Lookup::ContainsAny` / `ContainsAll` rules through `list:<column>` alone (`serves_query`), with the same residual (`membership_residual`), so predicted equals planned (`tests/list.rs`: the build lookup, committed segments, transient segments through WP4, and `SegmentPlanner` agree run by run; the unindexed replayer prices every run the same). A transient list key measures each row group's distinct elements. Both baselines read every live row group for a membership run (`BaselinePlanner::plan_membership`), and so does an unserved membership run under `Replayer::with_unserved(Baseline::ParquetStats)` (its Replay is labelled as priced by min/max): Parquet keeps a list's statistics on its nested leaf, which the catalog reports as unknown, and a run's values are elements, never compared with a column's bounds.

### Aggregate runs (B7)

A run recorded with `RunQuery::Aggregate(RunAggregate { value, functions })` (`query_kind` `aggregate`; `RecordedRun.aggregate`, omitted for every other run so existing workload hashes are unchanged) asks `fn(value) group by key where key in (...)`. Recording refuses a value column that is a key column or cannot be aggregated. Both planners price it through `PlanSource::plan_aggregate`, the build planner's `aggregate` rules over a built or transient agg key (transient cells are written by the build's writer, sized by the same cell estimate): the run costs the row groups the counts do not prove answered, or, when no agg key serves it, a plain locate of its keys. Scoring an agg proposal without `exact:<key>,counts` complete is refused, naming the propose step. Tests (`tests/agg.rs`): predicted equals planned on both planners over a built and a transient agg key, against `Planner::aggregate` and an oracle over the decoded rows, including a finer lookup over a coarser agg key; the locate fallback; the refusals.

### Live runs (B8)

A `latest` or `asof` run (or a bitemporal one) recorded with a tombstone (`RecordedRun.tombstone`, `RunBitemporal.tombstone`; omitted otherwise, so existing workload hashes are unchanged) is `RunQuery::Live(RunLive { asof, tombstone })`: `arg_max(ts, *) by key` then `where not <tombstone>`. Both planners plan it through a temporal (or bitemporal) key on the run's columns, pruned by its tombstone sidecar exactly when the key declares that tombstone (the build planner's `plan_ordered_live`); the transient index builds the sidecar's segments too, and `TransientIndex::newest_tombstones` gives `why`'s `tombstone` column. `tests/tombstone.rs` checks predicted equals planned, with and without the sidecar, temporal and bitemporal.

## Configurations

`Replayer` pins one manifest version. It is built three ways:

- `for_snapshot`: a committed index
- `unindexed`: a folder, using `Catalog::discover`
- `new`: any `PlanSource`

Configurations:

- **Current.** The manifest's declared specs that are complete at the pinned sequence. With none, every run is unserved (below). A declared spec that is not complete is not current: the index cannot plan with it yet.
- **Hypothetical** (`replay_specs`). Any specs. Those the index cannot plan with, including a declared spec that is not complete, are built as transient segments and planned through `PlanSource::with_transient`. A configuration holds every hypothetical index it needs at once, because one plan intersects them.
- **Unserved runs.** A run that no spec of a configuration serves is priced under the replayer's unserved baseline (`Replayer::with_unserved`):
  - `RawScan`, the default: every live row group the planner cannot exclude (a window run's partitions). The index cannot narrow it.
  - `ParquetStats`: of those, the row groups the `ParquetStats` baseline (below) keeps for the run's keys and window: what any Parquet reader skips to without an index. Both keep every row group holding a key, so their intersection does (invariant 1). An `asof` or `latest` run that no temporal spec serves is planned on its keys alone: a recorded run names no time column, so two callers could not agree on an instant bound. The facade sets it on every Parquet replayer, indexed or not, so a predicted cost equals the realized one.
- **Key proposal.** The current specs, plus the proposed spec, which replaces a current spec of the same `KeySpec::name`. A proposed spec that serves none of the workload's runs is refused, never reported as "no change".
- **Layout proposal.** Costs come from the layout model: `exact: false`. The `Replay` names the model in `model`, the statement `LayoutModel::describe` gives (below).
- **Rewrite proposal.** Refused. Its cost depends on the rewritten plan, and a `RewriteShape` states that plan in prose (contract v3 backlog #2).
- **Baselines.**
  - `RawScan` reads every row group.
  - `ParquetStats` keeps a row group when some key tuple passes, for every key column, two tests: the footer min/max, and the row group's embedded split-block bloom filter (below). A NaN probe is never pruned by range or by a filter.
  - A window run: `ParquetStats` also skips a row group whose footer range for the window's time column lies wholly outside the window; `RawScan` still reads every row group.

**`ParquetStats` filter probing.** The filter is probed with the PLAIN value of the key converted to the file column's physical type (ADR 008 amendment):

- **`INT32` and `INT64`:**
  - integers, unsigned ones as their bit pattern
  - dates: days, or milliseconds for `INT64`
  - decimals, rescaled exactly to the file's scale
  - times and timestamps, converted exactly to the file's unit
- **`FLOAT` and `DOUBLE`:** IEEE bits. Both zeros are probed.
- **`BYTE_ARRAY`:** string and binary bytes.
- **`FIXED_LEN_BYTE_ARRAY`:**
  - decimals as big-endian two's complement of the column width
  - `Float16` as two little-endian bytes
  - fixed binary as its bytes

A value the file column cannot hold fails the probe. A run column whose filter has none of these shapes is refused with `ReplayError::UnprobeableFilter`: `BOOLEAN`, `INT96`, a decimal stored as `BYTE_ARRAY`, or a `Decimal256` dataset column. The baseline is never silently weaker than `docs/benchmark.md` defines it.

The baseline reads its filters once, after verifying each file's fingerprint on the opened handle, and **holds every embedded filter of every run column of every file** for its lifetime. A replayer pricing unserved runs under `ParquetStats` reads a row group's filters only when its footer min/max (and the window) keeps it, one file at a time, and drops them after the run (ADR 018; `Replayer::bloom_filters_read` counts them). A filter it cannot probe exactly is treated as absent there, which keeps the row group; only the bench baseline refuses it.

## The Replay record

`Replay` follows contract v2.1 (`RECORD_VERSION` 2):

- **`inputs`** is the catalog digest. Everything else a replay reads is named elsewhere in the record or derived from what the digest names:
  - the key sets, by the workload hash
  - the proposal, by its hash
  - transient segments, from the data the digest names

  Committed segments are not listed. A manifest version names them within a store, and how an index is split into segments never changes a cost, so listing them would make two equal answers differ. An unindexed folder keeps manifest version `"0"`; its digest tells two folders apart, and one folder before and after a file lands.
- **`model`** is present exactly when a proposed cost is inexact. That is a layout replay over a workload with runs, and the model is the layout model's statement. A key proposal's replay is exact throughout and names no model. Current costs are always exact.

The ledger enforces these rules on `put`. `write_replay` checks them first, against the proposal it resolves itself.

**`unserved`** is `"parquet_stats"` when some run, under the current configuration or the proposal, was unserved and priced under `ParquetStats` (above); absent otherwise, so a record that prices nothing that way keeps the bytes it had before the field existed. The replayer sets it; the ledger stores it as given.

## Errors

Refusals are `ReplayError` variants, and every refusal happens before anything is written: to the ledger, to the key-set sink, or to a recorder.

| Variant | When |
|---|---|
| `RewriteNotScorable` | a Rewrite proposal is scored |
| `SpecServesNoRun` | a proposed spec serves none of the workload's runs |
| `ProposalMismatch` | `write_replay` finds a mismatch with the proposal it cites: another body than the scored one (`Scored::body`), no inputs, an inexact current cost, or costs or a model that contradict the proposal's kind |
| `NotAProposal` | the hash to score is another record kind |
| `BenchMismatch` | a bench workload does not verify |
| `ClusterKeysExceeded` | the layout model's cluster-key map passes its cap |
| `UnprobeableFilter` | the statistics baseline meets a filter it cannot probe exactly |

`ReplayError` converts into the shared `Error` as the variant that fits:

- `LedgerRule` for rewrite, mismatch and not-a-proposal
- `KeySpec` for a spec that serves no run
- `Source` for bench and filter problems

`write_replay` resolves the proposal from the ledger itself before writing.

## Layout model, version 1

A layout proposal's costs are computed exactly under these statements; `LayoutModel::describe` names every one:

1. **Sort.** Rows are sorted by `cluster_by`, ascending, nulls first, with ties kept in current order.
2. **Repack.** The sorted rows are re-packed into row groups of `rows_per_row_group`, and row groups into files of `row_groups_per_file`. The defaults are the current mean sizes, and both are knobs.
3. **Bytes.** Each row group's bytes are its pro-rata share, by rows, of the dataset's compressed bytes. This is assumed, not measured.
4. **Reads.** A run reads exactly the row groups holding its keys.

## Determinism (invariant 3)

A `Replay` is a function of five things:

- the manifest version
- the data the catalog digest names
- the workload hash
- the proposal
- the caller-supplied `created_at`

Runs are replayed in workload order, one at a time, through ordered maps.

"The same data" means the same files. The catalog digest names each file by path, size, mtime and content hash, so the same bytes at another path are other data (`tests/inputs.rs`).

`tests/determinism.rs` writes one corpus, then re-executes its own binary in two modes. The modes differ in:

- committed-segment splitting
- run recording order
- transient segment limits
- cache state

They also differ in store paths and hash seeds. Each child builds the index from scratch in its own store, scores a composite-key proposal over committed segments, writes the `Replay` through the ledger, and emits the record's canonical JSON. The parent requires three things:

- the outputs are byte-equal, inputs included
- they equal an in-process run with a third limit
- with the catalog digest masked, they hash to a checked-in golden value. The digest is masked because it names this run's temporary paths and mtimes.

The golden value was last refreshed for record version 4 (B8: the record's `record_version` and its proposal's hash move, nothing else); before that, for contract v2.1 (`RECORD_VERSION` 2 and `inputs`).

## Memory (ADR 018)

**Recording** holds one run's key set and one input batch.

**Replay** holds:

- one run's key set
- the planner's block indexes and the blocks a lookup touches
- the cached transient indexes, bounded by `TransientCache`'s byte cap (default 2 GiB, least recently used evicted). The cache is in-process: it lives as long as the `Replayer`, and nothing is written to the store's `cache/`, so a one-shot process rebuilds a candidate's index.

**A transient build** measures each row group's keys before adding them, the way the build crate accounts segments:

- **Exact:** distinct key tuples and their canonical bytes.
- **Temporal:** distinct `(key, ts)` pairs, plus 8 bytes each.
- **Bloom:** 8 bytes per distinct key.
- **Range:** twice the longest key.

While building, it holds:

- one row group's projected key columns
- one open segment writer, cut before the next row group would take it past `max_segment_rows` rows (default 2^20) or `max_segment_key_bytes` accounted key bytes (default 64 MiB). A single row group larger than a cap gets its own segment.
- the finished segments. Their bytes, plus the open writer's accounted key bytes, plus the next row group's, are checked against `max_index_bytes` (default 1 GiB) **before** each row group is added. Exceeding the cap is an error, never a partial index.

**The layout model** holds:

- a map of distinct cluster keys, each charged its bytes plus `CLUSTER_KEY_OVERHEAD` (64), capped at `max_cluster_key_bytes` (default 256 MiB). Past the cap it returns `ReplayError::ClusterKeysExceeded`; a partitioned sort is a later addition.
- one row group's keys per run key-column set
- its modeled segments, under the same three segment caps

**The statistics baseline** holds every embedded filter of every run column of every file.

`tests/memory.rs` tests these bounds. `tests/current_index.rs` proves two things:

- replaying the current index makes no `read_columns` call
- a candidate reads only its key columns

The build planner reads footers through its own source, which the test's counting source does not see.

### Memory caps

`max_segment_key_bytes` and `max_index_bytes` cap **accounted** key bytes (the measures above), not resident memory.

The per-key resident overhead (contract v3, entry 32) was measured in a release build by `tests/resident.rs` (`cargo test --release -p bellwether-replay --test resident -- --ignored`), over 1,048,576 keys, on 2026-10-26 ([the measured document](../../docs/measured/2026-10-26-performance.md)). These are the documented constants:

- **Open writer: 320 bytes resident per key.** An open transient segment writer over `UInt64` keys accounts 9 bytes per key and holds 320, which is 35.5 times the accounted bytes. A key cap therefore bounds resident memory at about 36 times itself for narrow keys.
- **Layout map: 84 bytes resident per key.** The cluster-key map over 9-byte keys accounts 73 bytes per key (`CLUSTER_KEY_OVERHEAD` included) and holds 84, which is 1.15 times the accounted bytes.

Earlier debug-build figures, for scale:

- **Index cap, u64 keys.** An 8 MiB `max_index_bytes` cap admitted about 900,000 keys, and resident memory peaked near 284 MB, 33 times the cap.
- **Default caps, u64 keys.** 2 million unique keys peaked near 397 MB for a 90 MB index.
- **Rows cap.** For narrow keys, `max_segment_rows` (2^20 rows, roughly 300 MB resident) is the bound that takes effect.

Charging the writer's measured overhead to the caps, shared with the build crate, is a WP22b candidate.

## Tests

| File | Covers |
|---|---|
| `tests/determinism.rs` | Invariant 3: two processes in different modes over one corpus, byte-identical record, golden hash with the digest masked. |
| `tests/inputs.rs` | `Replay.inputs`: the same data at two paths gives equal costs and two records; so does one folder before and after a file lands. |
| `tests/wp4_planner.rs` | A committed index built by the build crate's `Builder`. Every run plans to the same whole `ScanPlan` under each configuration (exact, temporal, composite prefix, range, bloom, range ∩ exact), three ways: WP4 committed, WP4 with this crate's transient segments, and `SegmentPlanner` with them. `for_snapshot` replays equal `SegmentPlanner::from_snapshot`'s. |
| `tests/sketch_planner.rs` | Two indexes over one corpus with the same key specs, one also declaring a `sketch`: `SegmentPlanner::from_snapshot` plans every run under each spec and all together to the same `ScanPlan` (some prune), and `Replayer::for_snapshot` replays current and hypothetical specs to the same costs. A sketch never reaches a replay plan. |
| `tests/null_row_groups.rs` | All-null row groups: covered by every spec kind, recorded in the segments' own bytes, never selected, split-invariant. Two adjacent key-less segments have different bytes. A run of only null tuples plans nothing. Catalogs that differ by one all-null row group are kept apart. |
| `tests/oracle.rs` | Invariant 1 through replay, on in-test corpora (also a property test over random layouts). The oracle reads the files directly. Exact paths must equal it: exact, temporal, composite prefix, range ∩ exact, and bloom ∩ prefix. Inexact paths must contain it. The per-key intersection of two single-column specs must equal the union over tuples of `oracle(a) ∩ oracle(b)`, and on some run be strictly smaller than the per-run intersection. Every cost must equal the footer sum of its cells, read with the `parquet` crate. `ParquetStats` must equal an independent implementation. |
| `tests/typed_oracle.rs` | The same, two-sided, on the review's typed corpus: nulls, both zeros, NaN, decimals at two file scales, `u64 ≥ 2^63` and `u32 ≥ 2^31` under embedded filters, strings with NUL. |
| `tests/baseline_types.rs` | `ParquetStats` on filters of all six physical types (`FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, `INT32`, `INT64`): it contains the oracle and lies inside an independent bloom-only plan. `Decimal256` filters are refused. |
| `tests/current_index.rs` | Committed and transient segments cost the same. An incomplete spec or a coverage gap refuses only the plans that list it, in both planners. A rewritten file is refused. No column reads for the current index. Proposals through the ledger, with inputs and model. Typed refusals that write nothing, including doctored `Scored` values. A layout replay worked by hand and written with its model. Template reports. Attribution and interaction. |
| `tests/seam.rs` | The transient cache key (`fpr`, `ts_column`, catalog digest, key types), the seam's type checks, and bloom `fpr` values surviving the segment header's JSON round trip. |
| `tests/workload.rs` | Identity, cache and `workload.json` round trips, refusal of doctored files, dataset-type encoding, query kind, `run_keys` hash checks, corrupt-input sweeps, and the bench format with each tampering it detects, including a last-run mismatch that writes nothing. |
| `tests/memory.rs` | Segment caps by rows and key bytes, the index cap including the open writer, the cluster-key cap, and cache bounds and keys. |
| `tests/bench_corpus.rs` | Needs `BELLWETHER_BENCH_CORPUS`. Baselines equal `results.json`'s `raw_scan` and `parquet_stats` costs run by run. The exact key and range ∩ exact equal the oracle. Every candidate contains the oracle. Transient segments reproduce `expected.json`'s fanouts. |
| `tests/latency.rs` | The p50-run budget, asserted in optimized builds. CI's "latency budgets" job generates the bench small corpus and runs it with `cargo test --release` and `BELLWETHER_REPLAY_LATENCY_CORPUS`. The same run against a committed index through the build planner is held to the same budget. |

To run the bench-gated tests, generate a corpus with `bellwether-bench`:

```sh
bench generate --out <dir> --days 4 --files 64 --row-groups-per-file 2 --rows-per-row-group 2000 --entities 640000 --churn 0.1
bench workload --corpus <dir> --runs 48 --p50 40 --p95 20000 --max 60000
bench ground-truth --corpus <dir>
bench run --corpus <dir> --out <dir>/results.json --no-duckdb
BELLWETHER_BENCH_CORPUS=<dir> cargo test -p bellwether-replay --test bench_corpus
```

## The two planners

`Replayer::for_snapshot` plans through the build crate's `Planner`, the one the index answers queries with:

- committed specs through `Planner::plan`
- transient specs through `Planner::plan_with_transient`, from `TransientIndex::segment_bytes()`

It refuses a source whose schema differs from the index's, because keys must encode at the index's types.

`SegmentPlanner` serves `Replayer::unindexed`, and any caller of `Replayer::new`. Its `from_snapshot` gives the same plans over a committed index, which `tests/wp4_planner.rs` checks.

## Contract gaps

The contract v3 backlog in `docs/contract-v3.md` tracks this:

- **Rewrite proposals.** This crate defines no replay for them (backlog #2).

Contract v2.1 closed two gaps: the layout model, through `Replay.model`, and unindexed data identity, through `Replay.inputs`.

## Semi-join runs (B3)

A run recorded with `RunQuery::SemiJoin(RunJoin { right, right_columns })` (`query_kind` `semijoin`, `RecordedRun.join`) semi-joins its keys, values of the run's columns in the data root (the left dataset), with the right dataset, the folder `right` beside the root. A replayer prices it only with the right side attached (`Replayer::with_pair(PairInput)`: the right dataset's own replayer and, per join key priced, its transient pair index from `bellwether_build::transient_pair`). Under a configuration holding the join key on the run's pair, each step reads the row groups holding a key both datasets hold, as `Planner::plan_semijoin` plans (`semijoin_plans`; `tests/join.rs` checks predicted equals planned side by side, with and without counts); under any other, each step is a locate of the keys by that side's own keys, and a side no key serves is priced by its files' statistics. A run's cost is the sum of its steps (`pair_cost`), and a Replay of a semi-join workload cites both catalogs. A missing right side, another right dataset, or a join key with no pair index is refused.
