# Benchmark methodology

This page fixes the benchmark design before any implementation or result
exists, as ADR 019 requires. A change to anything below after the first
harness run is recorded here with a date and a reason, and results from
before the change are labeled as such.

The harness is WP10, `bellwether-bench`
([`work-packages.md`](work-packages.md)). Every dataset is synthetic. No
real data is used, and none is needed to reproduce a result.

## Questions the benchmark answers

1. How many bytes does a keyed lookup read with the index, compared with
   each baseline?
2. How does lookup latency grow with corpus size?
3. How far is the engine's predicted cost from the measured cost? The
   thesis (ADR 005) is that replay is exact, so the expected error is zero
   and any nonzero value is a bug.
4. Given a dataset with a planted anti-pattern, how often does the engine
   recommend the fix an expert would make?

## Dataset generator

A synthetic versioned ledger. Each entity has a true id and a series of
versions over time. The layout imitates a common lake shape on purpose:
files are partitioned by day and written in arrival order, so a version's
location depends on when it arrived, not on its entity.

| Parameter | Meaning | Sweep |
|---|---|---|
| `files` | number of Parquet files | `<files sweep>` |
| `row_groups_per_file` | row groups in each file | `<rg sweep>` |
| `rows_per_row_group` | rows in each row group | `<rows sweep>` |
| `entities` | distinct true ids (cardinality) | `<cardinality sweep>` |
| `churn` | mean versions per entity over the time span | `<churn sweep>` |
| `days` | partition span, one directory per day | `<days>` |
| `batch_stamp` | include a batch-stamp column that looks like a key | on |
| `null_ts_fraction` | fraction of versions with a null timestamp | `<fraction>` |
| `seed` | RNG seed; the dataset is a pure function of parameters and seed | fixed per run |

Columns: a true id, a version timestamp, a batch-stamp column (a high
cardinality value assigned per write batch, ordered with time), a partition
date, decimal amount columns for totals checks, and filler columns to give
row groups realistic width.

### Planted anti-patterns

Each generated dataset may plant one or more patterns from
[`rule-catalog.md`](rule-catalog.md), and records which ones it planted as
ground truth next to the data.

- `batch_stamp_key`: consumer workloads join on the batch-stamp column.
- `low_selectivity_single_column`: a declared single-column key with a
  composite that prunes much better.
- `declared_but_unused_key`: a declared key spec no workload uses.
- `scattered_time_column`: timestamps written out of order across files.

The expert fix for each planted pattern is written down before the
engine runs, as part of the ground truth.

## Workload generator

A recorded workload is a list of runs; each run is a key set. Key-set sizes
are drawn to hit three points.

| Parameter | Meaning | Value |
|---|---|---|
| `runs` | recorded runs per workload | `<runs>` |
| `keys_p50` | key-set size at p50 | `<keys p50>` |
| `keys_p95` | key-set size at p95 | `<keys p95>` |
| `keys_max` | key-set size at max | `<keys max>` |
| `key_skew` | how keys are drawn from entities (uniform or skewed) | `<skew>` |
| `recency_bias` | how strongly runs prefer recently changed entities | `<bias>` |
| `query_kinds` | mix of `locate`, `latest`, and `asof` | `<mix>` |
| `seed` | RNG seed | fixed per run |

## Baselines

From ADR 019.

- **Raw scan**: read every row group and filter.
- **DuckDB**: the same lookup as a query over the same files.
- **Parquet-native statistics**: prune with footer min and max and any
  embedded bloom filters, no sidecar index.
- **Apache DataFusion**: a popular Rust engine that prunes with Parquet statistics, the page index, and embedded bloom filters.

Each baseline answers the same lookups on the same files and must return
the same rows as the engine. A baseline that returns different rows is a
failed run, not a data point.

## Metrics

From ADR 019.

| Metric | Definition | Unit |
|---|---|---|
| Bytes read | compressed bytes fetched from storage per lookup | bytes |
| Latency versus corpus size | wall time per lookup across the `files` sweep, at p50, p95, max | ms |
| Cost-estimation error | `abs(predicted - measured) / measured`, where predicted is `Replay` bytes and measured is bytes actually read | ratio |
| Expert-fix recovery rate | fraction of planted anti-patterns for which the top recommendation equals the recorded expert fix | ratio |

Also recorded, for the SLO assertions in ADR 018: peak resident memory per
operation, and observe-tool latency against its budget.

## Environment

Every result records: commit hash, `rustc` version, OS, CPU model, core
count, memory, storage kind (local disk or object store and region), and
whether caches were warm or cold. Cold runs drop the OS page cache before
each lookup where the platform allows; otherwise the result is labeled
warm.

## How to reproduce

Intended commands, to be confirmed when WP9 and WP10 land.

```text
bellwether bench generate ./bench-data/ --config bench/<config>.toml
bellwether bench run ./bench-data/ --baselines all --out results.json
```

The configuration file holds every parameter above, including seeds. The
output is JSON: one object per (baseline, lookup) with the metrics above
and the environment block. Charts are rendered from that JSON, never edited
by hand.

## Results

Results are published here after the first harness run; nothing in this
document is a measurement until then.

## Amendments

### 2026-10-24: measured bytes

"Bytes read" in every result is **measured**: the harness wraps the file reader in a byte-counting layer and records the compressed bytes actually fetched for each run, alongside the **planned** bytes computed from row-group statistics. The realized-versus-predicted figure plots these two numbers against each other; the claim "no estimation error" means their difference is zero for every strategy whose plan is executed as written, and any nonzero difference is reported, not hidden.

### 2026-10-24: pre-registered sweep values (revised 2026-10-24, before any harness run)

The first pre-registration (10 / 100 / 1,000 files with 30 days each) was inconsistent with the generator, which writes at least one file per day and re-versions entities per day. The ladder is now defined by days and files per day. Churn is the fraction of entities re-versioned per day. Rows per day are fixed by the layout (files per day × row groups per file × rows per row group), so each re-versioned entity receives about 16 (small), 64 (medium), and 51 (large) versions per day; the generator records the measured versions per entity in its ground truth (corrected 2026-10-24: the first wording claimed about one version per re-versioned entity per day, which the row model does not give).

| Rung | Days | Files per day | Files | Row groups per file | Rows per row group | Entities | Churn per day |
|---|---|---|---|---|---|---|---|
| small | 10 | 1 | 10 | 8 | 100,000 | 1,000,000 | 0.05 |
| medium | 30 | 4 | 120 | 8 | 100,000 | 1,000,000 | 0.05 |
| large | 30 | 32 | 960 | 8 | 100,000 | 10,000,000 | 0.05 |

Payload width 256 bytes. Workload: 200 recorded runs with key-set sizes at p50 / p95 / max of 50 / 10,000 / 100,000, split 150 search / 50 held-out by run id. Seeds: 7 for the corpus, 11 for the workload. Any further deviation is recorded here with a date and a reason.

### 2026-10-24: latency accounting for engine baselines

An engine baseline's planning phase may itself scan key columns (Apache DataFusion's pruning does). Reported latency for such a baseline is split into planning time and execution time, and the realized-versus-predicted figure uses the harness's own byte-counting reader for every strategy, so an engine's internal reads are counted the same way as the index's.

### 2026-10-24: advisory ignore for an engine baseline dependency

`deny.toml` ignores RUSTSEC-2024-0384 (`instant`, affecting wasm32 only), pulled in by DataFusion 43 behind the optional `datafusion` feature. It is removed by the Phase 3 dependency upgrade together with the other four ignores.

### 2026-10-24: gate ablation operationalization

The gate ablation (search with versus without the equivalence gate) is measured by running the ungated arm to completion and then reconciling every candidate on its final frontier after the fact; the fraction of frontier candidates that would have failed the gate, and the cost advantage they claimed, are the reported quantities. Adopted from the paper draft so that it is fixed before the first harness run.

### 2026-10-24: ablation details adopted from the paper draft

Two choices made while drafting the paper are adopted as methodology: (1) the Pareto-versus-mean ablation uses the same proposer budget and seed for both arms; (2) the attribution ablation replaces exact per-node differences with the proposer's self-reported attribution, holding everything else fixed.

### 2026-10-24: WP10 generator parameters and row model

Reason: the harness had to choose a concrete row model, and some table entries could not be implemented as written.

- **`churn`** is the fraction of entities re-versioned per day, the reading the pre-registered sweep already uses ("churn 0.05 per day"). It is not mean versions per entity over the span. `expected.json` reports the measured mean (`versions.mean_versions_per_entity`), so both readings are available.
- **Rows per day** come from the layout: files that day × row groups per file × rows per row group. A day re-versions `min(rows per day, round(churn × entities))` entities. If rows per day exceed `churn × entities`, a re-versioned entity gets several versions that day. The ground truth reports the versions it measures; it does not assume one per day.
- **No `batch_stamp` switch.** `batch_id`, a 20-minute write-window stamp written as `YYYYMMDDHHMM`, is always present. Whether the pattern is planted is measured from the data (see the planted-pattern amendment). It depends on files per day.
- **`null_ts_fraction` is deferred.** `version_ts` is never null. Null ranking (ADR 014) is exercised by WP1's temporal chains; the generator adds nulls once a temporal index exists to test against.
- **Late rows.** A parameter not in the table, `late_fraction`, back-dates that fraction of rows after day 0 by one to three whole days. This is what plants `scattered_time_column`.
- **Partition date.** It is the directory (`day=YYYY-MM-DD`, Hive style), not a column inside the files. DuckDB reads it through `hive_partitioning`.
- **Columns.** One decimal amount column, `Decimal128(18, 2)`; one five-value `status` column; one wide `payload` filler column.
- **Bloom filters.** Every row group carries a Parquet-native split-block bloom filter on `entity_id`, sized for one row group's distinct values at a 1 % false-positive rate. The statistics baseline and DataFusion both prune with it.
- **Files per day.** The generator requires `files >= days`, so every day directory holds at least one file.
- **Determinism.** The RNG is ChaCha12, keyed from explicit bytes per row group. Golden hashes pin a tiny corpus's `corpus.json`, its decoded values, its Parquet bytes, and its workload.

### 2026-10-24: the revised ladder in the harness

Reason: this records how the harness runs the revised ladder (above).

- **Presets.** The rungs are presets: `generate --rung small|medium|large` (corpus seed 7) and `workload --preregistered` (200 runs, 50 / 10,000 / 100,000, workload seed 11). `late_fraction` keeps the WP10 default of 0.02. `generate --rung <r> --dry-run` prints the layout without writing.

  | Rung | Rows | Arrow size in memory (estimate) | Measured on disk |
  |---|---|---|---|
  | small | 8 million | about 2.5 GB | 2.1 GB |
  | medium | 96 million | about 30 GB | not run |
  | large | 768 million | about 240 GB | not run |

- **`batch_stamp_key` on small.** The small rung writes one file per day, so a batch value touches one file and the pattern is *measured* as not planted there. It is planted on medium and large.
- **Cost of a full run.** The harness computes every run's oracle in one pass over the corpus. Raw scan still reads the whole corpus on every run: about 30 GB per run on medium and 240 GB on large, or 200 × that for the pre-registered workload. On large, record plans with `run --no-scan`, or time raw scan on a subset of runs. The generator holds one 100,000-row group (an RSS bound of about 204 MB). Ground truth holds `O(entities)` plus one day's `(entity_id, batch_id)` map, about 25.6 million entries on large.
- **The 150 / 50 search and held-out split** is by run id. `workload.json` exposes the ids (`run-NNNN`); WP13 applies the split, not WP10.

### 2026-10-24: planted anti-patterns, rule mapping, and measured planted flags

Reason: `rule-catalog.md` defines the rule ids, and a dataset must not claim a pattern its layout does not exhibit.

- **Mapping.**
  - **`batch_stamp_key`** is planted on `batch_id`. The composite `(entity_id, batch_id)` is recorded as that rule's fix, a `RewriteShape` from `join on batch_id` to `join on (entity_id, batch_id)`; it is not a separate rule. Its exact fanout is in `fix_fanout`.
  - **`low_selectivity_single_column`** is planted on `status`: five values, each touching most row groups. The fix is the key `[status, entity_id]`. It is deferred; see "two planted patterns deferred".
  - **`scattered_time_column`** is planted on `version_ts` by the late rows.
  - **The versioned history** of `entity_id` maps to **`lookback_unbounded`**. The fix is a temporal index on `entity_id` by `version_ts`, then the `LatestPerKey` residual. It is deferred; see "two planted patterns deferred".
  - **`declared_but_unused_key`** is deferred. It lives in a manifest, not in the data, and is planted once a manifest fixture exists (WP3/WP4).
- **Fixes are written down first.** Each fix is recorded in the catalog's guidance-template shape (`RewriteShape`), plus the implied `KeySpec` or clustering, in `expected.json` before any engine runs.
- **`planted` is measured from the data.** The thresholds:

  | Rule | Planted when |
  |---|---|
  | `batch_stamp_key` | p50 files per value ≥ 2, and p50 row groups per value ≥ 2 × the fix's p50 (a time-concordance condition was added the same day: see "shared time measures") |
  | `low_selectivity_single_column` | the median value touches ≥ 50 % of all row groups, and that median ≥ 2 × the fix composite's p95 |
  | `scattered_time_column` | gap ratio ≥ 1.5 (superseded the same day by overlap depth: see "shared time measures") |
  | `lookback_unbounded` | the p95 entity's history spans ≥ 2 row groups |

  One file per day with no late rows plants neither `batch_stamp_key` nor `scattered_time_column`, and a test checks this.
- **Time scatter uses window intersection** (superseded the same day; see "shared time measures"). For each 20-minute window:
  - *covering* is the number of row groups whose footer `[min, max]` intersects the window;
  - *holding* is the number of row groups that hold a row in it;
  - the gap ratio is mean covering / mean holding. It is at least 1 by construction.

### 2026-10-24: two planted patterns deferred

Reason: a planted pattern counts toward the expert-fix recovery rate only when the benchmark supplies what its fix needs. Two do not yet. Both keep their measured `planted` value in `expected.json`, and both name the missing input in `deferred`, as `declared_but_unused_key` does.

- **`low_selectivity_single_column`** (on `status`) is deferred. The fix key `[status, entity_id]` is unusable: every recorded run supplies only `entity_id`, and no key spec on `status` is declared. It needs a recorded workload whose runs supply `status`, and a declared key spec on `status`.
- **`lookback_unbounded`** (on `entity_id`) is deferred. It needs a consumer query plan with a latest-per-key step, which no record carries; the recorded workload is locate-only.
- **Recovery rate.** Until those inputs exist, it is scored over `batch_stamp_key` and `scattered_time_column`, where they are planted.

### 2026-10-24: shared time measures for two planted flags

Reason: the review of the profiler (PR 17) found its lints and these planted flags disagreeing on four corpora. The window gap ratio also counted empty windows inside one row group's own range, so sorted but unevenly dense data read as scattered. A planted flag and the lint it scores must be one computation.

- **One definition.** `bellwether_source::stats` defines overlap depth and cross-day time concordance, with their thresholds. `truth.rs` calls those functions over the corpus's footer statistics, read through `bellwether_source`. The profiler calls the same functions. (For the batch stamp this held only for the concordance; the second review found the profiler's other gates still differed, and the whole decision moved into `bellwether_source::stats::batch_stamp` the same day: see "one batch-stamp decision".)
- **`scattered_time_column`** is planted when the overlap depth of `version_ts` is at least 1.5 and its sortedness is below 0.9.
  - *Overlap depth* is the sum of the closed row-group ranges' lengths divided by the length of their union. Sentinel ranges are excluded (revised the same day: see "one batch-stamp decision").
  - *Sortedness* is the fraction of consecutive within-file row groups whose ranges ascend without overlapping.
  - The 20-minute window, covering, and holding are gone.
- **`batch_stamp_key`** keeps its two fanout conditions and also needs `batch_id`'s cross-day concordance with `version_ts` to be at least 0.9 over at least two days.
  - Each row group is placed on the UTC day of its `version_ts` maximum.
  - Over pairs of row groups on different days, concordance is the fraction whose `batch_id` minima are strictly in day order.
  - A one-day corpus therefore does not plant it.
- **`expected.json`** is now `bellwether-bench-expected/3`. Its `time_scatter` detail carries depth, sortedness, covered and union lengths, pair counts, excluded ranges, and the generator's late-row count.
- **Effect on the layouts:**
  - One file per day without late rows plants neither pattern (depth 1).
  - Several files per day without late rows plant the batch stamp but not the scatter: every file still ascends.
  - Late rows plant the scatter once they leave fewer than 9 in 10 consecutive within-file row groups ascending; a handful may not.
  - A single day never plants the batch stamp.

### 2026-10-24: one batch-stamp decision, and sentinels that reach outside the data

Reason: the second review of the profiler (PR 17) found its batch-stamp lint still disagreeing with this planted flag on three corpora (seeds 21, 22, 25). The profiler had two gates the benchmark lacks: range narrowness, and a partner with more distinct values than `batch_id`. A fourth corpus (seed 31) showed late rows excluded as sentinels, hiding real scatter.

- **One decision.** `bellwether_source::stats::batch_stamp` holds the files gate (p50 files per value ≥ 2), the ratio (p50 row groups ≥ 2 × the composite's), and the cross-day concordance (≥ 0.9). Its constants are defined there and nowhere else. `truth.rs` calls it against `(entity_id, batch_id)`, and the profiler calls it too. The narrowness and partner-distinct gates are gone.
- **Agreement.** The profiler fires on the first partner, in its preference order, for which the decision holds (revised the same day: see "one partner rule", under which agreement with a supplied `entity_id` is by construction).
- **Sentinels.** A range is excluded from the overlap depth only if all of these hold:
  - it is longer than 8 × the median;
  - it reaches outside the span of the ranges that are not that long;
  - at most 4 ranges qualify. When more qualify, none is excluded.

  Late rows stay inside the data's span, so they always count. Excluded ranges still count in sortedness.
- **Pinned.** `tests/bench.rs` pins the flags of the four corpora, with the generator parameters of the review's script.
- **Format.** `expected.json` keeps format `/3`. Its fields are unchanged; two definitions changed.

### 2026-10-24: one partner rule

Reason: the third review (PR 17) found four corpora (seeds 71 to 74) where every row is a distinct entity. The profiler refused `entity_id` as a row id, while this flag planted the batch stamp with `(entity_id, batch_id)`.

- **One rule.** `bellwether_source::stats::partner_order` chooses the composite partner for both composite rules, on both sides, from the same inputs:
  - a row id (an exact distinct count reaching the row count) is refused unless the recorded workload supplies it;
  - when the workload supplies any candidate, only supplied candidates count;
  - a repeating candidate ranks before a near-unique one, then the smaller composite p95.
- **The benchmark's side.** `truth.rs` applies the rule to its fix partner, `entity_id`, which its workload supplies (`WORKLOAD_KEY_COLUMNS`). A fully unique `entity_id` therefore stays the partner.
- **Agreement.** The profiler, run with the corpus's workload, considers only `entity_id` too, so the two agree by construction. The profiler's ground-truth test now requires a workload.
- **Pinned.** `tests/bench.rs` pins the four corpora's flags.

### 2026-10-24: workload generator

Reason: the first harness implements the lookup workload only.

- **`key_skew`** is uniform.
- **`recency_bias`** is not tunable. A run draws the entities re-versioned on consecutive days, starting from a random day and offset, which models an incremental job.
- **`query_kinds`** is `locate` only. `latest` and `asof` wait for the temporal index.
- **Run sizes** hit the nearest-rank p50, p95, and max exactly. Every quantile in the harness is nearest-rank: the value at zero-based index `ceil(q × n) - 1`.

### 2026-10-24: baselines

Reason: these are what could be run honestly in the first harness.

- **Parquet-native statistics and bloom filters** prune by footer min and max of `entity_id`, then by the row group's embedded bloom filter.
  - The filters are read once when the strategy is built and cached, like footers.
  - Their bytes are not part of any plan's bytes, because plans count row-group data only.
- **DuckDB runs through the external `duckdb` CLI** and is not linked into the harness.
  - Why: the bundled build added about 20 GB of build artifacts, and minutes to every CI run that uses `--all-features`.
  - How it runs: `duckdb -no-agent -init /dev/null -csv -noheader`, with `AI_AGENT` removed from its environment.
    - DuckDB 2 otherwise switches to an "agent" output mode when `AI_AGENT` is set and stdout is not a terminal.
    - `-init /dev/null` keeps a user's `~/.duckdbrc` out.
    - The parser accepts only one CSV row with the expected fields and one `.timer` line, with nothing on stderr. It rejects anything else.
    - Latency is DuckDB's own `.timer` query time, and the CLI version is recorded in the `environment` block.
  - How to enable it: it sits behind the `duckdb` feature and is recorded as unavailable otherwise. `--require-duckdb` makes it mandatory.
  - What it reports: files, row groups, and bytes are not observable, so they are recorded as `null`.
- **Apache DataFusion** is a plan strategy behind the `datafusion` feature: DataFusion 43, pure Rust, no native dependencies. It runs DataFusion's own `ParquetExec` with the lookup predicate, with statistics, page-index, and bloom-filter pruning on (DataFusion's defaults).
  - **The plan** is the set of row groups whose column chunks DataFusion actually read, recorded at its reader.
  - **Granularity:** page-level savings inside a row group are not represented, because plans are row-group granular.
  - **Latency:** the plan time includes DataFusion's projected scan.
  - **Large key sets:** DataFusion rewrites an `IN` list into prunable form only up to its own size limit, so large key sets prune by bloom filters alone, exactly as DataFusion would.
- **"Same rows" is checked three ways:**
  - matched row count;
  - the exact `amount` total as a decimal;
  - for plan strategies, an order-independent 128-bit digest over every column of every matched row, compared with raw scan's.

  DuckDB is compared by count and exact amount total. This is stronger than counting rows, and weaker than full row equality for the engine.
- **The exact oracle** finds the row groups that actually hold a queried key by scanning `entity_id`. It is a reference used to check every plan for false negatives (invariant 1), not a strategy.

### 2026-10-24: metrics as implemented

Reason: these are the operational definitions behind the metric table.

- **Bytes read** is measured inside the `ChunkReader` that parquet-rs drives. Storage is read at column-chunk granularity: the first request into a chunk fetches the whole chunk, as object-store readers do.
  - `results.json` records `bytes_read` (fetched), `bytes_consumed` (handed to the page decoder), the planned bytes, and their difference.
  - The SLO requires fetched and consumed to equal planned for every executed plan.
  - Plans are not trusted: files, row groups, and bytes are recomputed from the footers, and a plan's own counts are kept beside them.
- **Cost-estimation error.** For the baselines it is planned (from statistics) against measured. The `Replay`-against-measured version waits for WP6.
- **Expert-fix recovery rate.** The ground truth records the fixes; scoring waits for WP5 and the advisor.
- **Peak resident memory per operation** is measured for the generator against a stated bound of 6 × the row-group estimate + 16 MiB.
  - On Linux, `clear_refs` plus `VmHWM` gives the exact peak.
  - Elsewhere, RSS is sampled with `ps`.
  - The smoke corpus holds at least 3× the bound, and a deliberately retaining generator must fail the check.
  - Index lookup and build budgets are named and stay unset until the index lands; setting one without matching results fails.
- **Latency.**
  - For plan strategies, planning (`plan_ms`) and execution (`scan_ms`) are reported separately. Execution reads all columns of the planned row groups through the byte-counting reader and filters to the keys. `latency_ms` is their sum.
  - DataFusion's planning includes its pruning scan of key columns. The bytes it fetched while planning are reported as `plan_bytes_read`, beside the execution bytes; the baselines read nothing per lookup while planning (`0`).
  - For DuckDB: query time only. Planning cannot be separated through the CLI.

### 2026-10-24: environment, commands, and output

Reason: this records what the first harness emits.

- **The `environment` block** records:
  - commit hash, with a dirty flag;
  - `rustc` version;
  - OS, version, and kernel; architecture;
  - CPU model, core count, and memory;
  - storage (`local disk`) and cache (`warm`).

  Every run is warm, because the harness never drops the page cache.
- **Commands** are `cargo run -p bellwether-bench --release -- <generate|workload|ground-truth|run|report>` with flags. There is no TOML configuration yet; WP9's `bellwether bench` will wrap these commands.
- **`results.json` is nested:** corpus, then runs, then one result per strategy. Each per-run strategy result is the "(baseline, lookup)" object; the schema is in the crate README.
- **Earlier numbers.** Numbers quoted in the WP10 pull request before this date are pre-amendment. They come from a layout with 4 files per day and from a harness without measured bytes or row digests.
