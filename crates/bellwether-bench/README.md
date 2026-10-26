# bellwether-bench

Work package WP10. This crate is the benchmark ADR 019 fixed before any index existed. `docs/benchmark.md` has the methodology and the dated amendments that record how this implementation differs from it. The crate contains:

- a synthetic versioned ledger with planted anti-patterns,
- recorded incremental workloads,
- exact ground truth for those anti-patterns,
- a harness over the baselines,
- SLO assertions that cannot pass vacuously,
- an HTML report.

```
cargo run -p bellwether-bench --release -- <subcommand>
```

| Subcommand | Writes |
|---|---|
| `generate` | `<corpus>/data/day=YYYY-MM-DD/part-NNNNN.parquet`, `<corpus>/corpus.json`, `<corpus>/generator-stats.json` |
| `workload` | `<corpus>/workload/run-NNNN.parquet`, `<corpus>/workload.json` |
| `ground-truth` | `<corpus>/expected.json` |
| `text --shape ledger\|market` | `<out>/data/day=YYYY-MM-DD/part-NNNN.parquet` (1–4 KB `body` documents with terms planted at 1% and 0.01% of rows) and `<out>/workload/text-truth.json` (per query, the rows and row groups holding a match); `scripts/text-funnels.py` |
| `bellwether bench expr --shape ledger\|market --out DIR` | `DIR/data/part-NNNN.parquet` with a JSON attribute column (`attrs` holding an `account` join key, or a vendor `payload` holding `symbol`), `DIR/workload/keys-{rare,medium,wide}.parquet` (and `keys-day.parquet` for `date_trunc(ts,day)` on market), each with one column named by the expression, and `DIR/workload/truth.json`: per query, the row groups and rows holding a key. Rare values live in one row group, medium values in one span of ten, wide values in every row group (the case no index improves). `scripts/expr-funnels.py` measures the `expr` key funnel on both shapes. |
| `run` | `benches/out/results.json` (or `--out`) |
| `report` | `benches/out/report.html` (or `--out`) |
| `--assert-slo` | Exit status. With no subcommand (what CI runs), the tool generates a smoke corpus in `benches/out/slo/`, runs the harness, and asserts in **strict** mode: a skipped core check fails. With `--results PATH` it asserts on that file, leniently. `run --assert-slo` asserts on the results that run just wrote. On any other subcommand it is a usage error. |

`benches/out/` is git-ignored. Paths are relative to the working directory, so run from the repo root.

## Reproduce

The commands below work in both bash and zsh. Run them from the repo root.

```sh
bench() { cargo run -q -p bellwether-bench --release -- "$@"; }
```

### Pre-registered ladder (small, medium, large)

`docs/benchmark.md` (revised 2026-10-24) fixes three rungs. Each has 8 row groups of 100,000 rows per file, churn 0.05 per day, a 256-byte payload and corpus seed 7. The pre-registered workload is 200 runs at p50 / p95 / max of 50 / 10,000 / 100,000 keys, with seed 11. The presets set every parameter exactly:

| Rung | `--rung` sets | Rows | Arrow size in memory (estimate) | Measured on disk |
|---|---|---|---|---|
| small | `--days 10 --files 10 --entities 1000000` | 8 M | about 2.5 GB | 2.1 GB |
| medium | `--days 30 --files 120 --entities 1000000` | 96 M | about 30 GB | not run |
| large | `--days 30 --files 960 --entities 10000000` | 768 M | about 240 GB | not run |

```sh
bench() { cargo run -q -p bellwether-bench --release --features datafusion -- "$@"; }
for rung in small medium large; do
  bench generate --rung $rung                 # writes benches/out/$rung
  bench workload --corpus benches/out/$rung --preregistered
  bench ground-truth --corpus benches/out/$rung
done
bench run --corpus benches/out/small --corpus benches/out/medium --corpus benches/out/large \
  --out benches/out/results.json --assert-slo
bench report --results benches/out/results.json --out benches/out/report.html
```

- **Dry run:** `bench generate --rung large --dry-run` prints the layout (rows, row groups, size, versions per entity per day) without writing anything.
- **Run cost:** raw scan reads the whole corpus on every run, about 30 GB per run on medium and 240 GB on large. For plans only, pass `--no-scan` to `run`. With `--no-scan`, the row and byte checks report SKIP, and a latency budget fails.
- **Versions per day:** the rungs give each re-versioned entity 16 (small), 64 (medium) or 51 (large) versions per day. `docs/benchmark.md` records this.
- **DataFusion:** drop `--features datafusion` to skip the DataFusion baseline.

### Quick check (38 MB, seconds; not a rung)

```sh
bench() { cargo run -q -p bellwether-bench --release -- "$@"; }
bench generate --out benches/out/quick --days 4 --files 64 --row-groups-per-file 2 \
  --rows-per-row-group 2000 --entities 640000 --churn 0.1
bench workload --corpus benches/out/quick --runs 48 --p50 40 --p95 20000 --max 60000
bench ground-truth --corpus benches/out/quick
bench run --corpus benches/out/quick --out benches/out/results.json
bench report --results benches/out/results.json --out benches/out/report.html
bench --assert-slo --results benches/out/results.json
```

### DataFusion baseline

DataFusion 55 (the release line built on arrow 59, the workspace's arrow) is pure Rust with no native dependencies. It is off by default because it adds a large compile:

```sh
bench() { cargo run -q -p bellwether-bench --release --features datafusion -- "$@"; }
bench run --corpus benches/out/quick
```

### DuckDB baseline

The DuckDB baseline runs an external `duckdb` CLI, so it compiles nothing extra:

```sh
bench() { cargo run -q -p bellwether-bench --release --features duckdb -- "$@"; }
BELLWETHER_DUCKDB=/path/to/duckdb bench run --corpus benches/out/quick --require-duckdb
```

- **Finding the CLI:** it uses `$BELLWETHER_DUCKDB`, or `duckdb` on `PATH`.
- **How it runs:** `duckdb -no-agent -init /dev/null -csv -noheader`, with `AI_AGENT` removed from its environment. DuckDB 2 otherwise switches to an "agent" output mode when `AI_AGENT` is set, and `~/.duckdbrc` stays out.
- **Strict parsing:** the parser accepts only one CSV row with the expected fields and one `.timer` line, with nothing on stderr. It rejects anything else.
- **Version:** recorded as `environment.duckdb_cli`.
- **Default mode:** without `--require-duckdb`, a missing CLI or a build without the feature is recorded as `"available": false` with the reason.
- **`--require-duckdb`:** turns either case into an error.
- **`--no-duckdb`:** skips the baseline.

## The synthetic ledger

Schema, written in this column order:

| Column | Type | Role |
|---|---|---|
| `entity_id` | UInt64 | The true high-cardinality id. **Planted:** its versions spread over row groups (`lookback_unbounded`). |
| `batch_id` | Int64 `YYYYMMDDHHMM` | A 20-minute write-window stamp. **Planted:** it looks like an id, but it is a timestamp (`batch_stamp_key`). |
| `version_ts` | Timestamp(µs, UTC) | **Planted:** `--late-fraction` of rows are back-dated by one to three days (`scattered_time_column`). |
| `status` | Utf8 | Five values, each in nearly every row group. **Planted:** `low_selectivity_single_column`. |
| `amount` | Decimal128(18, 2) | Summed exactly to compare results. |

Every row group carries a Parquet-native bloom filter on `entity_id`: split-block, sized for one row group's distinct values, 1 % false positives.
| `payload` | Utf8 | `--payload-bytes` wide, to make rows heavy. |

**Row model.**
- Day `d` writes `files_d * row_groups_per_file * rows_per_row_group` rows.
- It re-versions `min(rows, round(churn * entities))` entities, chosen by a keyed Feistel permutation of `0..entities` that needs O(1) memory.
- Rows of a day go to the day's files round-robin, as if from parallel writers. So row group `r` of every file covers the same slice of the day, and every `batch_id` spans every file of its day.

**Determinism.** Everything is a pure function of the parameters. The RNG is ChaCha12, keyed per row group from explicit seed bytes (`stats::chacha`). `tests/bench.rs` pins golden hashes of a tiny corpus:
- its `corpus.json`;
- every decoded value;
- its Parquet bytes;
- its workload hash and first run's keys.

**Streaming (ADR 018).** The generator builds one row group, writes it, flushes it, and drops it. Two bounds are stated from the parameters before the run:
- **Process RSS growth** over the whole generation must stay within `rss_growth_bound_bytes = 6 * estimated_row_group_bytes + 16 MiB`.
  - On Linux the probe writes `5` to `/proc/self/clear_refs` and reads `VmHWM`, which gives the exact peak.
  - Elsewhere it samples `ps -o rss=`.
  - This is the guard that sees retention. The smoke corpus is sized to hold at least 3× the bound, and `tests/streaming_guard.rs` runs a deliberately retaining generator that must fail the check.
- **Buffered bytes** (the batch plus the writer's buffers) must stay within `memory_bound_bytes = 3 * estimated_row_group_bytes + 8 MiB`.

## Workloads

Each run models one incremental job: the entities re-versioned over consecutive days, starting at a random day and offset. Run sizes are built so that the **nearest-rank** p50, p95 and max equal the targets exactly. Sizes are clamped to the entities that exist, and `workload.json` records `clamped`.

- **Nearest-rank:** the q-quantile of `n` sorted values is the value at zero-based index `ceil(q*n) - 1`.
- **Run hash:** BLAKE3 over the run's sorted keys, each as 8 big-endian bytes.
- **Workload hash:** BLAKE3 over the 32-byte run hashes concatenated in run-id order.

Neither hash depends on how the Parquet is encoded.

## Ground truth (`expected.json`, `format: "bellwether-bench-expected/3"`)

`expected.json` has one entry per rule from `docs/rule-catalog.md`. Each entry has:

- **`planted`:** measured from the data, with the criterion and this corpus's numbers in `criterion`.
- **`fanout`:** exact, as a `bellwether_types::Fanout` with `sampled: false`, plus a log2 histogram of row groups per value.
- **The expert fix:** `fix` is a `RewriteShape` in the catalog's guidance-template shape. `fix_key_spec` or `fix_layout` holds the implied spec or clustering, and `fix_fanout` is the exact fanout of the key the fix introduces.

| `rule` | Columns | Planted when (thresholds in `truth.rs`) | Expert fix |
|---|---|---|---|
| `batch_stamp_key` | `batch_id` | `stats::batch_stamp`: p50 files per value ≥ 2, p50 row groups ≥ 2 × the fix's p50, and cross-day concordance with `version_ts` ≥ 0.9 over ≥ 2 days (shared decision, below) | `join on batch_id` → `join on (entity_id, batch_id)` |
| `low_selectivity_single_column` | `status` | the median value touches ≥ 50 % of row groups and is ≥ 2 × the fix's p95 (**deferred**: every run supplies only `entity_id` and no key spec on `status` is declared) | `key = [status]` → `key = [status, entity_id]` |
| `scattered_time_column` | `version_ts` | overlap depth ≥ 1.5 and sortedness < 0.9 (shared measure, below) | cluster by `version_ts` within each partition |
| `lookback_unbounded` | `entity_id` | the p95 entity's history spans ≥ 2 row groups (**deferred**: needs a consumer query plan no record carries; the workload is locate-only) | `Temporal [entity_id] by version_ts`, then `LatestPerKey` |
| `declared_but_unused_key` | — | deferred until a manifest fixture exists | remove the spec |

A layout with one file per day and no late rows plants neither `batch_stamp_key` nor `scattered_time_column`. The recipe layouts plant all four measured rules, and tests check both cases. The four corpora of the PR 17 review (two time-scatter cases that sat on the old gap-ratio threshold, one day, and two days) keep their measured flags in `tests/bench.rs`. A deferred rule keeps its measured `planted` value; `deferred` names the input it is missing.

**Shared decisions.** Two planted flags are not decided here.
- `scattered_time_column` is `bellwether_source::stats::overlap_depth` of `version_ts`.
- `batch_stamp_key` is `bellwether_source::stats::batch_stamp`: its files gate, its ratio against `(entity_id, batch_id)`, and its concordance with time.

Both read the corpus's footer statistics through `bellwether_source`, at the thresholds defined there. The profiler's lints call the same functions.
- The scatter flag and its lint agree by construction.
- The composite partner is chosen by `bellwether_source::stats::partner_order` on both sides. Every bench workload supplies `entity_id` (`WORKLOAD_KEY_COLUMNS`), so with it the profiler's only candidate is `entity_id`, as here, and the batch-stamp and low-selectivity flags agree with its lints by construction. A fully unique `entity_id` is still the partner, because the workload supplies it.
- **Overlap depth** of `version_ts` is the sum of the closed row-group ranges' lengths divided by the length of their union. A range is excluded as a sentinel only when it is longer than 8 times the median range and reaches outside the span of the others, at most 4 of them; late rows never qualify.
- **Sortedness** is the fraction of consecutive within-file row groups whose ranges ascend without overlapping.
- **Cross-day concordance** places each row group on the UTC day of its `version_ts` maximum. Over every pair of row groups on different days, it is the fraction whose `batch_id` minima are strictly in day order. With one day it is undefined, and the batch stamp is not planted.

**How fanout is counted.** It is exact, without sets: per value, the scan keeps the last row group and file seen and increments a counter when either changes. Memory:
- `O(entities)` for `entity_id`. The same tracker is reused for five `(status, entity_id)` passes.
- `O(rows in one day)` for `batch_id` and `(entity_id, batch_id)`. A batch never crosses a day, and the scan checks this.
- `O(row groups)` for the footer statistics the shared time measures read.

## Harness

```rust
pub trait IndexUnderTest {
    fn name(&self) -> &str;
    fn plan(&self, keys: &[Key]) -> ScanPlan; // keys sorted, deduplicated
}
```

Plan entry paths are relative to the corpus root (as in `corpus.json`) or absolute under it. Fingerprints must match on size and tail hash. Name the index `index` and pass it to `harness::run_corpus` as an extra `&dyn IndexUnderTest`.

| Strategy | Kind | What it does |
|---|---|---|
| `raw_scan` | plan | Reads every row group. |
| `parquet_stats` | plan | Parquet-native pruning: footer min/max of `entity_id`, then the row group's embedded bloom filter. |
| `datafusion` | plan | Apache DataFusion's Parquet scan pruning (statistics, page index, bloom filters), behind `--features datafusion`. The plan is the set of row groups whose column chunks DataFusion read. `plan_ms` covers that pruning scan, and its bytes are reported as `plan_bytes_read`; `scan_ms` is the execution. |
| `duckdb` | engine | Runs `read_parquet(...) WHERE entity_id IN (SELECT entity_id FROM read_parquet(run))` through the CLI and aggregates every column. Latency is DuckDB's `.timer` query time. |
| `index` | plan | Reserved for WP1/WP4. |

**What the harness trusts.** Nothing a plan says about itself.
- `verify_plan` resolves every entry against the corpus and recomputes files, row groups and bytes from the footers.
- An unknown file, an out-of-range row group or a stale fingerprint makes that run a failed result with an `error`. So does a panicking `plan()`.
- A plan whose own counts disagree is still executed and scored on the recomputed cost, and flagged `cost_misreported`.

**Per run**, the harness:
- computes the **oracle**: the row groups that actually hold a key, found by scanning `entity_id`. It is the exact minimum and the false-negative reference (invariant 1).
- **executes** each plan: it reads every planned row group with all columns, filters to the keys, and records matched rows, the exact `amount` total, and an order-independent digest of every matched row.
- **measures bytes** inside the `ChunkReader` that parquet-rs drives, at column-chunk granularity. The first request into a chunk fetches the whole chunk, as object-store readers do. It records `bytes_read` (fetched), `bytes_consumed` (handed to the decoder), and their difference from the planned bytes.

## `results.json` schema (`format: "bellwether-bench-results/2"`)

```jsonc
{
  "format": "bellwether-bench-results/2",
  "generated_by": "bellwether-bench 0.1.0",
  "environment": {
    "os", "os_version", "kernel", "arch", "cpu_model", "cores", "memory_bytes",
    "rustc", "commit", "commit_dirty",
    "storage": "local disk", "cache": "warm: ...", "duckdb_cli": "v2.0.0 ..." | null,
    "corpus_parameters": [{ "corpus_id", "path", "params" }] },
  "corpora": [{                       // one per --corpus, in order
    "corpus_id": "<blake3 of corpus.json>",
    "path": "benches/out/small",
    "params": { "seed", "files", "row_groups_per_file", "rows_per_row_group",
                "entities", "churn", "days", "payload_bytes", "late_fraction" },
    "files_total", "row_groups_total", "rows_total",
    "bytes_total",                    // compressed row-group bytes: x axis of latency vs size
    "file_bytes_total",               // on-disk bytes including footers
    "generator": {                    // generator-stats.json, or null
      "peak_buffered_bytes", "memory_bound_bytes",
      "rss": { "method", "baseline_bytes", "peak_bytes", "growth_bytes", "samples" } | null,
      "rss_growth_bound_bytes", "row_groups_written", "rows_written", "elapsed_ms" },
    "workload": { "hash", "runs", "sizes_p50_p95_max": [40, 120000, 415000], "clamped" },
    "scan_executed": true,            // false with --no-scan
    "strategies": [{ "name", "kind": "plan" | "engine", "available", "setup_ms", "note" }],
    "runs": [{
      "run_id": "run-0000", "keys": 40, "key_hash": "<blake3>",
      "oracle": { "files", "row_groups", "bytes", "matched_rows", "amount_total": "123.45" },
      "funnel": { "row_groups_total", "after_stats", "after_index": null, "oracle" },
      "results": [{                   // one per (strategy, run): the "(baseline, lookup)" object
        "strategy": "parquet_stats",
        "cost": { "run_id", "keys", "files", "row_groups", "bytes", "exact": true },  // RunCost, recomputed from footers; null for engines and invalid plans
        "reported": { "bytes", "row_groups_selected", "row_groups_total", "entry_bytes_mismatches" },
        "cost_misreported": false,
        "plan_ms",                    // planning latency
        "plan_bytes_read",            // bytes read while planning (DataFusion's pruning scan); 0 for baselines
        "scan_ms",                    // execution latency, through the byte-counting reader
        "latency_ms",                 // plan_ms + scan_ms; query time for engines
        "bytes_read",                 // measured: fetched from storage
        "bytes_consumed",             // measured: handed to the page decoder
        "bytes_read_minus_planned": 0,
        "matched_rows", "amount_total", "rows_digest",
        "false_negative_row_groups": 0,
        "peak_memory_bytes": null,    // the index will report this (ADR 018)
        "error": null }]
    }],
    "build": [],                      // WP4 index builds: { strategy, workers, peak_memory_per_worker_bytes, elapsed_ms }
    "summary": [{ "strategy", "runs", "latency_ms": [p50, p95, max], "plan_ms",
                  "bytes_planned_total", "bytes_read_total", "row_groups_total" }],   // display only
    "ground_truth": { /* expected.json embedded, or null */ }
  }]
}
```

All quantiles are nearest-rank.
- **Funnel:** plot `runs[].funnel`.
- **Latency against corpus size:** plot `corpora[].summary[].latency_ms[0]` against `corpora[].bytes_total`.
- **Realized against predicted:** plot `results[].bytes_read` against `results[].cost.bytes`.

## SLOs (`--assert-slo`)

Every check reads per-run data, never `summary`. Each has three outcomes: PASS, FAIL, or SKIP (nothing to compare). In strict mode, a SKIP on a core check counts as a FAIL. The core checks are `same_rows`, `bytes_read_matches_plan`, and `generator_rss_growth` on Linux.

| Check | Holds when |
|---|---|
| `generator_rss_growth` | RSS growth is within its bound (ADR 018). SKIP where there is no probe. |
| `generator_buffered_bound` | Buffered bytes are within their bound. |
| `harness_completes` | Every run has a result from every available strategy, with no errors. |
| `baselines_present` | `raw_scan` and `parquet_stats` both ran. |
| `no_false_negatives` | No plan misses an oracle row group (invariant 1). |
| `plan_cost_reported` | Every plan's own counts equal the recomputed cost. |
| `bytes_read_matches_plan` | For every executed plan, fetched and consumed bytes equal the planned bytes. |
| `same_rows` | Matched rows and the exact amount total equal the oracle's for every strategy that read rows, and plan strategies' row digests equal `raw_scan`'s. |

**Named budgets.** They are in `src/slo.rs` and stay `None` until the index lands. `slo::Budgets` lets tests set them. Once set, a budget never passes vacuously.

| Budget | Fails when |
|---|---|
| any `INDEX_*` | there are no `index` results |
| `INDEX_PLAN_P95_BUDGET_MS` | the sample is empty, or p95 is over budget |
| `INDEX_LATENCY_P95_BUDGET_MS` | scans were off, or the sample is empty, or p95 is over budget |
| `INDEX_LOOKUP_MEMORY_BOUND_BYTES` | any index result lacks `peak_memory_bytes`, or one is over budget |
| `INDEX_MAX_ROW_GROUPS_OVER_ORACLE` | the sample is empty, or the worst ratio is over budget |
| `BUILD_MEMORY_BOUND_PER_WORKER_BYTES` | judged on `build[]` results only: there are none, or any is unmeasured or over budget |

`OBSERVE_LATENCY_BUDGET_MS` (100 ms, from `docs/architecture.md`) is recorded there too; WP12 asserts it.

`tests/slo_gate.rs` covers each failure mode a reviewer would probe:
- budgets with no index or build results;
- empty samples;
- unknown files, out-of-range row groups, stale fingerprints and panicking plans;
- a misreported cost;
- scans turned off;
- an executor that drops a row group or a column, or fetches an extra row group.

## Report

`report` writes one HTML file with no scripts and no external resources. Charts are inline SVG drawn to scale from `results.json`:

- latency against corpus size (log-log, one line per strategy),
- the pruning funnel per run (log scale: total, after statistics, after the index, with the oracle as a tick),
- the planted-pattern table and fanout histograms from the ground truth.

Hovering a mark shows a native tooltip, and each chart has a table view. Light and dark themes follow `prefers-color-scheme`.

## Bitemporal `market` corpus (WP21)

`bellwether_bench::market` (and `bellwether bench market` in the CLI) generates a deterministic synthetic bitemporal market trade corpus so every WP21 time-series capability gets a measured funnel:

```sh
bellwether bench market --out benches/out/market-small --rung small
```

**Layout and schema.** Files are written under `<out>/data/day=YYYY-MM-DD/part-NNNN.parquet` (Hive directory partitioning; no `day` column inside the Parquet files) with five non-null columns:

| Column | Type | Role |
|---|---|---|
| `symbol` | `Utf8` | Symbol identifier (`S0001..=S{symbols:04}`). |
| `ts` | `Timestamp(µs, "UTC")` | Event / valid time (within trading hours `14:30:00..21:00:00` UTC). |
| `known_at` | `Timestamp(µs, "UTC")` | Knowledge / transaction time (`ts + 0..=500 ms` for on-time prints; 1–3 days later for late prints and restatements). |
| `price` | `Float64` | Trade price (`10.00..=500.00`). |
| `size` | `Int64` | Share quantity (`1..=10,000`). |

- Within each partition day `day=YYYY-MM-DD` (the row's `known_at` day), rows are sorted by `(symbol, ts, known_at)` and cut across `files_per_day` files and row groups of `rows_per_row_group` rows so each symbol's rows straddle row groups.
- **Late prints** (`late_fraction`): arrive 1–3 days after their `ts` day and live in their `known_at` day's partition. Late arrivals whose `known_at` day falls beyond the corpus's last day are **dropped** (never clamped).
- **Restatements** (`restatement_fraction`): emit both the original on-time print on its `ts` day and a corrected row with the same `(symbol, ts)`, a different `price`, and `known_at` 1–3 days later in that later day's partition (dropped if beyond the last day).

**Presets (`MarketRung`).**

| Rung | `seed` | `symbols` | `days` | `start` | `files_per_day` | `rows_per_row_group` | `prints_per_symbol_per_day` | `restatement_fraction` | `late_fraction` |
|---|---|---|---|---|---|---|---|---|---|
| `Small` | `21` | `50` | `10` | `2026-01-01` | `2` | `500` | `200` | `0.05` | `0.05` |
| `Medium` | `21` | `200` | `30` | `2026-01-01` | `4` | `2000` | `300` | `0.05` | `0.05` |

**Workload and ground truth.** `generate_market` also writes:
- `<out>/workload/market.json` and single-column `symbol` key files `<out>/workload/keys/qNNNN.parquet` covering `window` queries (1-hour intraday, 1-day, and 5-day windows across symbol sets of 1, 10, and 50), bitemporal `asof` queries (including `K` before and after restatements and late arrivals), and `latest_known` queries.
- `<out>/workload/truth.json`: exact ground truth computed directly from the generator's in-memory rows (for `window`: `matching_row_groups`, `matching_rows`, `symbol_row_groups`, `ts_overlap_row_groups`, and `partition_days_touched`; for `asof` and `latest_known`: per-symbol winning `(ts, known_at, price)` and every `(file, row_group)` holding it).

## Reading planned row groups (`bellwether bench read`, WP22)

`bellwether_bench::perf` reads row groups through the harness's byte-counting reader, for the release measurements of [`scripts/perf.sh`](../../scripts/perf.sh):

```sh
bellwether locate data/ --keys keys.parquet --json > plan.json
bellwether bench read --plan plan.json --json   # the plan's row groups
bellwether bench read --all data/ --json        # every row group: the full scan
```

- **What it reads.** Every column of every named row group (the plan inside a lookup's `--json` output, `evidence.record.body.plan.entries`, or every row group of every `.parquet` file under a folder, skipping directories that start with `.`), files opened and row groups read in parallel on the rayon pool. Rows are decoded but not filtered to the keys.
- **What it reports** (`read` in the JSON): `files`, `row_groups`, `rows`, `bytes_fetched` and `bytes_consumed` (counted inside the reader, as in `results.json`), `footer_ms` (parsing the footers of the files read), `read_ms`, and `total_ms`.
- **Refusals.** JSON without a plan is refused, naming the output to pass; one of `--plan` or `--all` is required.

## Sketch funnel (B2): "is this column a key?" over 10⁴ small files

`scripts/sketch-funnels.py --bin <bellwether>` measures, through the CLI only, how many data files and footers a `why` diagnosis reads with and without a `sketch` sidecar, on both bench shapes, each over at least 10⁴ files:

| Shape | Corpus | The question |
|---|---|---|
| ledger | `bench generate --files 10000 --days 1 --row-groups-per-file 1 --rows-per-row-group 1000 --entities 1000000 --churn 1 --payload-bytes 16`: 10⁷ rows, one 1,000-row row group per file, so the day's 72 twenty-minute `batch_id` windows each cover about 139,000 rows beside `entity_id` | is `batch_id` a key, or a batch timestamp shared by many rows (`R-sketch-1`, the low-cardinality key)? |
| time series | `bench market --rung small --files-per-day 1000`: 10 days × 1,000 files | is `(symbol, ts)` unique per file (vendor onboarding)? |

For each it records `reads.files`, `reads.row_groups`, and `reads.footers` of `why` with no index, `why --sketches-only` (run with every data file made unreadable, mode 000, so a single open would fail it; it must report zero of each), and `why` with the sketch present; the row groups the one sketch build read; and every item's approximate distinct count and interval from `sketch cardinality` beside an independent oracle (pyarrow over the decoded files: exact distinct values and the files that repeat a value), with the detection floor under which a file's repeats go unflagged (`duplicate_floor`). Read counts are exact; distinct counts from a sketch are approximate and printed with their interval. No timing is measured. The measured tables, labelled with machine and build, are in the B2 pull request.

## List corpora (`bench lists`, B4)

`bellwether bench lists --shape ledger|market --out DIR` writes a corpus with a list column of skewed element cardinality (`src/lists.rs` documents the model): the ledger shape's `attributes: List<Int64>` on flat files, the market shape's `holdings: List<Utf8>` under one `day=` directory per day. The preset is 40 files (or days) of 4 row groups of 5,000 rows, 200,000 element values with a head of 50 shared by every row group and a long tail clustered by position, 2% null and 3% empty lists. `workload/near-{10,1000,100000}.parquet` are `contains_any` runs whose terms belong to one period (consecutive tail values), `workload/any-{10,1000,100000}.parquet` `contains_any` runs whose terms are drawn uniformly from the whole vocabulary, and `workload/all-2.parquet` a `contains_all` run, each one column named after the list column; `workload/truth.json` gives every matching row group and row count, computed while the rows are generated. `scripts/list-funnels.py --bin target/debug/bellwether` drives the CLI over both shapes (why on the values, `index --key list:<column>`, `locate --any/--all`), asserts invariant 1 against the truth and that `why` predicted what `locate` planned, and prints the funnel labelled with the machine and build. The Parquet-statistics baseline reads every row group for a membership run: a list's statistics are on its nested leaf.
