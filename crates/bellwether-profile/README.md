# bellwether-profile

Work package WP5. This crate measures which columns of a folder of columnar files discriminate rows and which only look like they do. It produces:

- the fanout of every candidate key column and candidate composite,
- range statistics: typical overlap and sortedness from footers, and for date and timestamp columns the overlap depth defined in `bellwether_source::stats`,
- the statistics-only lints of [`docs/rule-catalog.md`](../../docs/rule-catalog.md),
- a recorded-workload summary,
- a `profile` Evidence whose provenance names every input it was derived from.

It owns **invariant 6** for Evidence ("every Evidence carries provenance", shared with `bellwether-ledger`). It implements ADR 014 (nulls), 016 (canonical encoding), and 018 (bounded memory). Following ADR 005, an estimate is labeled as one.

Dependencies: `bellwether-types`, `-core` (canonical encoding), `-source` (sources and the shared measures of `bellwether_source::stats`), `-store` (cache, manifest), `-ledger` (Evidence writes), as `dev/check-layering.py` permits.

## API

| Item | What it does |
|---|---|
| `profile(source, root, store, &ProfileOptions) -> ProfileReport` | Measures everything below. `store` is rooted at the index root (`docs/format.md`): the cache is at `cache/`, the manifest, if any, at `manifest.json`. The folder need not be indexed. |
| `manifest_profile(root, store, &ProfileOptions, DatasetTotals) -> ProfileReport` | A profile that reads no data file, footers included: the caller's totals (files, live row groups, rows; the facade takes them from the manifest and a sketch) stand in for the footer pass, nothing is measured, and the only provenance input is the manifest at the store root, which must exist. For `why --sketches-only`, whose lints come from sketches. |
| `ProfileOptions::for_source(config)` | The defaults with the required `source_config` set. `ProfileOptions::default()` leaves it unset, and `profile` refuses that. |
| `evidence(&report) -> Evidence` | Builds the `profile` Evidence. Refuses a report with no provenance inputs. |
| `record(&ledger, &report)`, `profile_and_record(..)` | Write it through the ledger. |
| `render(&report, evidence_hash) -> String` | The one-screen summary (below). |
| `ProfileReport` | `Serialize`, for `--json`. It carries more than the Evidence can: sampling detail per fanout, the candidates auto selection skipped, range statistics, time measures, provenance inputs with ages, the memory report. |
| `bin/bellwether-profile` | A thin wrapper for running a profile by hand until WP9's facade calls `profile` directly (ADR 010). Its source configuration is `parquet`. |

`ProfileOptions` fields:

| Field | Default | Meaning |
|---|---|---|
| `columns` | auto | Candidate key columns. |
| `time_columns` | all dates and timestamps | Columns for the range analysis. |
| `max_tuple_len` | 2 | Largest composite `k`. |
| `max_tuples` | 64 | Most composites measured. |
| `memory_budget` | 256 MiB | Bytes for all sampler tables. |
| `max_tracked_per_key` | from the budget | Per-key table size. |
| `hll_precision` | 14 | Sketch precision. |
| `seed` | fixed | Value-hash seed, so a profile is reproducible. |
| `workload` | none | A bench-format workload directory. |
| `source_config` | **none: required** | Folded into the footer cache key; unset is an error. It must change whenever the source would report other row groups for the same files, for example a text source's chunk size (`csv:chunk=67108864`). A Parquet source is `parquet`. |
| `reuse_cache` | true | `false` recomputes and overwrites the cached artifacts. |
| `now_millis` | `None` | The instant the manifest's commit age is measured at (milliseconds since the epoch); `None` is the system clock. The facade passes its fixed `--clock` here, so an indexed folder's Evidence is byte-identical across runs. |

## Fanout

For a key (one column or a tuple), fanout is the distribution, **over distinct values**, of the number of files and the number of row groups each value occurs in. It reports `[p50, p95, max]`, with `files_total` and `row_groups_total` as denominators.

- **Quantiles are nearest-rank.** For `n` sorted values, the q-quantile is the value at zero-based index `ceil(q·n) − 1`. This is the definition the benchmark's ground truth uses.
- **Values are canonical bytes.** Each value is encoded as its column's dataset type (ADR 016, `bellwether_core::encoding`), so files at another decimal scale or timestamp unit agree. A file value that does not convert is an error.
- **Tuples follow `KeyColumns` exactly (ADR 014 amended).** A row whose leading column is null is not counted. A null trailing column is the `0x00` marker. A unit test checks byte equality with `KeyColumns::encode`. Fanout therefore describes the keys a segment built on that tuple would hold.
- **Order.** Files are visited by path and row groups by index, each once, so a per-value counter that increments when the row group (or file) changes counts exactly, without a set per value.
- **Candidates.** By default, every id-like column is a candidate: integers, strings, binary, and dictionaries of them.
  - A string or binary column wider than 256 compressed bytes per row (a document or blob, not a key) is skipped. No identifier is that wide, so a UUID in text, or a short id in tiny row groups whose chunk overhead inflates bytes per row, stays a candidate. Integers are never skipped by width.
  - Floats, decimals, booleans, and dates and times are not ids.
  - Every column not selected is listed in `ProfileReport.skipped` with its reason.
- **Composites, in measured order.** Every combination of 2..=`k` candidates is measured from the same key reads. Each composite's columns are ordered before measuring: the columns the recorded workload supplies first, the rest in schema order. A composite is reported, and recommended by a lint, in exactly the order it was measured; it is never relabeled. With nulls the order changes the keys a tuple holds (ADR 014), so a relabeled composite would show another tuple's numbers.
- **Ranking.** `ProfileReport.fanout` lists single columns in schema order, then composites ranked by p95 files per value over `files_total`, then by p95 row groups over `row_groups_total`.

### Exact until the table fills, then sampled by value

Every key has a [`DistinctSampler`](src/sampler.rs): a table of exact per-value counters, keyed by a seeded 128-bit hash of the canonical bytes, with at most `high_water` entries.

- **Unsampled (`sampled: false`).** If the table never fills, every distinct value was counted. `distinct` is the table size and the quantiles are exact.
- **Sampling is a reservoir keyed by hash priority.** When the table is full, it keeps the half with the smallest hashes and lowers the admission bound to the largest hash it kept. The bound only falls. Every value at or below the final bound was admitted at its first occurrence and never evicted, so **its counts are exact**. The final table is exactly the set of distinct values hashed at or below the bound: a uniform sample by value, never by row.
- **`distinct` when sampled** comes from a fixed-size HyperLogLog sketch: 2^p one-byte registers and Ertl's improved estimator, with relative standard error `1.04/√2^p` (0.81 % at p = 14), raised to at least the retained sample size. It is capped at the dataset's exact row count, which the footers give: no key has more distinct values than there are rows. A capped key has `SampleInfo.capped_at_rows` set, and the summary notes the cap and the estimate.
- **Footer distinct counts are checked against the sketch.** When every row group of a single column reports `distinct_count`:
  - Over row groups whose ranges are pairwise disjoint, each value lives in one row group, so the sum of the counts is the distinct count, and the quantiles are `[1, 1, 1]`. That sum is used, and the fanout reported unsampled, only after the sketch agrees with it to within 4 standard errors (`FooterDisjoint`).
  - Otherwise the counts only bound the answer: at least the largest per-row-group count, at most their sum. The estimate must lie within those bounds widened by 4 standard errors, and is then clamped into them (`HllWithinFooterBounds`).
  - Footers that disagree with the sketch by more than that are refused with an error, so an approximate or null-counting writer that far off is never reported as exact. An error under 4 standard errors (3.25 % at p = 14) cannot be told from the sketch's own and passes.
- **Each key states its own error.** `SampleInfo.quantile_rank_error` is the DKW bound `ε = sqrt(ln(2/α) / 2n)` at α = 0.001, where `n` is that key's retained values; `SampleInfo.rate` is its sample rate. Each reported quantile lies, with probability at least 99.9 %, between the population quantiles at ranks `q − ε` and `q + ε`; the sampled max is a lower bound. The summary prints both numbers on every sampled key's row.
- **Labeling.** `Fanout.sampled` is true whenever `distinct` or the quantiles are estimated, and every lint built on a sampled fanout has low confidence.

## Range analysis

From footers only, so nothing here reads row data:

- **`ColumnRanges`, for every analyzed column:**
  - `typical_overlap`: for each row group, the fraction of row groups whose `[min, max]` intersects its own; the median over row groups. A row group with values but no bounds can never be pruned, so it intersects everything. Reported for reading, not used by any lint.
  - `sorted_fraction` and `within_file_sorted_fraction`: consecutive row groups, in dataset order or within one file, whose ranges ascend without overlapping.
  - `disjoint`: every range intersects only itself.
- **`TimeScatter`, for date and timestamp columns:** the overlap depth and sortedness of `bellwether_source::stats::overlap_depth`, the one definition the benchmark's measured `planted` flag also uses.
  - *Depth* is the sum of the closed row-group ranges' lengths divided by the length of their union. Sorted data has depth 1 however sparse or uneven; `k` parallel writers over one slice of time give `k`. A *sentinel* range (an open-record placeholder such as 9999-12-31) is excluded and counted: longer than 8 times the median range and reaching outside the span of the ranges that are not that long, at most 4 of them (none when more qualify). Late rows stay inside the data's span, so they always count.
  - *Sortedness* is the fraction of consecutive within-file row groups whose ranges ascend without overlapping, sentinels included. Late rows pull a row group's minimum back below its predecessor's maximum; parallel writers do not.
  - Recomputed from the cached footers on every profile. It is never cached, so changing the time columns can never serve a stale measure.

## Lints

| Rule | Fires when | Confidence |
|---|---|---|
| `batch_stamp_key` | `bellwether_source::stats::batch_stamp` holds against the composite of B with some id-like partner K, tried in the shared partner order (below): <ol><li>one value of B touches ≥ 2 files at the median;</li><li>B's median row groups per value is ≥ 2 × the composite's;</li><li>B concords with a time column across days, at least 0.9 over at least two days.</li></ol> | high with a workload and the composite's p95 files ≤ half of B's; medium otherwise; low if sampled |
| `low_selectivity_single_column` | C's median value touches ≥ 50 % of row groups, an id-like O that is not a batch stamp makes the composite's p95 ≤ half that median, and `batch_stamp_key` does not fire on C (a batch stamp is not a key at all, so neither the column nor the partner). | high if the workload supplies both columns; medium if it supplies one (which then leads), or with no workload; low if it supplies neither, or if sampled |
| `scattered_time_column` | A date or timestamp column's overlap depth is ≥ 1.5 and its sortedness < 0.9 (`bellwether_source::stats`). | medium (statistics alone; the bench workload records key sets, not range queries) |
| `declared_but_unused_key` | A manifest declares a spec, a workload exists, and no run supplies the spec's leading column. | medium (whether the workload covers the full usage window is unknown) |
| `R-sketch-1` (low-cardinality key, `low_cardinality_key`) | From `sketch` sidecars, not from this crate's own pass (the facade folds the sketch segments into `SketchedColumn`s and adds the lints to the profile before its Evidence is built): a sketched column B whose values each cover ≥ 100,000 rows on average (rows ÷ distinct), beside an id-like sketched column K with ≥ 10 × B's distinct values, in ≥ 90 % of the files B is in. Every threshold is inclusive. The advice is the composite (K, B); the partner with the most values is named. No data file is read. | medium (the distinct counts are approximate); high when both counts are exact (a complete `exact` key spec on the column) |

How the thresholds and choices were made:

- **Shared decisions.** `scattered_time_column` and `batch_stamp_key` are not reimplemented. They are `bellwether_source::stats::overlap_depth` and `bellwether_source::stats::batch_stamp`, and the composite partner of both composite rules comes from `bellwether_source::stats::partner_order`: the functions the benchmark's `truth.rs` calls.
  - The scatter lint and its planted flag agree by construction.
  - The benchmark's only partner candidate is `entity_id`, which its workload supplies. Run with that workload, the profiler considers only `entity_id` too, so the batch-stamp and low-selectivity lints agree with their flags by construction. Without a workload supplying `entity_id`, the profiler may judge another partner.
  - The low-selectivity thresholds are the benchmark's.
  - `tests/thresholds.rs` tests every threshold from both sides, with expected values written out rather than read from the constants.
- **Time concordance.** An id sorted within each daily file restarts every day, and a range-partitioned id repeats every day. Both fail time concordance: with the same values every day, under half of the cross-day pairs are strictly in day order. A write stamp passes. With one day, concordance is undefined and the lint stays silent.
- **Partner order** (`bellwether_source::stats::partner_order`).
  1. A row id is refused unless the recorded workload supplies it: a column whose *exact* distinct count reaches the row count. A sampled count, within the sketch's error, never makes a row id. Supplied, it is the workload's key.
  2. When the workload supplies any candidate, only supplied candidates count.
  3. A repeating candidate ranks before a near-unique one (at least 90 % of rows distinct), then the smaller composite p95, then schema order.
  
  A batch stamp is never a low-selectivity partner. That guard only knows the stamps `batch_stamp_key` established, though. Without a workload, a stamp the lint could not establish can still be paired with: for example, one confined to a single day, or one whose composites all fail the ratio. Low selectivity may then recommend a composite with it.
- **Composite order.** A recommended composite is the measured one, its supplied columns first. Without a workload the message says which column the order assumes lookups supply.
- **Confidence scale.** High, medium, and low map to 0.9, 0.6, and 0.3. Every message is one sentence and `site` is `None`.
- **Out of scope.** `unprefiltered_window`, `lookback_unbounded`, and `wide_projection_before_filter` need a consumer's query plan, and no `EvidenceBody` carries one. This crate never emits them.

## Workload summary

`workload_keys` holds the nearest-rank p50, p95, and max of distinct keys per recorded run. The only recorded-workload format that exists today is the benchmark's (`bellwether-bench-workload/1`), and nothing in its `workload.json` is trusted:

- every run file is read back (one run's keys at a time), deduplicated, counted, and hashed;
- the run hashes and the workload hash must match the file;
- a run path must stay inside the workload directory.

WP6 owns the recorder and may define its own format.

## Cache

Three artifacts are stored by input hash in `cache/<hash>.bin`, each with its `.meta.json` age sidecar:

- **Footer statistics.** Key: the root, every file fingerprint, the dataset schema, `source_config`, and the encoding version.
- **Fanout table.** Key: the footer key, the candidate columns and their types, every tuple in its measured order, the table size, sketch precision, seed, and α. It holds integers and flags only (`CachedEntry`): counts, the admission bound as hex, the rounded sketch estimate, whether `distinct` was capped at the row count. Every float a report shows (sample rate, rank error) is derived from those integers by the functions a fresh profile uses, so a reused profile is bit-for-bit the fresh one. `FANOUT_CACHE_FORMAT` is `bellwether-profile/fanout/3`, and a unit test pins the layout to it.
- **Workload summaries.** Key: the `workload.json` bytes and every run file's fingerprint.

Every key also carries the artifact's format version. A cached sampled table therefore cannot be served for a request that would be exact, and a changed file changes every key derived from it.

On a cache hit, a profile reads exactly this:

- one `Source::schema` call, which for a Parquet source without a supplied schema lists the root and parses the first file's footer;
- one `discover`, which fingerprints every file (the last 64 KiB of each under the default policy);
- the cached artifacts and their age sidecars;
- `manifest.json`, when the folder is indexed;
- with a workload, `workload.json` and each run file's fingerprint.

`Source::row_groups` is never called and no row data is read. `tests/cache.rs` counts these source calls with a spy. The cache key is what provenance cites, and the sidecar age is the age reported.

## Evidence and provenance (invariant 6)

`provenance.inputs` lists, in order:

1. the footer-statistics artifact;
2. the fanout-table artifact;
3. with a workload, the workload-summary artifact and the workload's own hash;
4. in an indexed folder, the manifest's content hash.

The other provenance fields:

- `oldest_input_age` is the largest of their ages: the cache sidecar age, the workload manifest's mtime age, and the manifest's commit age (measured at `now_millis` when given, zero when that is before the commit). Tests back-date a workload and a manifest and check each becomes the oldest age.
- `manifest_version` is the manifest sequence (ADR 020), or `None` for an unindexed folder.
- `created_at` is RFC 3339 UTC.

`evidence` refuses a report with no inputs before the ledger sees it, and the ledger refuses one again. A manifest that indexes a different root than the one profiled is an error.

## Cost

The first profile of a folder reads data; a repeat reads only what the cache key needs (above).

- **The fanout pass dominates a cold profile.** It reads every candidate key column of every row group, and nothing else (`tests/memory_bound.rs` checks this with a spy). Per row, it encodes canonical bytes and hashes them once per single column and once per composite. Footers are metadata only.
- **What drives the cost.** Auto selection takes every identifier-typed column, so a wide table makes the pass read nearly every column, with up to `max_tuples` (64) composites on top.
- **Example.** The WP9 facade measured a ClickBench slice: 10 files, 28 row groups, 10,000,000 rows, about 100 columns, in a debug build. The cold profile took 615 s and the warm cache 0.12 s.
- **What to do about it.** Pass `columns` to name the candidate keys; that is the lever when a profile must be fast. A release build is also much faster than debug.

## Memory (ADR 018)

- **Key reads.** One row group's candidate key columns per `read_columns`, dropped before the next. Time columns are never read: their measures come from footers.
- **Sketches.** Fixed size, `keys × 2^p` bytes: 16 KiB per key at the default precision.
- **Sampler tables.** `high_water` is chosen as the exact capacity of a power-of-two bucket count `B`, so a table never grows past `B` buckets of 33 bytes (`(u128, Track)` plus a control byte). Its transient peak is `1.5 · B · 33` bytes, during a resize or the shrink after an eviction. `MemoryReport.tables_bound_bytes` states the total. It fits in `memory_budget` unless the budget allows less than the 16-bucket minimum table per key, or `max_tracked_per_key` overrides the budget.
- **Footer statistics.** The statistics of every row group are held, `O(row groups × columns)`, the same order as the footers and the manifest's row-group space.
- **Workload summary.** One run's key set at a time.

`tests/memory_bound.rs` checks the bound two ways. The observed table capacity never exceeds `high_water`, with the same stated bound on a corpus eight times larger. And a control run without eviction must exceed it.

## One-screen output

```text
bellwether profile data
24 files · 48 row groups · 12,000 rows · not indexed · workload 20 runs, keys p50 10 / p95 300 / max 700

fanout: files and row groups one value touches, p50 / p95 / max
  key                        distinct  files/value         row groups/value    sample
  entity_id                    7,002   2 / 3 / 3           2 / 3 / 3           exact
  batch_id                       216   8 / 8 / 8           8 / 8 / 8           exact
  status                           5   24 / 24 / 24        48 / 48 / 48        exact
  payload                     12,000   1 / 1 / 1           1 / 1 / 1           exact
  (entity_id, batch_id)       12,000   1 / 1 / 1           1 / 1 / 1           exact
  (entity_id, payload)        12,000   1 / 1 / 1           1 / 1 / 1           exact
  (batch_id, payload)         12,000   1 / 1 / 1           1 / 1 / 1           exact
  (status, payload)           12,000   1 / 1 / 1           1 / 1 / 1           exact
  (entity_id, status)          9,591   1 / 2 / 3           1 / 2 / 3           exact
  (batch_id, status)             932   5 / 8 / 8           5 / 8 / 8           exact
  not candidates: version_ts (Timestamp(µs, "UTC"): a time column, analyzed for range overlap); amount (Decimal128(18, 2): not an identifier type)

ranges
  entity_id              typical overlap 100%  sorted within files   0%
  batch_id               typical overlap  17%  sorted within files 100%
  status                 typical overlap 100%  sorted within files   0%
  payload                typical overlap 100%  sorted within files   0%
  version_ts             typical overlap  83%  sorted within files  33%  overlap depth 23.51, 33% of consecutive row groups ascend (scattered)

lints
  batch_stamp_key [batch_id] high 0.90
    batch_id looks like an id but behaves like a write-batch stamp: it rises with version_ts across days, and one value touches 8 of 24 files and 8 row groups at the median, against 1 for (entity_id, batch_id), so look up or join on (entity_id, batch_id) or on entity_id alone instead.
  low_selectivity_single_column [status] medium 0.60
    As a single-column key status prunes almost nothing: its median value touches 48 of 48 row groups (100%), while (entity_id, status) touches at most 2 at p95, so index (entity_id, status) instead.
  scattered_time_column [version_ts] medium 0.60
    version_ts is out of order: a point in time falls inside 23.5 row-group ranges on average, and only 33% of consecutive row groups in a file ascend without overlapping, so range pruning on version_ts reads far more than it needs; sort or cluster each partition by version_ts when writing.

evidence e9df439b54ec · inputs footer-stats 987a24b7 fresh, fanout 02e1602b fresh, workload-summary 4cb3407d fresh, workload 0323e839 2h
```

This is the benchmark's planted layout: 3 days, 8 files per day, late rows, with its recorded workload (which supplies `entity_id`, so composites lead with it). Every fanout the benchmark measures equals its `expected.json`, composites matched by column set: the benchmark's `(status, entity_id)` is measured here as `(entity_id, status)`, and since its key columns have no nulls the order changes no number. A sampled key is marked `~`, and its row states its own sample rate and rank error.

## Tests

| File | Covers |
|---|---|
| `tests/fanout_exact.rs` | Exact fanout against a brute-force oracle that reads the Parquet files directly, on generated planted and flat layouts. The lints fire or stay silent as planted. Parallel writers without late rows are not time scatter. One day is never a batch stamp, even when every other gate passes; two days are, and then low selectivity is not raised on it; every composite a lint names is measured in that order; a supplied column leads its composites even when it comes later in the schema; a batch stamp is never a low-selectivity partner, and on the review's few-entities layout the stamp's partner is not the unique payload. Nulls follow ADR 014 per tuple. Column validation. A source that misreports row counts is an error. |
| `tests/ground_truth.rs` | On bench corpora with `expected.json` and the recorded `workload.json` the profiler runs with (a corpus without one is an error), read from `$BELLWETHER_BENCH_CORPORA` (else `benches/out/*/`), skipping with a message when there are none, and skipping a corpus above `$BELLWETHER_GT_MAX_ROWS` rows (default 1M): <ul><li>unsampled fanout equals the ground truth exactly, with the table sized from `expected.json` (composites matched by column set; the corpora's key columns have no nulls);</li><li>`batch_stamp_key` and `scattered_time_column` fire on exactly the planted column and nowhere else, exactly when planted;</li><li>`low_selectivity_single_column` fires on the planted column exactly when planted and never on a column planted as a batch stamp; on another column it may fire where the catalog criterion holds (a single-day batch column touching every row group);</li><li>sampled fanout is within its stated error, and a key that fits the table is exact.</li></ul> |
| `tests/thresholds.rs` | Every lint threshold from both sides, on in-memory layouts: overlap depth 1.5, sortedness 0.9 (and touching ranges counted as ascending), files p50 2, composite ratio 2, time concordance 0.9, the high-confidence files ratio 0.5, the sentinel factor 8, the auto-selection width of 256 bytes per row, low-selectivity fraction 0.5 and ratio 2, and both edges of the 4σ band in which footer distinct counts are used. An unsupplied row id is refused as a partner, a supplied one accepted. Where a layout can sit exactly on a threshold, one does. |
| `tests/review_probes.rs`, `tests/review_probes_r2.rs` | The PR 17 reviews' probes, kept as regression tests: the first round with the adaptations listed in its header, the second (late rows two days back are scattered, not sentinels) unchanged. |
| `tests/sampling.rs` | HyperLogLog within 4σ and quantiles inside the DKW band against an independently computed distribution. A 400-seed calibration of the stated errors. Footer distinct counts: exact only when disjoint and verified, bounds otherwise. An estimate above the row count is capped and noted. |
| `tests/memory_bound.rs` | The stated table bound on 1× and 8× data, a control without eviction that breaks it, and one read per row group of only the candidate key columns (spy source; a blob column is skipped and never read). |
| `tests/cache.rs` | A hit performs no data or footer reads, reports its age, and produces identical evidence. A rewritten file, another schema, and other options change the right keys. Changed time columns never serve a stale measure. A workload that reorders a tuple changes the fanout key. A rewritten run file is reread and refused. |
| `tests/provenance.rs` | Invariant 6: <ul><li>every input resolves;</li><li>RFC 3339 time and the oldest age, including a back-dated workload and manifest;</li><li>the manifest sequence and hash when indexed;</li><li>refusal without inputs writes nothing;</li><li>a foreign manifest is an error.</li></ul> |
| `tests/sketch_lint.rs` | `R-sketch-1`: fires on a stamp beside an id (message, partner, composite, approximate label, confidence raised by exact counts); quiet one step short of each threshold, beside a non-id partner, and alone; each threshold inclusive at its exact value; the partner with the most values named. |
| `tests/lints.rs` | `declared_but_unused_key` needs both a manifest and a workload; the confidence rubric; one sentence per message. |
| `tests/workload.rs` | Each run is verified against `workload.json`; any disagreement is refused. |
| unit tests | <ul><li>`sampler`: exact counts, sampled and unsampled, and the capacity bound;</li><li>`ranges`: overlap and sortedness;</li><li>`fanout`: tuple bytes equal `KeyColumns`, cached entries round-trip bit for bit, and supplied columns lead each tuple;</li><li>the cached fanout layout pinned to its format;</li><li>`quantile`;</li><li>`footer` cache round trip.</li></ul> The HyperLogLog moved to `bellwether_core::hll` (shared with the `sketch` sidecar) with its accuracy tests. |

The shared measures have their own unit tests in `bellwether-source` (`stats`), and the benchmark's `tests/bench.rs` recomputes them independently from row-derived ranges.

## Known limitations

- **Files on disk.** The workload reader is bench-format only and reads local files. The time measures cover dates and timestamps, not integers that encode times. `Lint` has no typed field for the recommended composite; it is named in the message and listed, in the same order, in the fanout.
- **Fields `Fanout` and `EvidenceBody::Profile` lack.** They have no field for sample size or error, skipped candidates, range statistics, or time measures. These live in `ProfileReport` and the cache, and the Evidence carries only what the contract types allow.
