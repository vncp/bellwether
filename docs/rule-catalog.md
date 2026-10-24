# Rule catalog

One entry per lint the profiler raises. A lint is a `Lint` record inside an
`Evidence` of kind `profile` (see [`agent-loop.md`](agent-loop.md)). This
catalog is the skeleton WP5 implements against; entries change by pull
request, and a new rule id is never reused for a different pattern.

Status (2026-10-24): the five statistics rules (`batch_stamp_key`,
`low_selectivity_single_column`, `declared_but_unused_key`,
`scattered_time_column`, `no_discriminating_column`) are implemented in
`bellwether-profile` (WP5), with
the detectors and thresholds stated in their entries below. Two decisions
are defined once in `bellwether_source::stats` and called by both the
profiler and the benchmark's ground truth: `overlap_depth` decides
`scattered_time_column` from footers alone, so a lint and its planted flag
agree by construction. `batch_stamp` decides `batch_stamp_key`, and
`partner_order` chooses the composite partner both composite rules are
judged against, from the same inputs on both sides. Wherever the recorded
workload supplies `entity_id`, as every benchmark workload does, the
profiler's only candidate is `entity_id`, the benchmark's only candidate,
and the two agree by construction; without such a workload the profiler
may judge against another partner. The three plan
rules describe intended behavior until an `EvidenceBody` carries a consumer
plan.

## How to read an entry

Every entry uses the same fields.

- **id**: the stable string in `Lint.rule`.
- **Detector**: the pattern on statistics or on a plan that raises it.
- **Estimator**: which fanout measurement sizes the problem, and how that
  maps to bytes. Fanout gives a ranking signal before any replay. The bytes
  a finding reports come only from exact replay of a recorded workload
  (`Replay`, `RunCost.bytes`, ADR 005). A byte figure shown before replay
  is labeled as an estimate.
- **Guidance template**: what the finding tells an agent. File, step, before
  shape, after shape, invariant. The last three map to `RewriteShape`
  (`before`, `after`, `invariant`). File and step have no typed field today;
  they go in `Lint.message` as text.
- **Verification method**: one of four.
  - *Static algebraic identity*: the rewrite is correct by a known identity
    on the plan, independent of data.
  - *Differential run on available data*: run before and after on local or
    synthetic data and compare.
  - *Reconcile over touched keys*: compare old and new output over the keys
    a recorded workload touched (ADR 004). Produces a `Certificate` with
    `Coverage` of kind `workload`.
  - *Prod shadow gate*: run the rewrite beside the current job on real
    inputs and compare before switching.
- **Confidence rubric**: how sure the finding is, built only from signals
  the records already carry.

### What reaches a receipt

A `Receipt` requires a passing `Certificate` (invariant 2), and only
reconciliation writes a `Certificate`. Reconcile over touched keys always
can. A differential run can, when it is run through `reconcile` and recorded
with `Coverage` of kind `full` or `sampled`. A static algebraic identity and
a prod shadow gate have no `Certificate` path in the current types. A rule
whose only method is one of those two can produce a `Proposal` and a
`Replay`, but not a `Receipt`, until the contract adds a way to record them.

### Shared confidence signals

| Signal | Where it lives | Raises confidence | Lowers confidence |
|---|---|---|---|
| Sampling | `Fanout.sampled` | `false` | `true` |
| Freshness | `Provenance.oldest_input_age` | small relative to the data's update cadence | older than the last build |
| Workload | `EvidenceBody::Profile.workload_keys` | present | absent (the lint is from stats alone) |
| Coverage | `Certificate.coverage` | `full`, then `workload` | `sampled` with a small fraction |

A rule's rubric says which levels apply. The levels are **high**, **medium**,
and **low**, recorded in `Lint.confidence` as 0.9, 0.6, and 0.3.

### Inputs outside the current contract

Four rules read only statistics, which `EvidenceBody::Profile` carries.
Three rules (`unprefiltered_window`, `lookback_unbounded`,
`wide_projection_before_filter`) need a description of the consumer's query
plan. No `EvidenceBody` variant carries a plan of a consumer query today;
those rules are specified here so the shape is agreed before the type is
added.

## Rules

### batch_stamp_key

- **id**: `batch_stamp_key`
- **Detector**: a column that looks like a key but is ordered with time: it
  behaves like a batch timestamp, not an entity id. As implemented
  (2026-10-24, revised the same day: one shared decision,
  `bellwether_source::stats::batch_stamp`), all three must hold against a
  composite of the column with a key column `K`; names are not consulted:
  1. one value touches at least 2 files at the median (nearest-rank p50 of
     files per value);
  2. the column's median row groups per value is at least 2 times the
     composite's;
  3. it concords with a date or timestamp column across days: cross-day
     concordance at least 0.9 over at least two distinct UTC days
     (`bellwether_source::stats::time_correlation`). Each row group is placed
     on the UTC day of its time-column maximum (late rows lower a row
     group's minimum, never its maximum). Over every pair of row groups on
     different days, concordance is the fraction whose column minima are
     strictly in day order, ties counting as out of order. With one day it
     is undefined and the lint stays silent. An id sorted within each daily
     file restarts every day, and a range-partitioned id repeats every day:
     both score below one half.

  The partner is chosen by `bellwether_source::stats::partner_order`, the
  same rule the benchmark uses:
  1. A row id, a column whose exact distinct count reaches the row count, is
     refused unless the workload supplies it. A sampled count never makes a
     row id.
  2. If the workload supplies any candidate, only supplied candidates count.
  3. A candidate that repeats ranks before a near-unique one (at least 90 %
     of rows distinct), then the smaller composite p95, then schema order.

  The lint fires on the first candidate for which the decision holds. The
  benchmark's only candidate is `entity_id`, which its workload supplies.
  Wherever a recorded workload supplies `entity_id`, both judge against
  `(entity_id, batch_id)` and agree by construction. Without one, the
  profiler may fire against another partner.
- **Estimator**: `Fanout.files_per_value` and `row_groups_per_value` at p50,
  p95, max for the column, compared with the same measurement for the true
  key candidate. Bytes: replay the workload under the current spec and under
  `Exact` on the candidate; report `RunCost.bytes` for both.
- **Guidance template**: file `<path of the consumer>`, step `<join or
  filter on the column>`. Before: `join on <batch column>`. After: `join on
  <true key>` or `join on (<true key>, <batch column>)`. Invariant: `the set
  of output rows per <true key> is unchanged`.
- **Verification method**: reconcile over touched keys.
- **Confidence rubric**: high when fanout is unsampled, a workload is
  present, and the composite's p95 files per value is at most 0.5 of the
  batch column's. Medium without a workload. Low when sampled.

### low_selectivity_single_column

- **id**: `low_selectivity_single_column`
- **Detector**: a single-column key spec, declared or proposed, whose fanout
  is a large fraction of the dataset: one value touches most files or row
  groups. A composite of this column with another reduces fanout sharply
  (from candidate composite enumeration). As implemented (2026-10-24): the
  column's median value touches at least 50 % of all row groups, and some
  id-like column makes the composite's p95 row groups at most half of that
  median. It is never raised on a column `batch_stamp_key` fires on, and a
  column `batch_stamp_key` fires on is never the partner: a batch stamp is
  not a key at all, so no composite of it is recommended as one. The
  partner is chosen by the same `partner_order` rule as for
  `batch_stamp_key` (a row id is refused unless the workload supplies it),
  and the benchmark applies it to its `(status, entity_id)` fix.
- **Estimator**: `row_groups_per_value` p95 divided by `row_groups_total`
  for the single column and for the best candidate composite. Bytes: replay
  under `Exact{[col]}` and `Exact{[col, other]}`.
- **Guidance template**: file `<index config or consumer>`, step `<the
  lookup>`. Before: `key = [<col>]`. After: the composite of `<col>` and
  `<other>`, its columns ordered with the ones the recorded workload supplies
  first (the order a lookup can seek by prefix) and the rest in schema order;
  without a workload the finding states that order's assumption. Invariant:
  `every row group holding a queried key is still selected` (invariant 1).
- **Verification method**: reconcile over touched keys.
- **Confidence rubric**: high with an unsampled measurement and a workload
  whose queries supply both columns. Medium when the workload supplies one
  of them, which then leads the composite so prefix seek works, or when
  there is no workload. Low if the workload supplies neither: no recorded
  lookup could seek the composite by its leading column. Low when sampled.

### declared_but_unused_key

- **id**: `declared_but_unused_key`
- **Detector**: a key spec in the manifest that no recorded run used. The
  workload's key sets never filter on its leading column.
- **Estimator**: no read savings. The cost is index bytes (segment `bytes`
  in the manifest) and build time. Replay shows identical `RunCost.bytes`
  with and without the spec.
- **Guidance template**: file `bellwether.toml`, step `key_specs`. Before:
  `<spec name>` declared. After: removed. Invariant: `no recorded run's
  ScanPlan changes`.
- **Verification method**: differential run on available data: replay with
  and without the spec and compare plans. No reconcile is needed because no
  output changes; see [What reaches a receipt](#what-reaches-a-receipt).
- **Confidence rubric**: high only with a workload that covers the full
  usage window. Without a workload this lint is not raised.

### scattered_time_column

- **id**: `scattered_time_column`
- **Detector**: a timestamp column used as a range or temporal filter whose
  per-row-group min and max ranges overlap heavily because rows were written
  out of time order, so `Range` pruning selects far more row groups than
  hold the range. As implemented (2026-10-24), for a date or timestamp
  column, by `bellwether_source::stats::overlap_depth`: overlap depth at
  least 1.5 and sortedness below 0.9.
  - *Overlap depth* is the time-weighted mean number of row-group ranges
    `[min, max]` containing a point, over the union of those ranges: the sum
    of the ranges' lengths divided by the length of their union, lengths in
    the column's unit over closed ranges. A *sentinel* range (an
    open-record placeholder such as 9999-12-31) is excluded and counted: one
    longer than 8 times the median range that also reaches outside the span
    of the ranges that are not that long. At most 4 are excluded; when more
    qualify, none is, since that many outliers are the data's own shape. Rows
    written late stay inside the data's span, so their ranges always count.
    Sorted data has depth 1 however sparse; `k` parallel writers over the
    same slice of time give `k`.
  - *Sortedness* is the fraction of consecutive row groups within one file
    whose ranges ascend without overlapping (the later minimum at or after
    the earlier maximum). Late rows pull a row group's minimum back below
    its predecessor's maximum; parallel writers do not. Every pair counts,
    sentinels included. With no such pair it is 1.0.
- **Estimator**: `row_groups_per_value` for time buckets, and the fraction
  of row groups a typical range selects. Bytes: replay under the current
  layout and under a `Layout{cluster_by: [<ts>]}` proposal. The current
  layout's bytes are exact. The proposed layout's bytes are not: they depend
  on row-group statistics a re-cluster would produce, so they are an
  estimate. `RunCost` is documented as exact and has no field to label an
  estimate, so this number has no legal home in a `Replay` today. Until the
  contract adds one, the finding reports fanout only and the exact bytes
  come from a replay after the re-cluster.
- **Guidance template**: file `<writer job>`, step `<the write>`. Before:
  `written in arrival order`. After: `sorted or clustered by <ts>` within
  each partition. Invariant: `the multiset of rows is unchanged`.
- **Verification method**: reconcile over touched keys, comparing the
  dataset before and after re-clustering.
- **Confidence rubric**: high with a workload of range queries on this
  column. Medium from statistics alone.

### no_discriminating_column

Added 2026-10-24 after the first real-data run, where a time-ordered taxi
dataset produced a fanout table in which every candidate touched 20 to 22
of 22 row groups, and no rule said so (defect 8 of
[`measured/2026-10-24-realdata.md`](measured/2026-10-24-realdata.md)).

- **id**: `no_discriminating_column`
- **Detector**: from statistics alone. The dataset has at least 2 row
  groups, and the measured key with the lowest median `row_groups_per_value`
  (ties: lower p95, then fewer columns), over every single column and
  composite the profile measured, still touches at least 0.75 of
  `row_groups_total` (`NO_DISCRIMINATING_MIN_FRACTION`). The lint names that
  lowest-fanout key. At this threshold `low_selectivity_single_column`
  cannot fire on the same profile: its composite's p95 is at most half of
  the single column's median, so at most 0.5.
- **Estimator**: the lowest median fanout over `row_groups_total`. Bytes:
  replay of any exact key reads at least that share of the row groups, so
  the saving an index can offer is bounded by one minus it. With a workload,
  `why` states the same verdict as its culprit (`layout`), from the replayed
  candidates themselves: every candidate, the floor included, reads at least
  0.75 of the row groups the runs span (`LAYOUT_BOUND_FRACTION`).
- **Guidance template**: file `<writer job>`, step `<the write>`. Before:
  `written in arrival (often time) order`. After: `clustered by the column
  lookups supply`, or no index at all. Invariant: `the multiset of rows is
  unchanged`.
- **Verification method**: reconcile over touched keys, comparing the
  dataset before and after re-clustering.
- **Confidence rubric**: high when every fanout it rests on is exact; low
  when any is sampled.

### unprefiltered_window

- **id**: `unprefiltered_window`
- **Detector**: on the consumer's plan, a window function partitioned by key
  `K` is computed before a semi-join that filters to a key set on the same
  `K`. The window runs over every key and the filter discards most of them.
- **Estimator**: fanout of `K` and the workload's key-set size
  (`workload_keys`). Bytes: replay the key set as a lookup on `K` and
  compare with a full scan of the window's input.
- **Guidance template**: file `<consumer>`, step `<window step>`. Before:
  `window(partition by K) then semi_join(keys on K)`. After:
  `semi_join(keys on K) then window(partition by K)`. Invariant: `a window
  partitioned by K only reads rows of its own K, so filtering on K first
  leaves every kept partition intact`.
- **Verification method**: static algebraic identity. See
  [What reaches a receipt](#what-reaches-a-receipt).
- **Confidence rubric**: high when the plan shows the partition key and the
  semi-join key are the same column with no intervening transformation.
  Low if either side passes through a non-injective expression.

### lookback_unbounded

- **id**: `lookback_unbounded`
- **Detector**: on the consumer's plan, a latest-per-key or as-of step reads
  the full history of the source, with no lower time bound, while the
  temporal index could locate each key's current version directly.
- **Estimator**: fanout of the key over all row groups, against the
  row groups a `Temporal` spec would select for `latest`. Bytes: replay
  under `Temporal{[key], ts}`.
- **Guidance template**: file `<consumer>`, step `<dedupe step>`. Before:
  `scan all history, rank by <ts> per key, keep first`. After: `latest(keys)
  via temporal index, then apply LatestPerKey residual`. Invariant: `the
  newest version per key is unchanged` (invariant 5).
- **Verification method**: reconcile over touched keys.
- **Confidence rubric**: high when null timestamps are absent or handled per
  ADR 014. Medium if the column has nulls; they rank last and never win.

### wide_projection_before_filter

- **id**: `wide_projection_before_filter`
- **Detector**: on the consumer's plan, a step selects all or most columns
  before the filter that discards most rows, so pruning by row group is
  available but the read is not limited to the needed columns.
- **Estimator**: not a fanout rule. The row groups read are unchanged; the
  saving is in column chunks. Bytes come from per-column sizes in the row
  group statistics, summed over the row groups the workload selects.
- **Guidance template**: file `<consumer>`, step `<projection>`. Before:
  `select * then filter`. After: `filter then select <needed columns>`.
  Invariant: `projection commutes with a filter that only references
  projected columns`.
- **Verification method**: static algebraic identity. See
  [What reaches a receipt](#what-reaches-a-receipt).
- **Confidence rubric**: high when the filter's columns are known from the
  plan. Low when the step is opaque code.

Note: `RowGroupMeta.bytes` is a single total per row group. The estimator
for this rule needs per-column chunk sizes, which `ColumnStats` does not
carry today.
