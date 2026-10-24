# Paper figure data

Every results panel in the paper (Figures 5 to 7) is a pgfplots template over
one CSV file in this directory. `bellwether report --paper` (WP13; planned)
writes the files from ledger records, as `docs/evolve.md` §11 specifies. The
same command renders the HTML charts. Each panel switches on its own file:

```tex
\IfFileExists{data/<name>.csv}{<plot>}{<framed placeholder>}
```

When the file is absent, the panel is drawn as a framed placeholder that names
the file. When the file is present, the panel is drawn from it, and only from
it. Plot and placeholder occupy the same fixed box, so adding or deleting a file
never changes the paper's pagination. Each panel's code is in
`paper/figures/plot-<panel>.tex`, and the shared style is in
`paper/figures/plotstyle.tex`.

**This directory holds no data until a harness run exists.** A file here is a
result. Never add a hand-made, synthetic, or edited file, and never recompute a
value for display. The templates were compile-tested against synthetic files
in a scratch copy of `paper/` outside the repository, at the caps below. No such
file is, or will be, committed.

## Rules for every file

- **Format.** UTF-8, comma-separated, one header row with exactly the column
  names below, and LF line ends. Columns may appear in any order. Values are
  not quoted, so no value may contain a comma or a double quote.
- **Provenance.** The file may begin with comment lines starting with `#`.
  `report --paper` writes at least these: the commit hash, the manifest
  sequence, the workload hash, the corpus rung or rungs, the machine (CPU,
  cores, memory, storage, and warm or cold caches), and the hashes of the
  ledger records the file was rendered from. `#` may not appear anywhere else,
  because pgfplots treats it as a comment wherever it occurs.
- **Numbers.** Plain decimal or scientific notation (`1234`, `0.001`, `1e-4`).
  Counts and bytes are integers. `nan` is allowed only where a schema says so.
- **Logarithmic axes.** A value drawn on a logarithmic axis must be at least 1
  for bytes and row groups, and greater than 0 for milliseconds and knob
  values. `report --paper` refuses to write a file that breaks this rule, and
  reports why, rather than writing a value the plot cannot draw. The panel
  then stays a placeholder.
- **Empty results.** Every file has at least one data row. An empty result,
  such as no promoted rule, is reported in the text, not as an empty file.
- **Labels.** A `label` value has a length cap, given per file, and may not
  contain `,` `#` `%` `\` `{` `}` or `"`. Other characters, including
  `_ & $ ^ ~`, are printed literally. Everything a panel draws is also clipped
  to its box, so a label over the cap is cut off; it cannot run into a
  neighboring panel.
- **Grids and matrices.** `landscape.csv`, `attribution.csv`, and
  `interaction.csv` are written as scanlines in the order stated, with one
  empty line after each scanline.
- **Strategy keys.** The lookup strategies are `raw` (raw scan), `stats`
  (Parquet statistics), `duckdb`, `datafusion`, and `index` (Bellwether). Each
  key has one color, marker, and dash pattern in every figure. The plot ignores
  a row with any other key.
- **Corpus.** Rungs are those of `docs/benchmark.md` (amended): small
  (10 files), medium (120 files), and large (960 files). A file covers only
  rungs that have been run. Where a figure needs one rung, it takes the
  largest rung run so far, and the file says which. Text that shows a result
  names its rung and machine.
- **Bytes.** Realized bytes for every strategy are the compressed bytes counted
  by the harness's own byte-counting reader, so an engine's internal reads are
  counted the same way as the index's (`docs/benchmark.md`, amended).

## Run statistics

Several files summarize a candidate's cost over a set of runs. Its *p50-run
bytes*, *p95-run bytes*, and *max-run bytes* are the 50th and 95th percentiles
(nearest rank) and the maximum of its per-run bytes over that set. Unless a
file says otherwise, the set is the search runs.

## Files

### `funnel.csv`: Figure 5a, pruning funnel of one run (Q1)

One row per lookup strategy, for a single recorded run on the largest rung run
so far. The run is the search run whose key-set size is the workload's p95,
taking the lowest run id among ties. The provenance comment names the run and
its key count.

| Column | Type | Meaning |
|---|---|---|
| `strategy` | key | lookup strategy |
| `files` | integer | files in the rung, the same in every row; the axis label prints it |
| `rg_total` | integer ≥ 1 | live row groups in the corpus |
| `rg_stats` | integer ≥ 1 | row groups left after footer min/max statistics and embedded bloom filters |
| `rg_index` | integer ≥ 1 | row groups left after the sidecar index; equal to `rg_stats` for a strategy without one |

`rg_total ≥ rg_stats ≥ rg_index`. Drawn as grouped bars on a logarithmic axis,
one group per strategy, in a fixed order that does not depend on row order.

### `latency.csv`: Figure 5b, latency versus corpus size (Q2)

One row per strategy and rung, sorted by `files` ascending. The plot connects
each strategy's points in file order.

| Column | Type | Meaning |
|---|---|---|
| `strategy` | key | lookup strategy |
| `files` | integer | files in the rung: 10, 120, or 960 |
| `p50_ms` | > 0 | median over the recorded runs of per-lookup wall time, planning included, in ms |
| `p95_ms` | ≥ `p50_ms` | 95th percentile of the same |
| `plan_p50_ms` | ≥ 0 | median of the planning phase alone, in ms |
| `plan_p95_ms` | ≥ 0 | 95th percentile of the planning phase alone, in ms |

The `p50_ms` line is drawn with a band and a whisker up to `p95_ms`, on log-log
axes. The x axis always spans the three rungs, so a rung that has not been run
is visibly absent, and the whisker keeps p95 visible even with one rung. The
`plan_*` columns are not drawn. They record the split between planning and
execution that `docs/benchmark.md` (amended) requires for engine baselines,
whose planning may itself scan key columns, next to the totals they split. The
provenance comment states whether caches were warm or cold.

### `realized.csv`: Figure 5c, realized versus predicted bytes (Q3)

One row per executed plan.

| Column | Type | Meaning |
|---|---|---|
| `strategy` | `index`, `evolve`, or `estimate` | see below |
| `run` | `[A-Za-z0-9-]+` | run or candidate id, for traceability; not drawn |
| `predicted_bytes` | integer ≥ 1 | bytes predicted before execution |
| `realized_bytes` | integer ≥ 1 | bytes counted by the harness's reader when the plan was executed |

The three strategies are:

- `index`: Bellwether lookups for every recorded run on the largest rung run
  so far, predicted by replay.
- `evolve`: every member of the final evolve frontier on every held-out run,
  predicted by replay.
- `estimate`: the estimate-based advisor's chosen design on every held-out
  run, predicted by its engine's cost estimate.

The plot uses one log range for both axes, from half the least value in the two
byte columns to twice the greatest. Its corner-to-corner line is therefore
y = x. By design, replay's points lie on that line, and a point off it is a
defect to report, not noise.

### `convergence.csv`: Figure 6a, convergence of evolve (Q5)

One row per generation, `generation` = 0 to G in ascending order. Generation 0
is the baseline alone.

| Column | Type | Meaning |
|---|---|---|
| `generation` | integer | generation |
| `best_p50`, `best_p95`, `best_max` | integer ≥ 1 | p50-run, p95-run, and max-run bytes of the best candidate so far |
| `median_p50`, `median_p95`, `median_max` | integer ≥ 1, or `nan` | the same for the generation's median candidate |

- **Best candidate so far.** Among the gated candidates evaluated in
  generations ≤ g, the baseline included, the one with the least p95-run
  bytes. Ties go to the least max-run bytes, then the least p50-run bytes, then
  the earliest candidate.
- **Median candidate.** Sort the candidates evaluated in generation g that
  passed the gate by p95-run bytes, and take element ⌊(n−1)/2⌋. At generation
  0 it is the baseline. In a generation where no candidate passed the gate, the
  three median columns are `nan`, and the median line breaks there.

Each candidate is drawn as a line at its p95-run bytes, with a band from its
p50-run to its max-run bytes, on a logarithmic axis.

### `frontier.csv`: Figure 6b, Pareto frontier evolution (Q8)

Rows are grouped by snapshot.

| Column | Type | Meaning |
|---|---|---|
| `snapshot` | 0 to 4 | 0 is the baseline (one row, generation 0); 1 to 4 are the frontier at generations ⌈G/4⌉, ⌈G/2⌉, ⌈3G/4⌉, and G, the last generation |
| `generation` | integer | the snapshot's generation; the legend prints it |
| `p50_bytes` | integer ≥ 1 | the member's p50-run bytes |
| `p95_bytes` | integer ≥ 1 | the member's p95-run bytes |

Each snapshot lists every member of the frontier over runs × objectives at that
generation (paper §6.6), sorted by `p50_bytes` ascending, because the plot
connects a snapshot's points in file order. The plot is a projection of a
higher-dimensional frontier, so a member can look dominated in it. Later
snapshots are drawn more opaque, and the baseline is a star.

### `landscape.csv` and `descent.csv`: Figures 7a and 7b, loss landscape with the descent path

`landscape.csv` is a grid of exact replay cost over two numeric knobs. All
other knobs and the structure are held at the parent from which the descent
started.

| Column | Type | Meaning |
|---|---|---|
| `fpr` | 0 < `fpr` < 1 | bloom false-positive rate |
| `gate` | ≥ 1 | key-count gate, in keys |
| `bytes` | integer ≥ 0 | the scalar the descent minimized at this point, by exact replay; by default, p95-run bytes |

The grid is written as scanlines. Each `fpr` value in ascending order is one
scanline; within it, every `gate` value appears in ascending order, the same
values in every scanline. One empty line follows each scanline. The grid is at
most 40 × 40 points; `contour filled` runs inside TeX, and the templates were
tested at that cap. Both axes are logarithmic, so log-spaced knob values give
an even grid.

`descent.csv` holds the iterates the finite-difference descent accepted, in
order.

| Column | Type | Meaning |
|---|---|---|
| `step` | integer | 0 for the starting point (a hollow marker), then 1, 2, … |
| `fpr`, `gate` | as above | the iterate's knob values; it need not lie on the grid |
| `bytes` | integer ≥ 0 | the exact replay cost at the iterate |

Panel 7a draws the grid as a surface in a three-quarter view (`view={35}{30}`),
and panel 7b draws the same grid as a filled contour map. Each panel overlays
the path when `descent.csv` exists. The figure is possible only because the
loss is exact and cheap enough to evaluate at every grid point.

### `attribution.csv`: Figure 7c, counterfactual attribution

The attribution of the baseline a₀ on the search runs: Δ_v = c(a₀) − c(a₀ ⊖ v),
per paper Equation 4.

| Column | Type | Meaning |
|---|---|---|
| `run` | integer, 0 to n−1 | column index; runs ordered by template, then by key-set size ascending, then by run id |
| `op` | integer, 0 to k−1 | row index of an attributed node, in topological order of the plan, sources first |
| `label` | at most 10 characters | the node's label, identical in every scanline; the row labels are taken from the first scanline |
| `delta_bytes` | signed integer | Δ_v on that run, in bytes |

The attributed nodes are the top k by cost (ADR 024), with k ≤ 10. If the
search used a larger k, the file keeps the 10 costliest nodes. Each run is one
scanline, in ascending order; within it, every `op` appears in ascending order.
One empty line follows each scanline. The colormap is ColorBrewer RdBu, from
−M to +M, where M is the largest |Δ_v| in the file. Zero is therefore the white
midpoint, red marks a node that reads more than its alternative would, and
blue marks one that reads less.

### `interaction.csv`: Figure 7d, pairwise interaction matrix

The interactions I_uv = c(a_uv) − c(a_u) − c(a_v) + c(a), per paper Equation 5,
among the nodes of `attribution.csv`, in the same order, summed over the search
runs.

| Column | Type | Meaning |
|---|---|---|
| `u`, `v` | integer, 0 to k−1 | row and column; the same indices as `op` in `attribution.csv` |
| `label` | at most 10 characters | the label of node `v`, for traceability; not drawn, because the rows align with panel 7c |
| `i_bytes` | signed integer, or `nan` | I_uv in bytes; `nan` exactly on the diagonal u = v, which is drawn gray |

Each `u` is one scanline, in ascending order; within it, every `v` appears in
ascending order. One empty line follows each scanline. The matrix is symmetric,
and its color scale is centered on zero, as for attribution. Blue marks a pair
whose joint change saves more than its two changes do separately.

### `transfer.csv`: Figure 6c, rulebook transfer (Q7)

One row per promoted rule, at most 10 (the ten with the largest held-out gain),
in descending order of `gain_pct`.

| Column | Type | Meaning |
|---|---|---|
| `rank` | integer, 1 to R | the rule's rank |
| `label` | at most 14 characters | a short name for the rule; the provenance comment maps each label to its `Rule` record hash |
| `gain_pct` | number | reduction in total bytes over the held-out templates when the rule alone is applied to the baseline, as a percentage of the baseline's held-out bytes |
| `lo_pct`, `hi_pct` | numbers | the lowest and highest of the same reduction computed per held-out template; drawn as error bars, the rule's confidence (ADR 025) |
| `cumulative_pct` | number | the same reduction when rules 1 to `rank` are applied together, by exact replay, not a sum |

Drawn as horizontal bars with error bars, plus a line for `cumulative_pct`, on
one axis in percent.
