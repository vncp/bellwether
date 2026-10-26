# Time-series capabilities, for a strategy agent

What Bellwether can do over a folder of market-data files, what each capability makes cheap, and its status. Written 2026-10-25 for an agent that formulates strategies and needs to know which questions are now cheap to ask. Status is one of **merged** (on `main`, reviewed), **building** (in a reviewed pull request this week), **planned** (in the work-package plan with a decision record). Every cost figure the engine reports is an exact sum of file statistics unless it is labeled measured; nothing here promises a speed that has not been measured.

## The data shape this assumes

Files (Parquet, CSV, or JSON lines) partitioned by date, each file holding rows for many symbols, each row carrying a symbol, a timestamp, and measures. Nothing is converted or loaded; the index lives beside the files.

## Capabilities

| Capability | The question it makes cheap | How to call it | Status |
|---|---|---|---|
| Diagnose the layout | Which column is a real key here, and exactly how many bytes would each candidate index save on my recorded queries? | `bellwether why ./data --from keys.parquet` | merged |
| Symbol-set lookup | Which files and row groups hold these 300 symbols, with exact bytes, without scanning the lake | `bellwether index ./data --key exact:symbol` then `locate --keys` | merged |
| Composite keys | Lookups on (symbol, date) or (symbol, venue) with prefix seek on the leading column | `--key exact:symbol,date` | merged |
| Bloom keys | Cheap membership for very large key sets, with a stated false-positive rate | `--key bloom:symbol@0.01` | merged |
| Range keys | Row groups whose timestamp (or price, or volume) range overlaps a window | `--key range:ts` | merged |
| Latest per symbol | The newest row per symbol across the whole history, located to the row group | `bellwether latest ./data --keys` with `temporal:symbol@ts` | merged |
| As-of per symbol | The row current at time T per symbol: the point-in-time read a backtest needs to avoid lookahead | `bellwether asof ./data --keys --at T` | merged |
| Price latest and as-of | How many bytes a `temporal` key would save for newest-version or as-of reads of these symbols, before building it, next to the point-lookup cost | `bellwether why ./data --from keys.parquet --mode latest\|asof --latest-by ts [--at T]` | merged |
| Price lookback windows | How many bytes a `temporal` key would save for every version of these symbols in the last N (as of T, or before each symbol's newest row); calendar units s, m, h, d, w, not trading days | `bellwether why ./data --from keys.parquet --mode asof --latest-by ts --at T --lookback 252d` | merged |
| Verify a derived dataset | Do my bars, factors, or vendor files agree with the source by key, newest version, row hash, and exact decimal totals? | `bellwether reconcile ./a ./b --key symbol,ts --totals volume` | merged |
| Exact totals over float columns | Do the Float64 close and volume columns sum the same on both sides, exactly, at a stated precision? Each value is rounded half to even at the scale, then summed as a 128-bit integer; a NaN names its key | `bellwether reconcile ./a ./b --key symbol,ts --totals volume:dec(4)` | merged |
| Memory budget in human units | Fewer passes over a 25M-row lake by giving the comparison more room | `reconcile --memory-budget 8G` | merged |
| Receipts | A hash-linked proof that an index or rewrite returned the same rows, for review or CI | `bellwether fix`, `receipt show`, `report --html` | merged |
| Exact what-if pricing | The cost of a hypothetical key before building it, from statistics, with zero estimation error | part of `why` | merged |
| Partition values from paths | `day=2024-03-01/` directories prune without being read; the `day` column is typed from the dataset schema or inferred over every value (Int64, Date32, else Utf8) | `index --partitioning hive` once; then automatic, e.g. `locate --ts day --from 2026-01-05 --to 2026-01-05` | merged (WP21) |
| Ordered plans | Row groups returned in time order for per-symbol sequence math; the result names `ordered_by` only when reading the plan in order really is time order, and otherwise says why not | automatic with a timestamp range key (`--key range:ts`) | merged (WP21) |
| Pinned snapshots | Run any query against an earlier index state, for reproducible backtests over a growing lake; returns the earlier plan byte for byte | `locate`/`latest`/`asof`/`why --at-manifest N`, and `at_manifest` on MCP `observe.locate`/`observe.asof`; a sequence garbage collection pruned is refused with the retained range (`not_retained` over MCP) | merged (WP21) |
| Per-key time bounds | Symbol × window pruning that skips row groups where the symbol exists but outside the window | `--key exact:symbol@ts`, then `locate --keys K --from T1 --to T2` | merged (WP21) |
| Bitemporal as-of | What was known at time K about the state at time V: restatements, corrections, late prints, without lookahead | `--key temporal2:symbol@valid,known`, `asof --valid V --known K`, `latest --known K` | merged (WP21) |
| Columns from file names | A lake of one file per session (`grouped_daily_us/2026-03-02.parquet`) gets `day` as a column with zero rewrites: typed placeholders `{day:date}`, `{symbol:str}`, `{n:int}` (or untyped, inferred); min = max = the path value, never read from a file. Recorded in the manifest's `source.partitioning`, so a changed template is refused; a file that does not match is an error naming up to 10 such files. Then `temporal:symbol@day` and `asof --at 2026-03-01` work on such lakes | `--path-template "grouped_daily_us/{day:date}.parquet"` on `index` (recorded), and on `why`, `locate`, `latest`, `asof`, `reconcile` of an unindexed folder | merged (WP21, strategy request 1) |
| Lookback windows | Every row of a symbol set in `[T − N, T]`: the trailing history a feature needs, not just the one row current at T. Pruned per key by `exact:symbol@ts` bounds, a temporal key's version chains, a range key on the time column, and path values, intersected (each one used when declared). Residual `In(keys)` plus `Between(ts, T − N, T)`. Without T, each symbol gets its own window `[newest − N, newest]` ending at its own newest row, through a temporal key's version chains or, without one, `exact:symbol@ts` bounds (residual `In(keys)` plus each symbol's `NotOlderThan`). Units s, m, h, d (24 hours of UTC), w (7 days): calendar units, and a length must be a whole number of the time column's unit (`36h` on a date column is refused, not rounded); months, years, and trading-day calendars are out of scope (`252d` is 252 calendar days) | `asof --at T --lookback 252d [--ts ts]`, `latest --lookback 252d` (each symbol's own window), `locate --window A..B`; priced by `why --window A..B` (locate in a window) or `why --latest-by ts --lookback N [--at T]` | merged (WP21, strategy request 2) |
| Market benchmark corpus | A synthetic symbols × days corpus with restatements and late prints, so every claim above has a measured funnel | `bellwether bench market`, then `scripts/market-funnels.py` | merged (WP21) |
| Exact counts per key | How many rows these symbols have, per row group, without reading; exact join cardinality between two indexed datasets | storage version 2 | planned (WP14) |
| Relational cost model | Exact cost of joins, windows, projections, and layouts over indexed data, so pipeline rewrites can be priced and proven | replay over the operator table | planned (WP15) |
| Pipeline optimizer | Search over plans, keys, layouts, and multi-step pipelines with the exact cost as the loss and reconciliation as the gate | `bellwether evolve` | planned (WP13) |
| Engine adapters | Plans executed inside DataFusion and DuckDB with no sidecar step | planned (WP16) |
| Measured performance | Lookup p50 and p95, build throughput and its core-scaling curve, reconcile rows per second, in release builds | measured (WP22a, [2026-10-26](measured/2026-10-26-performance.md)); wins planned (WP22b) |
| Multi-machine build | N workers writing segments, one committer, for lakes a single machine cannot index in time | planned (WP22) |
| SQL table as a side | Reconcile a warehouse table against files, with measured costs | planned (WP20) |
| Agent interface | The same engine as MCP tools with job handles, typed refusals, dry runs, budgets | building (WP12) |
| Is this column a key (sketches) | How many distinct symbols, timestamps, or `(symbol, ts)` pairs a vendor file set has, rows per value, and which files repeat a `(symbol, ts)`, approximately and without reading the files; never used to plan | `bellwether index ./data --key sketch:symbol,ts,symbol+ts`, then `bellwether sketch cardinality ./data` (or `--transient` before building); `why --sketches-only` | building (B2) |

## Query patterns that become viable

- **Cross-sectional pulls on a date-partitioned lake.** Any universe of symbols over any window, read from only the row groups that hold them, instead of a scan. This is the pattern most strategy research is built on, and it is usually the one that takes minutes.
- **Event studies.** For a set of event timestamps per symbol, `asof` gives the state at each event and a window lookup gives the aftermath, located to the row group.
- **Point-in-time features without lookahead.** Features computed from `asof` reads carry no future information by construction, and once bitemporal keys land, restated fundamentals and corrected prints are handled by the two clocks rather than by hand.
- **Reproducible backtests.** Pinning the index snapshot means a backtest run next month over a larger lake reads exactly what it read today.
- **Data-quality gates.** Reconcile vendor data against your own derivation after every ingest, with exact totals per symbol, and keep the certificate.
- **Exact data sizing.** Once counts land, "how much data does this universe have" is a lookup, not a scan.
- **Interactive iteration.** Lookups are index probes, so the question-per-hour rate on a laptop goes up, which is where strategy ideas actually come from.

## Limits to design around

- The index skips; it does not compute. Derived signals (gaps, streaks, moving averages) are a scan over the pruned row groups or a materialized dataset. The optimizer, when built, can propose that materialization.
- Costs are exact for bytes over files. Latency is measured, and only in release builds that are still to come.
- One machine today. Indexing terabytes is a scheduling problem until the multi-machine build lands; pilot on a slice first.
- The on-disk format freezes at M5; an index built before that may need one rebuild.
- Nested columns can be present but not used as keys; flatten first.
- Lookbacks count calendar time. "The last 252 trading sessions" needs the session calendar outside Bellwether: pass the calendar date of the 252nd session back as the window start (`--window START..T`).
- A lookback is a whole number of the time column's unit: `36h` on a date column is refused rather than rounded to a day.

## How an agent should drive it

Every command takes `--json` and returns a typed result with embedded record hashes. Use `why` to choose keys, `index` once per layout change, `locate`/`latest`/`asof` for reads, `reconcile` after every derivation, and quote receipt hashes rather than restating results. Refusals carry a reason and a next step; do not retry a refusal unchanged.
