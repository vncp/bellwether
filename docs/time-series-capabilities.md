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
| Verify a derived dataset | Do my bars, factors, or vendor files agree with the source by key, newest version, row hash, and exact decimal totals? | `bellwether reconcile ./a ./b --key symbol,ts --total volume` | merged |
| Receipts | A hash-linked proof that an index or rewrite returned the same rows, for review or CI | `bellwether fix`, `receipt show`, `report --html` | merged |
| Exact what-if pricing | The cost of a hypothetical key before building it, from statistics, with zero estimation error | part of `why` | merged |
| Partition values from paths | `day=2024-03-01/` directories prune without being read | automatic | building (WP21) |
| Ordered plans | Row groups returned in time order for per-symbol sequence math | automatic with a timestamp range key | building (WP21) |
| Pinned snapshots | Run any query against an earlier index state, for reproducible backtests over a growing lake | `--at-manifest N` | building (WP21) |
| Per-key time bounds | Symbol × window pruning that skips row groups where the symbol exists but outside the window | `--key exact:symbol@ts` | building (WP21) |
| Bitemporal as-of | What was known at time K about the state at time V: restatements, corrections, late prints, without lookahead | `--key temporal2:symbol@valid,known`, `asof --valid V --known K` | building (WP21) |
| Market benchmark corpus | A synthetic symbols × days corpus with restatements and late prints, so every claim above has a measured funnel | `bellwether bench generate --market` | building (WP21) |
| Exact counts per key | How many rows these symbols have, per row group, without reading; exact join cardinality between two indexed datasets | storage version 2 | planned (WP14) |
| Relational cost model | Exact cost of joins, windows, projections, and layouts over indexed data, so pipeline rewrites can be priced and proven | replay over the operator table | planned (WP15) |
| Pipeline optimizer | Search over plans, keys, layouts, and multi-step pipelines with the exact cost as the loss and reconciliation as the gate | `bellwether evolve` | planned (WP13) |
| Engine adapters | Plans executed inside DataFusion and DuckDB with no sidecar step | planned (WP16) |
| Measured performance | Lookup p50 and p95, build throughput and its core-scaling curve, reconcile rows per second, in release builds | planned (WP22) |
| Multi-machine build | N workers writing segments, one committer, for lakes a single machine cannot index in time | planned (WP22) |
| SQL table as a side | Reconcile a warehouse table against files, with measured costs | planned (WP20) |
| Agent interface | The same engine as MCP tools with job handles, typed refusals, dry runs, budgets | building (WP12) |

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

## How an agent should drive it

Every command takes `--json` and returns a typed result with embedded record hashes. Use `why` to choose keys, `index` once per layout change, `locate`/`latest`/`asof` for reads, `reconcile` after every derivation, and quote receipt hashes rather than restating results. Refusals carry a reason and a next step; do not retry a refusal unchanged.
