# `bellwether-evolve`

Workload-level physical design advisor for Bellwether (`bellwether advise`, MCP `score.advise`, WP13a / Milestone M6).

## Governing decisions

- **ADR 032 (amended 2026-10-27)**: Reduced stage 1 (`advise`) ships deterministic physical design search without a language model: rule-driven key enumeration, key subsumption and multi-key set search, exact numeric knob descent over bloom false-positive rates and key-count gates, preserve-and-extend anchor admission with regression allowance $\delta$, Pareto frontier selection with crowding distance and knee-point recommendation, and receipts for every admitted change.
- **ADR 021**: Preserve-and-extend admission with anchor runs (`p95` and `max` key-set runs per template) and per-run regression allowance $\delta$ (`delta`, default `0.05`).
- **ADR 022**: Exact numeric descent on knobs (`bloom:col@fpr`, key-count gates) using exact replay (`bellwether-replay`, ADR 005).
- **ADR 023**: Set-valued physical design `D = {spec_1, ..., spec_k}` with subsumption and maintenance cost `m(D)` across multiple workloads and runs.

## Modules

- `enumerate`: Rule-driven enumeration (`docs/evolve.md` §4.4) from `ProfileReport` and recorded workloads (`exact`, `exact,counts`, `exact@ts`, composites ordered by selectivity, `temporal`, `temporal2`, `tombstone`, `bloom`, `range`, `list`, `text`, `agg`, `expr`, and `ConsumerPlan` candidates).
- `subsumption`: Models key subsumption (`docs/evolve.md` §5.2)—`exact:a,b` prefix seek over `exact:a`, `counts` over uncounted, `bounds` over unbounded, `temporal:a@ts` over `exact:a` and `exact:a@ts`, `tombstone` over untombstoned, leading timestamp over `range:ts`—so no proposed set contains a dominated or redundant spec.
- `knobs`: Exact numeric knob descent (`docs/evolve.md` §4.3) over `BLOOM_FPR_GRID = [0.0001, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1]` with `bloom_bytes_estimate(rows, fpr) = ceil(rows * (-ln(fpr) / (ln 2)^2) / 8)` and key-count gate threshold descent (`broadcast` vs `in` vs `shuffle`).
- `maintenance`: Maintenance cost `m(D)` (realized transient/committed segment bytes, analytical byte estimates, and build row-group/row scan costs).
- `anchors`: Anchor set selection (`select_anchors`) and preserve-and-extend admission (`admit`) recording exact discard reasons (`no_gain`, `anchor_regression`, `subsumed`, `over_index_byte_cap`).
- `frontier`: Multi-objective Pareto dominance over `(total_bytes_read, p95_run_bytes_read, max_run_bytes_read, spec_count, estimated_index_bytes)`, NSGA-II crowding distance, and normalized utopia knee-point selection.
- `search`: End-to-end deterministic search (`search_configurations`) combining singletons and greedy forward selection + local swap up to `max_keys`.
