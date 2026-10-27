# ADR 026: The cost model prices the relational subset exactly from statistics and counted indexes

**Status:** accepted · **Date:** 2026-10-24 · **Amended:** 2026-10-27

## Decision

Replay prices the following from statistics and the index alone, without reading data: projection (per-column chunk bytes from footers); equality and `IN` filters (bitmaps); range filters (min/max bounds, exact under a range key); temporal `latest` and `asof` (temporal chains); a semi-join of a key set against an indexed dataset; inner and left joins between two datasets indexed on the join key (row groups touched on each side from the intersection of the two indexes, and output cardinality when both sides carry counts); window partition prefilters (row groups holding the partition keys of interest); and the inputs of aggregations. Every `RunCost` states its exactness as one of `exact`, `bounded`, or `measured`; a bound is a pair, never a point.

## Rejected alternatives

Histogram and sample-based cardinality estimation; a learned cost model as the arbiter (it may propose, ADR 022).

## Why

Parquet footers carry per-column chunk sizes, so projection cost is a sum, not a guess. An exact segment enumerates the keys present in each row group, so a join between two indexed sides is a set intersection over two indexes, and with per-key counts its output cardinality is a sum of products. Those are the quantities an optimizer estimates worst, and here they are computed. What cannot be computed from statistics, CPU time and shuffle bytes beyond their bounds, is bounded by exact quantities or measured, and labeled as such.

## Consequences

Storage version 2 adds optional per-key, per-row-group counts to exact segments (`counts` on the key spec) and per-column bytes to scan entries. The format freeze (M5) waits for them. Contract v3 adds `Predicate` variants for joins and window partitions and the tri-state exactness. Lookups remain the first thing priced; nothing in this decision changes the Phase 2 crates.

## Amendment 2026-10-27 (WP15)

Relational replay landed as a typed plan DAG (`ConsumerPlan`, never SQL) priced node by node; the README's exactness table ("Pricing a whole query") and `docs/evolve.md` section 14 list every operator's quantity and its tri-state. Refinements made in building it:

- **Exact only where the counted relation is proved.** Per-key counts give exact rows only for a scan's rows selected by whole keys on exactly the counted columns; after a range filter, a `latest`/`asof`, or a selection on other columns they bound the rows from above. A join is exact only when both sides are proved and counted.
- **Narrowing is sound.** A node above a `latest`, `asof`, or `window` drops row groups only by whole keys on its key (or partition) columns, so it never promotes a stale version or cuts a partition short.
- **Shared nodes.** A plan may share nodes: each is evaluated once per run, each scan read once with the union of its consumers' row groups, and each consumer prices its rows from its own selection. Buffering a shared output is memory, not priced.
- **Exchange bytes** are bounded with respect to a defined quantity, rows × average compressed row width of the planned row groups (compressed-equivalent bytes), and labelled as not network or memory bytes.
- **Cut** (ADR 032, amended 2026-10-27): layout pricing (a `Layout` proposal stays refused, `layout_not_applicable`) and plan-node counterfactuals over `ConsumerPlan`; the key-spec counterfactuals stay.
