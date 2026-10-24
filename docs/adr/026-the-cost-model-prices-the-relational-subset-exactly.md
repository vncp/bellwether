# ADR 026: The cost model prices the relational subset exactly from statistics and counted indexes

**Status:** accepted · **Date:** 2026-10-24

## Decision

Replay prices the following from statistics and the index alone, without reading data: projection (per-column chunk bytes from footers); equality and `IN` filters (bitmaps); range filters (min/max bounds, exact under a range key); temporal `latest` and `asof` (temporal chains); a semi-join of a key set against an indexed dataset; inner and left joins between two datasets indexed on the join key (row groups touched on each side from the intersection of the two indexes, and output cardinality when both sides carry counts); window partition prefilters (row groups holding the partition keys of interest); and the inputs of aggregations. Every `RunCost` states its exactness as one of `exact`, `bounded`, or `measured`; a bound is a pair, never a point. Layout proposals become exact under a stated layout model once per-key counts exist.

## Rejected alternatives

Histogram and sample-based cardinality estimation; a learned cost model as the arbiter (it may propose, ADR 022).

## Why

Parquet footers carry per-column chunk sizes, so projection cost is a sum, not a guess. An exact segment enumerates the keys present in each row group, so a join between two indexed sides is a set intersection over two indexes, and with per-key counts its output cardinality is a sum of products. Those are the quantities an optimizer estimates worst, and here they are computed. What cannot be computed from statistics, CPU time and shuffle bytes beyond their bounds, is bounded by exact quantities or measured, and labeled as such.

## Consequences

Storage version 2 adds optional per-key, per-row-group counts to exact segments (`counts` on the key spec) and per-column bytes to scan entries. The format freeze (M5) waits for them. Contract v3 adds `Predicate` variants for joins and window partitions and the tri-state exactness. Lookups remain the first thing priced; nothing in this decision changes the Phase 2 crates.
