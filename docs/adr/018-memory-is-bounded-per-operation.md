# ADR 018: Memory is bounded per operation

**Status:** accepted · **Date:** 2026-10-23

## Decision

Memory is bounded per operation.

## Rejected alternatives

Collect-to-driver patterns that materialize a full filename set in one process.

## Why

Build streams one row group per worker. Lookup holds one block index plus the bitmaps for the keys asked. Replay holds one run's key set. The bound is stated and benchmarked.

## Amendment 2026-10-27: the bound is physical memory, and it holds per batch (WP22b)

A bound holds on the operating system's books, not only on a meter's. WP22a measured `reconcile` at up to 4.5 times its stated bound on 100,000-row row groups while its meter stayed within the budget: each row group was decoded whole, and the allocator kept the freed row-group-sized buffers resident. So:

- **Reconcile decodes a row group in batches** of at most `READ_BATCH_ROWS` (8,192) rows (`Source::read_column_batches`), so the memory outside the budget is one batch, whatever the width of the sources' row groups. Parquet, text, and SQL sources stream; a source without a partial decode hands over one row group. A test measures the process's peak physical memory (`/usr/bin/time`), never only the meter.
- **Approximate counts only schedule.** Reconcile's first pass may count distinct keys in a HyperLogLog to group later passes; its registers are charged to the budget, and it never decides which rows are kept, enters a result, or is reported. A pass it undersizes overflows the meter and the run restarts.
- **Shared caches have one cap per process.** Segment byte windows read by lookups (64 KiB each) are kept up to 32 MiB in total across every open snapshot, so concurrent queries do not multiply the cache.
