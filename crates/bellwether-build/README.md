# bellwether-build

Work package WP4. This crate builds and maintains an index over a folder of files and plans lookups against it. It owns ADR 012 (key-spec completeness is a manifest fact) and ADR 013 (fingerprint drift retires row-group ids, which are never reused). It depends on `bellwether-types`, `-core`, `-source` and `-store` only (`dev/check-layering.py`).

| Item | Purpose |
|---|---|
| `Builder::build(source_root, key_specs, options)` | Discover, fingerprint, assign row-group ids, write segments in parallel, and publish everything in one manifest commit. Incremental: only uncovered row groups are read. Additive: the index declares `key_specs` and keeps every spec it already declares; removal is explicit. |
| `Builder::remove_spec(spec)`, `BuildOptions::remove_unlisted` | Remove a spec explicitly (one commit, no source reads), or let a build remove the specs it does not list. |
| `Builder::compact(options)` | Merge each spec's segments with core's `merge`, in contiguous groups under a size cap; one commit drops the inputs, which the store tombstones for GC. |
| `Builder::verify()` | Re-fingerprint every tracked file under the manifest's policy and report drift `(path, old, new)`; writes nothing. |
| `completeness(&manifest, &spec)`, `Builder::completeness`, `Planner::completeness` | The sequence at which a spec became complete, re-derived from coverage (ADR 012). |
| `Planner::open(store, root)`, `Planner::plan(specs, lookup)` | A `ScanPlan` against one pinned manifest: files, row groups, exact bytes, and a residual predicate. |
| `Planner::plan_with_transient(specs, lookup, extra)` | The same, with some specs served from in-memory segments (replay's hypothetical specs). |
| `Planner::encode_keys(columns, batch)`, `Planner::encode_tuple(columns, values)` | Canonical probe tuples at the dataset types. |
| `BuildOptions`, `SourceKind`, `BuildObserver`, `CompactOptions` | Configuration and instrumentation. |

## Building

**Sources** are always constructed by the builder:

- with the dataset schema (`with_schema`), the single source of column types (`docs/format.md`);
- with the index's fingerprint policy;
- for text, with the chunk size the manifest records for each file.

The dataset schema is the one given in `BuildOptions::schema`, else the one the manifest records, else inferred from the first file of a new index. It is recorded in the manifest as Arrow `Display` strings and read back with Arrow's `FromStr`. A type that does not survive that round trip is refused before anything is written.

`FingerprintPolicy` defaults to `Tail64K`. Only `Full` detects an in-place patch outside the last 64 KiB that keeps the size and mtime (`tests/verify.rs` shows both).

**Row-group ids** (ADR 013):

- Files are paired with tracked ones by path. A file seen for the first time gets dense ids from `next_id`, in path order, one per row group in `index_in_file` order.
- A tracked file whose fingerprint changed (size, etag or mtime, content hash) has its ids retired in a `rowgroup_space.tombstones` entry and gets fresh ids. So does a file that disappears and comes back.
- Ids are never reused. Every id below `next_id` is either live (held by a tracked file) or retired, never both.

**Coverage and completeness** (ADR 012):

- A segment's manifest `rowgroup_range` is the half-open range of ids its writer processed, not the ids that produced keys. A row group whose leading key column is all null is covered (`tests/fixtures.rs`, `nulls.parquet`; `tests/nulls.rs` for every kind).
- The segment records the same range itself (`SegmentWriter::set_rowgroup_range`, `docs/format.md`). So two segments over different row groups never share bytes or an id, even when none of their row groups held a key. Coverage never depends on content identity: the manifest keys segments by id, and two key-less segments with equal bytes would collapse into one entry and lose a range.
- A spec is complete when its live segments' ranges cover every live id.
- Every build commit recomputes `key_spec_completeness` from coverage:
  - a spec that just became complete records the commit's sequence;
  - a spec that was already complete keeps its sequence;
  - an incomplete spec is not recorded, and a build that would leave a declared spec incomplete commits nothing.
- `completeness()` and the planner re-derive coverage rather than trust the map. A foreign edit that drops a segment therefore makes the spec incomplete, and planning it is an error, never a partial answer.

**What a build indexes.** Each declared spec indexes every live row group its segments do not cover:

- for an existing spec, the new files' row groups;
- for a spec seen for the first time, every row group;
- the same for a spec whose definition changed under the same name, or whose key or timestamp column changed dataset type. That spec's old segments are dropped, so encodings never mix.
- Gaps left by another writer are filled too.

**Which specs a build declares.** The ones it is given, plus every spec the manifest already declares under another name. A given spec under an existing name replaces that definition. Kept specs index new files like any other, so a caller that names only the spec it cares about never destroys the rest.

Removal is explicit, in one of two ways:

- `Builder::remove_spec(spec)` drops the declaration, the completeness entry and the segments in one commit, without reading a source file. It refuses a different spec declared under the same name.
- `Builder::reset_sketch(spec)` drops a declared sketch's segments and completeness entry, keeping its declaration, in one commit that reads no source file, so the next build sketches every live row group again. The facade calls it when `index --key sketch:…` re-declares a sketch whose segments are missing from the store or damaged; nothing plans with a sketch, so nothing else depends on them.
- `BuildOptions::remove_unlisted` makes a build remove every spec it is not given.

Dropped segments are tombstoned by the commit, for GC. A build with nothing to do reads nothing and commits nothing.

**Before assigning ids**, a build validates the saved row-group space: no id assigned twice, none both live and retired, none at or above `next_id`. A damaged manifest is refused instead of seeding ids that are already in use (`tests/damaged_manifests.rs`).

**The work** is split into items that are independent of the thread count, so the output does not depend on it (`tests/memory.rs`):

- an item is a run of consecutive live ids that need the same set of specs;
- items are cut at file boundaries where possible and at `max_segment_row_groups`.

A worker (rayon) takes one item:

- it reads each row group once, projecting only the union of the item's key and timestamp columns;
- it feeds the batch to one open `SegmentWriter::for_dataset` per spec, which records the key types in the segment header;
- it drops the batch before the next read.

A writer is finished and uploaded (`put_content_addressed`) as soon as the next row group would take it past `max_segment_keys` or `max_segment_bytes`. Its manifest entry is kept. The manifest commit uses an empty staged set, so no build holds all its segments in memory.

**The commit** (`bellwether_store::commit_with`) applies the build's change to whatever manifest is current:

- **Restart.** If a concurrent commit changed the row-group space, the schema or the source since the build planned, the closure returns a conflict and the build restarts from discovery with fresh ids. Its segments' bitmaps hold the ids it assigned, so it cannot rebase them.
- **Rebase.** Any other concurrent change, such as compaction or GC, is rebased onto.
- **Claimed segment.** If GC claimed an uploaded segment, the build also restarts, with backoff.

`tests/concurrency.rs` and `tests/restarts.rs` interleave a competing build, a winner that only retires ids, a schema change, a compaction, and a foreign segment drop at the CAS.

## Planning

**Serving rules.** A lookup over columns `C` is served by a spec over `P` when either:

1. every column of `P` is in `C`: the key is projected onto `P`, an exact lookup (exact, temporal, bloom; range is `P = [c]`); or
2. `C` is a strict prefix of `P` and the spec is exact or temporal: a prefix lookup.

**Intersection.**

- Bitmaps are intersected per key tuple across serving specs, then unioned.
- No spec means a raw scan of every live row group.
- No key means no row group and an empty `In`, never a full scan, with or without specs.
- A probe containing a null matches nothing (ADR 014). It is dropped before any lookup (point, prefix or projection) and from the residual.
  - A range with a null bound selects nothing.
  - `encode_keys` skips any row with a null in any lookup column.
- Specs of which none serves the lookup are an error.
- WP6 (replay) plans with the same rules.

**Probes** are validated element by element against the dataset types. A probe encoded under another time unit tag or decimal width, or as a string for an integer column, is an error, not a silent miss.

Raw bytes still cannot carry everything:

- a decimal's scale, so 1.50 encoded at scale 2 for a scale-4 column looks like 0.0150;
- the unit a timestamp's value was computed in, so a millisecond count under the column's microsecond tag is accepted.

Either probe silently misses. **Callers must encode probes with the typed encoders**, `Planner::encode_keys` (from a batch) or `Planner::encode_tuple` (from values). Both convert every value to its column's dataset type exactly, or refuse (`tests/id_ledger.rs`).

**Retired ids.**

- Ids in a bitmap that a tombstone retired are dropped, because their file version is gone.
- An id the manifest never assigned is an error.
- Core's merge cannot filter ids, so segments from before a rewrite keep referencing retired ids until the spec is rebuilt.
- For temporal lookups, core's cross-segment `latest`/`asof` winner can lie only in retired row groups. When it does, the winner is recomputed from the stored version chains over live row groups, so a retired version never bounds a live key (`tests/invariants.rs`).

**Plan contents.**

- **Bytes.** Bytes are exact: the Parquet footer's `RowGroupMeta.bytes`, or a text file's recorded chunk lengths. The manifest records no per-row-group bytes.
- **Drift.** Every planned file is checked against its fingerprint, so a plan over a drifted file is an error.
- **Totals.** `row_groups_total` is the number of live row groups. `manifest_version` is the sequence (ADR 020).

**Residuals.**

| Lookup | Residual |
|---|---|
| `Keys` | `In` over the lookup columns |
| `Latest` | `And[In, NotOlderThan, LatestPerKey]` |
| `AsOf` | `And[In, Between(ts ≤ asof), NotOlderThan, LatestPerKey]` |
| `Lookback` | `And[In, NotOlderThan(newest − lookback per key)]` |
| `Between` | `Between` |
| `Window` (also an `asof` lookback, the window `[asof − lookback, asof]`) | `And[In, Between(ts in window)]` |
| `Bitemporal` | `And[In, Between(valid), Between(known), NotOlderThan(valid), NotOlderThan(known), LatestPerKey(valid)]` |

Details:

- Signed zero:
  - an equality probe for float zero uses both encodings, and `In` lists both;
  - a `Between` bound of +0.0 below or -0.0 above is widened (`docs/format.md`, statistics semantics).
- `NotOlderThan` carries each key's index-wide winning timestamp from every segment of the temporal spec. A stale row in a planned row group is dropped even when the newest version lives in a row group another spec pruned (invariant 5).
- `In` for `Latest` and `AsOf` lists only keys that have a version. A key whose versions all have null timestamps has no latest (ADR 014).
- `Lookback` returns every version of each key in its own window `[newest − lookback, newest]`, `newest` being the key's newest live version, so it has no `LatestPerKey`; its residual is `In` (keys with a version) then `NotOlderThan` with each key's window start. The versions come from the one temporal spec on the key columns, through every segment's version chains over live row groups (core `temporal::chains` and `temporal::lookback`); with none, from the one complete `bounds` sidecar on them, whose per-row-group maxima over live row groups give each key's newest time and whose ranges keep the row groups reaching into its window (`Planner::lookback_column` names the column either way). The lower bound is clamped to the smallest value the timestamp column holds. The plan is intersected per key with any other serving spec. A window ending at a fixed instant (`asof --lookback`) is a `Lookup::Window`. `tests/invariants.rs` checks lookbacks against decoded rows over random layouts, and `tests/windows.rs` both sources against decoded files.

**Time series (WP21).**

- **Sidecars.** `BuildOptions::sidecars` declares `bellwether_core::sidecar` definitions beside the key specs: `bounds` (per key, per row group, the min and max of a time column over the key's rows there; `exact:symbol@ts` on the command line) and `temporal2` (per key, the `(valid, known, row group)` triples). They are recorded in `schema.sidecars`, their segments carry `segments[].kind`, and they are built, covered, and recorded complete exactly as key specs are (`sidecar_completeness`). Compaction does not merge sidecar segments. A segment or declaration of a kind this binary does not know is kept unchanged by builds and never planned with (`docs/format.md`, "Segment kinds").
- **`Lookup::Window`.** Keys in `[lo, hi]` of a time column (its dataset unit): the key specs serving the key columns, intersected with the range specs on the time column, intersected per key with every complete `bounds` sidecar on that column, which keeps a row group only when that key's own time range there overlaps the window. An incomplete sidecar is not used; the plan is then a superset, never short (`tests/bounds.rs`, against decoded files, sentinels and nulls included).
- **`Lookup::Bitemporal`.** Through the one complete `temporal2` sidecar serving the key columns: per key, the greatest valid time at or before `valid` among versions known at or before `known` (either unbounded), then the greatest known time; the plan holds exactly the live row groups of each winner, ties kept (`tests/bitemporal.rs`). Retired row groups never win.
- **Order.** `Planner::plan_ordered` plans the same row groups and orders entries by a timestamp range key's row-group minimum, returning `PlanOrder::Ordered` only when that order holds for every planned row group (`tests/ordered.rs`).

**Sketches (B2): built here, never planned with.**

- `BuildOptions::sketches` declares `bellwether_types::SketchSpec`s (`sketch:a,b,a+b[,p=14][,minhash=K]` on the command line): per item, per file, the HyperLogLog registers and optional MinHash signature of `bellwether_core::sketch`. Each is recorded in `schema.sidecars[]` with kind `sketch` (items in `columns`, no time column, `p` and `minhash` in `params`), its segments carry `segments[].kind = "sketch"`, and it is built, covered, and recorded complete by the sidecar rules (`sketch_completeness`; `declared_sketches` lists them). A malformed sketch declaration is kept unchanged like an unknown kind and never used.
- **No planner path reads a sketch.** `declared_sidecars`, the only sidecars the planner sees, never yields one, and the planner modules never name the kind. `tests/sketch.rs` proves it: every lookup plans byte-identically with and without a sketch, and still does after every sketch segment is deleted from the store; a source tripwire fails if a planner file ever mentions the sketch kind.
- Memory: a sketch writer grows by one entry per file stretch, each at most `sketch::max_entry_bytes` (`2^p` registers and `k` slots per item); that bound is charged against `max_segment_bytes` when an entry starts, so a segment closes before the cap is crossed.

## Memory (ADR 018)

- **Build.**
  - Each worker holds one row group's key columns and one open segment per spec it builds.
  - A segment is capped by `max_segment_keys` and `max_segment_bytes`. These are counted as sums over its row groups of distinct key tuples (temporal: distinct `(key, ts)` pairs; bloom: 8 bytes per key; range: one entry), which bound the writer's contents from above. A row group larger than the cap gets a segment of its own.
  - Bound: `threads × (one row group's key columns + specs × max(cap, one row group))`.
  - `tests/memory.rs` holds weak references to every column a worker is handed and checks that at most one batch per worker is alive at any read, and exactly one with one worker. It also checks that only key columns are read, and that every segment respects the cap.
- **Plan.** A plan holds:
  - the block indexes of the keyed segments it probes (ranged reads through the snapshot);
  - the blocks it touches;
  - one whole bloom or range segment at a time (they are scanned per probe);
  - the bitmaps and version hits for the keys asked.
- **Compaction.** Holds a group's inputs and its output, so about twice `max_output_bytes`.
- **Resident bytes per key (contract v3, entry 32).** The caps count accounted key bytes, not resident memory. A release build measured an open segment writer over `UInt64` keys at **320 resident bytes per key**, against 9 bytes accounted: 35.5 times. Over 9-byte keys, the layout map measured 84 resident bytes per key, against 73 accounted: 1.15 times. For narrow keys, then, the resident memory of `specs × cap` is about 36 times the cap. The figures come from `bellwether-replay`'s `tests/resident.rs` and are listed in [the measured document](../../docs/measured/2026-10-26-performance.md). Charging that overhead to the caps is a WP22b candidate.

## Tests

Ground truth never goes through the code under test:

- Parquet row groups are decoded with the `parquet` crate's reader;
- text chunks are recomputed from the file bytes with the chunking rule restated in `tests/common`;
- residuals are evaluated by a reference evaluator over canonical bytes.

| File | Covers |
|---|---|
| `tests/fixtures.rs` | Every source fixture: manifest contents, inference, exact/composite/prefix/bloom/range/raw plans with exact bytes, an all-null row group, NaN, UInt32 ≥ 2³¹, Decimal(38,4), latest/asof, CSV and JSON lines chunks persisted, and each file read at its recorded chunk size after the option changes |
| `tests/invariants.rs` | Invariant 1 and 5 property test over random layouts, caps, item sizes and thread counts; the pruned-newest-version scenario, with the bound shown to be load-bearing; a retired version never bounding live rows |
| `tests/ids.rs` | Never-reused ids across rewrite, removal and re-add; mtime drift |
| `tests/completeness.rs` | A spec added later: every row group, recorded at its commit; incomplete spec queries are errors; gap filling; failed builds commit nothing; same-name redefinition; explicit removal by `remove_unlisted` and `remove_spec` |
| `tests/concurrency.rs` | A competing build at the CAS forces a restart with fresh ids; a concurrent compaction is rebased onto |
| `tests/memory.rs` | Peak batches alive per worker, key-only projection, segment caps, thread-count independence |
| `tests/incremental.rs` | Source calls linear in new files (counted); no-op builds; root, kind, policy and chunk-size refusals |
| `tests/types.rs` | Signed zero, key-type change rebuild with no mixed encodings, mixed file scales and units, unrepresentable values |
| `tests/compact.rs` | Plans identical after compaction; inputs tombstoned; no merge across a coverage gap |
| `tests/verify.rs` | Changed and missing files; `tail64k` vs `full` |
| `tests/nulls.rs` | All-null row groups covered for every kind; null probes match nothing; key-less segments over different row groups never collide, across items, caps and builds; empty key lists plan nothing; `encode_keys` skips a null in any column |
| `tests/restarts.rs` | Restart when the winner changes or only retires row-group ids, and when it changes the schema; refusal when a rebase leaves a declared spec incomplete |
| `tests/damaged_manifests.rs` | A lowered `next_id` is refused before ids are assigned; a segment dropped after a rewrite is caught by re-deriving coverage by subset |
| `tests/id_ledger.rs` | Every id maps to one (fingerprint, index) across rewrites, vanishings, reappearances and renames; a build listing one spec keeps the others; typed probe paths rescale or refuse |
| `tests/invariant1_by_value.rs` | Invariant 1 against an oracle comparing decoded values (SQL equality) over two decimal scales, two time units, nulls, signed zero, NaN, and CSV/JSONL chunks |
| `tests/invariant5_retired.rs` | Latest and asof give the live answer when an old segment still holds a retired newest version, including ties, signed-zero variants, and asof with a pruned winner |
| `tests/ledger.rs` | A ledger shaped like `bellwether-bench`'s; `real_bench_corpus` (ignored) runs the same checks over a corpus the bench crate wrote: `BELLWETHER_BENCH_CORPUS=<dir> cargo test -p bellwether-build --test ledger -- --ignored` |
| `tests/sketch.rs` | Stored sketches equal sketches of the decoded files (the `parquet` crate's reader fed to core's sketcher), complete like a sidecar; new and changed files re-sketched, retired entries unused; a sketch on a missing column refused with nothing committed; no planner path reads a sketch (plans equal with, without, and after deleting every sketch segment; the planner sources never name the kind) |

## Benchmark

`cargo bench -p bellwether-build --bench build` reports build throughput in row groups per second at 16, 32 and 64 row groups:

- with one worker, where the rate is constant if the build is linear in files;
- with the default pool.

The bench crate cannot depend on this one (layering), so the benchmark lives here. `tests/incremental.rs` asserts the linear work exactly, by counting source calls.

## Known limits

- Retired row-group ids stay inside segments written before the rewrite, because core's merge does not filter them. The planner drops them, and they cost space until the spec is rebuilt.
- `KeySpec::name` omits `ts_column` and `fpr`, and the manifest keys segments and completeness by name. So two specs that differ only there cannot be declared together; the build refuses.
- The manifest has no field for a CSV dialect. A non-default dialect is carried in `source.kind` as `csv;delimiter=<byte>;header=<bool>`, so a change is detected.
- Sources read local files only (`LocalFs`), like `bellwether-source`. The index itself can live on any `bellwether-store` backend.
