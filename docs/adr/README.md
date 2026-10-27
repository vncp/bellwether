# Architecture decision records

Short, dated, and final until superseded by a later ADR. Each states what was chosen, what was rejected, and why.

- [ADR 001](001-roaring-bitmaps-over-a-global-row-group-id-space.md) — Roaring bitmaps over a global row-group id space
- [ADR 002](002-memcomparable-composite-key-encoding.md) — Memcomparable composite key encoding
- [ADR 003](003-immutable-segments-a-manifest-and-conditional-put-commits.md) — Immutable segments, a manifest, and conditional-PUT commits
- [ADR 004](004-reconciliation-is-the-verifier.md) — Reconciliation is the verifier
- [ADR 005](005-exact-replay-is-the-cost-model.md) — Exact replay is the cost model
- [ADR 006](006-content-addressed-append-only-evidence-ledger.md) — Content-addressed, append-only evidence ledger
- [ADR 007](007-parquet-native.md) — Parquet-native; no new file format
- [ADR 008](008-split-block-bloom-filters-xxhash64-over-canonical-typed-byte.md) — Split-block bloom filters, xxhash64, over canonical typed bytes
- [ADR 009](009-the-agent-proposes.md) — The agent proposes; only the engine scores, verifies, and applies
- [ADR 010](010-cli-and-mcp-share-one-code-path.md) — CLI and MCP share one code path
- [ADR 011](011-result-contract.md) — Result contract: a ScanPlan is a superset with a residual predicate
- [ADR 012](012-adding-a-key-spec-builds-new-segments-over-all-row-groups-an.md) — Adding a key spec builds new segments over all row groups and records completeness in the manifest
- [ADR 013](013-source-file-changes.md) — Source file changes: fingerprint drift tombstones old row-group ids, ids are never reused, segments are retained before GC
- [ADR 014](014-nulls-are-not-indexed.md) — Nulls are not indexed; null timestamps rank last and never win latest
- [ADR 015](015-writer-conflicts-resolve-by-rebase.md) — Writer conflicts resolve by rebase
- [ADR 016](016-one-canonical-byte-encoding-per-arrow-type-for-ordering-bloo.md) — One canonical byte encoding per Arrow type for ordering, bloom hashing, and reconcile row hashing
- [ADR 017](017-forward-compatibility.md) — Forward compatibility: unknown fields are preserved on rewrite; a newer format version is refused with a typed error; the engine never downgrades in place
- [ADR 018](018-memory-is-bounded-per-operation.md) — Memory is bounded per operation
- [ADR 019](019-benchmark-design-is-fixed-before-implementation.md) — Benchmark design is fixed before implementation
- [ADR 020](020-manifest-sequence-number.md) — The manifest carries a monotonic sequence number
- [ADR 021](021-reflective-workload-optimization.md) — Reflective workload optimization with an exact loss and an equivalence gate (amended 2026-10-25)
- [ADR 022](022-what-gradient-means-here.md) — What "gradient descent" means for evolve (amended 2026-10-25)
- [ADR 023](023-joint-and-whole-pipeline-optimization.md) — Optimize sets and pipelines, not calls (amended 2026-10-25)
- [ADR 024](024-counterfactual-attribution.md) — Attribution by counterfactual replay is the backward pass (amended 2026-10-25)
- [ADR 025](025-rulebook-validation.md) — A rule is promoted only by held-out replay (amended 2026-10-25)
- [ADR 026](026-the-cost-model-prices-the-relational-subset-exactly.md) — The cost model prices the relational subset exactly from statistics and counted indexes
- [ADR 027](027-sql-tables-are-sources-with-measured-costs.md) — SQL tables are sources with measured costs and attest-only apply
- [ADR 028](028-storage-version-2-and-record-version-3-are-the-frozen-format.md) — Storage version 2 and record version 3 are the frozen format
- [ADR 029](029-signatures-are-detached-objects-beside-the-record.md) — Signatures are detached objects beside the record
- [ADR 030](030-multi-machine-build-workers-write-segments-one-committer-commits-once.md) — Multi-machine build: workers write segments, one committer commits once
- [ADR 031](031-scoped-proposers-and-a-frozen-outer-loop.md) — Scoped proposers and a frozen outer loop
- [ADR 032](032-staged-delivery-with-a-measured-trigger.md) — Staged delivery with a measured trigger
- [ADR 033](033-query-engines-are-measured-sources.md) — Query engines are measured sources
