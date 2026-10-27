# ADR 034: Commit-boundary durability on local stores

**Status:** accepted · **Date:** 2026-10-27

## Decision

On the local filesystem backend (`crates/bellwether-store/src/local.rs`), every write still stages its bytes in `<target>#<20 random digits>` and moves them into place (`rename`, or `hard_link` for create-if-absent) so readers never observe a partial file, while drive flushes (`File::sync_all()` and directory syncs) follow the commit boundary instead of running per content-addressed object:

1. **Manifest commits (`manifest.json`, `manifests/<seq>.json`) and detached signatures (`ledger/*.sig.*.json`).** Every conditional write (`put_if_absent`, `put_if_version`) fsyncs its staging file, moves or links it into place under the per-target lock, and fsyncs the parent directory.
2. **Segment writes (`segments/<blake3>.seg`) and the commit barrier (`StoreExt::sync_segments`).** Segments written via `put_content_addressed` (either pre-uploaded during a build or compaction, or uploaded from `StagedSegments` in step 6 of `commit`) write their staging file and atomically `rename` it into `segments/` without a per-segment `sync_all` or directory sync. Immediately after step 6 ensures every segment the new manifest adds is present and before step 7 CASes `manifest.json`, `commit` calls `StoreExt::sync_segments(root)`, which fsyncs `<root>/segments/` once (on macOS, `File::open(segments_dir)?.sync_all()` issues one `fcntl(F_FULLFSYNC)` barrier across the volume, flushing all preceding segment data and directory entries to non-volatile storage before the manifest rename becomes durable).
3. **Rebuildable cache entries (`cache/<hash>.bin`, `cache/<hash>.meta.json`).** Cache writes use atomic staging + `rename` so concurrent readers never see a torn file, and skip `sync_all` and directory syncs. `cache/` is regenerable scratch (footer statistics, recorded key sets, cached workloads) and is never a durability root; any entry lost to a power failure is recomputed on the next miss, and `Cache::get_cached` verifies `Hash::of(&payload) == meta.payload_blake3` on every read.
4. **Ledger records (`ledger/<hash>.rec`).** Ledger records written via `put_content_addressed` keep per-record staging `sync_all` + `rename` + directory sync so every `Evidence`, `Proposal`, `Replay`, `Certificate`, and `Receipt` is durable when its `Ledger::put*` call returns.

## Rejected alternatives

- **Fsyncing every file and parent directory on every `put_content_addressed`.** On macOS, `File::sync_all()` (and `File::sync_data()`) issues `fcntl(F_FULLFSYNC)`, which flushes the entire drive write cache (2–4 ms per call). A build committing $N$ segments issued $2N + 4$ full drive flushes (751 ms of 861 ms on `market-10x`, 180 segments), and a cold lookup issued 4 full drive flushes to cache its key set alongside its plan `Evidence` (`docs/measured/2026-10-27-performance.md`, H9).
- **Calling `libc::fsync` (`#ifndef F_FULLFSYNC`).** Every workspace crate sets `#![forbid(unsafe_code)]`, and `fsync` without `F_FULLFSYNC` on macOS only flushes to the drive controller's volatile cache anyway; deferring `F_FULLFSYNC` to `StoreExt::sync_segments` before the manifest CAS gives true non-volatile ordering at the commit boundary in safe Rust.
- **Skipping `sync_all` on `ledger/` records.** A receipt, certificate, or plan evidence hash returned to a caller or CI gate is an attestation root (ADR 006); losing a ledger record across a power failure after its hash was reported would break the audit chain.

## Why

A segment file in `segments/` has no meaning until a committed `manifest.json` references its content hash (ADR 003). If the machine loses power before the manifest CAS lands, any unreferenced segment is an orphan that a retry rewrites idempotently and GC reclaims. By placing a single `sync_segments` barrier after all segments of a commit are linked and before `manifest.json` is CASed, no published manifest can ever reference a segment whose bytes or directory entry had not reached non-volatile storage, while cutting the drive flushes of an $N$-segment commit from $2N + 4$ to a constant number independent of $N$.

## Consequences

- `StoreExt` gains `sync_segments(&self, root: &str) -> Result<()>` with a default no-op implementation; `ObjectStoreBackend` implements it for `BackendKind::Local` by calling `sync_dir` on `<root>/segments/` when that directory exists.
- `commit_inner` calls `store.sync_segments(root)?` after `ensure_segments_present` and before the `put_if_absent` / `put_if_version` on `manifest.json`.
- Cold lookup plans and `why` / `profile` runs that populate `cache/` perform zero `F_FULLFSYNC` calls for `cache/`, while keeping atomic staging + rename visibility and BLAKE3 verification on read.
