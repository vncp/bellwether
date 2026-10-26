# bellwether-store

WP3. This crate implements the `bellwether_types::Store` contract over `object_store` 0.11, and builds the following on top of it:

- the manifest
- its lock-free commit protocol
- snapshot-isolated readers
- a content-addressed cache
- garbage collection, by claim-then-delete

It owns invariant 4 ("readers see one manifest version") and implements ADRs 003, 013, 015, 017, 018 and 020.

```text
<root>/manifest.json            committed by conditional PUT
<root>/manifest.json.lock#0     local backend only: flock target, never deleted, never read, hidden from listings
<root>/manifests/<20-digit>.json immutable committed manifest history (byte-identical to manifest.json at that sequence)
<root>/segments/<blake3>.seg    immutable, content-addressed
<root>/cache/<hash>.bin         rebuildable payload
<root>/cache/<hash>.meta.json   sidecar: created_at, payload_blake3

## Backends

`ObjectStoreBackend::from_url(url)` picks the backend from the URL scheme. Credentials come from the environment. Use `from_url_with_options` to pass `object_store` config keys explicitly.

| URL | Backend |
|---|---|
| `file:///dir`, or a bare path | local filesystem (directory created if missing) |
| `s3://bucket/prefix`, `s3a://` | S3 or S3-compatible |
| `az://container/prefix`, `abfss://container@acct.dfs.core.windows.net/prefix`, `adl://`, `azure://`, `abfs://` | Azure Blob / ADLS Gen2 |
| `gs://bucket/prefix` | GCS |
| `memory://` | in-process (for tests) |

Plain `http(s)` stores are refused because they have no conditional PUT.

The trait is synchronous. Each backend owns a two-worker tokio runtime and blocks on it. When called from inside another tokio runtime, the future runs on a scoped helper thread, so the call never panics with "runtime within runtime". Dropping the last clone inside a runtime shuts the private runtime down in the background.

### Conditional-write semantics, as verified in the `object_store` 0.11.2 source

| Backend | `put_if_absent` | `put_if_version` | `StoreVersion` token |
|---|---|---|---|
| Local | **Implemented in this crate** (`src/local.rs`): under the path lock, a fsynced staging file is `hard_link`ed to the target, which fails atomically if the target exists. | **Implemented in this crate.** `object_store`'s `LocalFileSystem` returns `NotImplemented` for `PutMode::Update`. See below. | blake3 of the content |
| S3 | `PutMode::Create` sends `If-None-Match: *` | `PutMode::Update` sends `If-Match: <etag>`. `object_store` maps S3's `404` to `Precondition` and retries `409`. | etag |
| Azure / ADLS Gen2 | `PutMode::Create` sends `If-None-Match: *` | `PutMode::Update` sends `If-Match: <etag>` | etag |
| GCS | `PutMode::Create` sends `x-goog-if-generation-match: 0` | `PutMode::Update` sends `x-goog-if-generation-match: <generation>`. It reads `UpdateVersion.version`, not the etag. | etag and generation, joined by `\x1f` |
| Memory | `PutMode::Create` | `PutMode::Update` on etag | etag |

These `object_store` errors map to `Error::VersionConflict`:

- `AlreadyExists`, from `Create`
- `Precondition`, from `Update`
- `NotFound`, from `Update`

`found` is filled in by a best-effort `head`.

**S3 conditional puts.** `object_store` 0.14 defaults S3 to `S3ConditionalPut::ETagMatch` (0.11 left conditional puts off, so every conditional put returned `NotImplemented`). This crate still sets `ETagMatch` explicitly unless `aws_conditional_put` is already configured, so it does not depend on the default. The only other accepted value is `disabled`, under which every commit after the first fails with `NotImplemented`. The `dynamo:<table>` lock no longer exists upstream, and a store configured with it is refused when it is opened. AWS S3 has supported `If-None-Match` since 2024-08 and `If-Match` since 2024-11.

**S3 needs `s3:ListBucket`.** Without `s3:ListBucket` on the bucket, S3 answers a GET of a missing key with `403 AccessDenied` instead of `404`. The first commit reads `manifest.json` to see whether it exists, so on a fresh index it fails with `Error::Store` (permission denied) instead of creating the manifest. Grant `s3:ListBucket` alongside `s3:GetObject`, `s3:PutObject` and `s3:DeleteObject`. GC's listing needs it anyway.

**Local writes.** `object_store`'s local backend neither implements `PutMode::Update` nor fsyncs. The local backend therefore does its own writes:

- **Durability.** Every write (segment, cache entry, manifest) goes to a staging file `<target>#<20 random digits>`. The staging file is fsynced and moved into place (`rename`, or `hard_link` for create-if-absent), then the parent directory is fsynced. A crash leaves either the old file or the new one. Directories created on first write (a new index root, `segments/`, `cache/`) are fsynced together with the nearest pre-existing ancestor, so their entries survive a crash too.
- **CAS.** Conditional writes take an exclusive kernel `flock` (via `fs4`) on `<target>.lock#0`. The lock is taken with `try_lock` until a 30 s deadline (`LOCAL_LOCK_TIMEOUT`) and otherwise fails with `Error::Store`; it never blocks unboundedly. With the lock held, `put_if_version` compares the blake3 of the current file with the expected token before writing.
- **Cleanup.** After a successful conditional write, leftover `<target>#<digits>` staging files from crashed writers are removed. This is safe because every conditional write to that target holds the lock. The lock file's single-digit suffix never matches.
- **Why this is not a stale lock.** ADRs 003 and 015 reject "file locks with stale healing". This lock has nothing to heal: the kernel releases it when the handle drops or the process dies, and the lock file's existence means nothing. Readers never take the lock.
- **Limits.** The guarantee is only as strong as `flock`: correct on local disks, **not** on NFS or other network filesystems.

`object_store`'s local listing hides file names of the form `name#<digits>` (`is_valid_file_path` in its `local.rs`). Staging and lock files use that form, so they never show up in `list`, which a test checks. The flip side is that a staging file left by a crashed *unconditional* write (a segment or cache entry) is invisible to GC too, and is not cleaned up automatically.

**Not-found.** `Store::get` on an absent path returns `Error::NotFound(hash)`. If the file stem is 64 hex characters, `hash` is that stem; otherwise it is the blake3 of the path. `StoreExt::get_opt` returns `None` instead.

**Listing.** `list` matches whole path segments, the way `object_store` does: `list("a/b")` returns `a/b/c` but not `a/bc`.

## Manifest

`Manifest` follows `docs/format.md` exactly:

- `storage_version`
- `sequence` (ADR 020)
- `created_at`: first creation, preserved across commits
- `committed_at`
- `commit_nonce`
- `source`
- `schema{columns, key_specs, sidecars?[{kind, columns, time_columns}]}`
- `rowgroup_space{next_id, files[{fingerprint, rowgroup_ids, rowgroup_rows?, rowgroup_bytes?, column_bytes?, chunk_size?, chunks?}], tombstones[{since, rowgroup_ids}]}`
- `key_spec_completeness{identity -> sequence}`
- `segments[{id, kind?, key_spec, rowgroup_range{start, end}, bytes}]` (`key_spec` is the spec's `KeySpec::identity()`, or a sidecar's name)
- `segment_tombstones[{id, since}]`
- `deleting[{id, claimed_at, token, lease_ms}]`

`rowgroup_space.tombstones` holds only retired row-group ids (ADR 013). Unreferenced segments go in `segment_tombstones`.

**Storage version 2 (ADR 028).** `FileEntry::{rowgroup_rows, rowgroup_bytes, column_bytes}` are the row-group statistics a plan reads instead of footers: `rowgroup_stats(i)` gives row group `i`'s rows and bytes, `column_bytes_of(i, column, columns)` one column chunk (row-group-major), and `has_rowgroup_stats()` whether they are recorded. `Manifest::from_slice` refuses a file whose arrays do not align with its row groups or the schema's columns. A version 1 manifest is read with its `segments[].key_spec` and `key_spec_completeness` keys rewritten from each spec's name to its identity, so every caller sees one shape; its `storage_version` stays 1 until a commit stamps 2. The loader's allow-lists are per version: `counts` on a key spec is refused in a version 1 manifest. `tests/manifest_v2.rs` pins the minimal version 2 manifest of `docs/format.md` byte for byte, the misalignment refusals, and an every-byte sweep.

**Forward compatibility (ADR 017).** There are two rules, depending on who owns the type:

- **Manifest-owned structs** have a `#[serde(flatten)] extra` map, so a newer binary's fields survive a rewrite. These are every struct in `manifest.rs`: `Manifest`, `SourceInfo`, `SchemaInfo`, `SidecarDecl`, `ColumnDef`, `RowGroupSpace`, `FileEntry`, `ChunkRange`, `RowGroupTombstone`, `SegmentRef`, `RowGroupRange`, `SegmentTombstone` and `DeleteClaim`. A fixture test checks the round trip with unknown keys at every one of these levels.
- **Sidecar declarations and segment kinds.** Sidecars are declared in `schema.sidecars` (`SidecarDecl { kind, columns, time_columns, params }`, named `{kind}:{columns}@{time_columns}`, or `{kind}:{columns}` with no `@` when there is no time column, as for `sketch:a,b`) and their segments set `SegmentRef.kind` (`"bounds"`, `"temporal2"`, `"sketch"`, or a future kind; absent means an ordinary key-spec segment) and `SegmentRef.key_spec` to the sidecar's name. `params` is an optional string map of the sidecar's parameters (`sketch`: `p` and `minhash`), omitted from the JSON when empty, so existing declarations serialize byte for byte as before; a reader that predates it keeps it as an unknown field (ADR 017). `schema.sidecars[]` also holds structures the planner never reads: `sketch` is approximate and only `cardinality`, `overlap`, and `why` read it. `Manifest::key_spec_segments(name)` yields only segments with `kind == None` and `key_spec == name`; `Manifest::sidecar_segments(kind, name)` yields sidecar segments of that kind and name. A manifest listing a segment or sidecar declaration whose `kind` is not in `KNOWN_SEGMENT_KINDS` (`bounds`, `temporal2`, `sketch`) still opens: `Manifest::unknown_segment_kinds()` returns the sorted distinct unknown kinds, `key_spec_segments` never yields them, and rewrites preserve both entries byte-for-byte.
- **Types-owned structs** (`FileFingerprint`, `KeySpec`) live in `bellwether-types` and get no extra map, so the contract stays additive. Instead:
  - Adding a `KeySpec` kind, or a field to either type, requires a `storage_version` bump.
  - `Manifest::from_slice` pre-scans the raw JSON. If it finds an unknown kind, or an unknown field in either type, it refuses with `Error::UnsupportedVersion { what: "key-spec or fingerprint shape in storage" }` (`UNSUPPORTED_SHAPE`). That beats a serde error, and it never silently drops the field on rewrite.
  - A drift test serializes every `KeySpec` kind and a `FileFingerprint` and compares their keys with the loader's allow-lists. Adding a variant or a field breaks that test's compilation or fails it, so the allow-list and the version bump can't be forgotten.

`Manifest::from_slice` also refuses a newer `storage_version` with `Error::UnsupportedVersion { what: "storage" }` (`UNSUPPORTED_STORAGE`) before typed deserialization.

### List keys (B4)

`list` is a key-spec kind (`key_spec_fields`: `kind`, `column`): declared in `schema.key_specs`, its segments ordinary key-spec segments named `list:<column>`. A build that predates the kind refuses such a manifest with the typed unknown-kind error, never a serde error and never by dropping the declaration on a rewrite (tested with storage version 1's allow-list). The kind is admitted by storage version 2 (WP14); this branch merges after it.

## Commit protocol

```rust
commit(store, root, &staged, |m: &Manifest| -> Result<Manifest> { ... }) -> Result<Committed { version, sequence }>
```

Per attempt:

1. Read `manifest.json` and its version token. If there is none, start from `Manifest::new()` (`sequence` 0).
2. Apply `f`. `f` may return `Err` to abort. For example, a builder seeing that the winner of a race touched `rowgroup_space` should abort rather than assign overlapping row-group ids (ADR 013).
3. Stamp the commit-owned fields:
   - `storage_version` (never downgraded)
   - `sequence = base + 1` (ADR 020)
   - `created_at`, kept from the base
   - `committed_at`
   - a fresh random `commit_nonce`
   - `deleting`, restored from the base whatever `f` returned
4. If the new manifest references a segment in `deleting`, refuse with the claim conflict (see `claimed_segment(&err)`).
5. Reconcile segment tombstones. A dropped segment gets one stamped now; a segment that is referenced again loses its tombstone.
6. Make sure every added segment is present.
   - A **staged** segment is uploaded on every attempt, unconditionally. It is content-addressed, so the write is idempotent, and not trusting `exists` removes any read-after-delete assumption on S3-compatible stores.
   - An **unstaged** (pre-uploaded) segment must `exist`. That check does rely on read-after-delete consistency, which S3, GCS, Azure and local disks provide.
   - Otherwise fail before touching the manifest. A manifest that references a missing segment is never published.
7. CAS the manifest: `put_if_absent` for the first commit, `put_if_version(base_version)` after that.
8. If the CAS reports **any** error, re-read the manifest once:
   - If it carries this attempt's `commit_nonce`, the write landed and only the response was lost: a retried request answered 412, a timeout after the write, a failed fsync after the rename. Return success without re-applying `f`. An applied write is never reported as failed, which apply-then-attest depends on.
   - Otherwise, on `VersionConflict`, back off (5 ms doubling to 500 ms, times a factor in [0.5, 1.5)) and go to step 1. `f` is re-applied to the winner's manifest, which for additive segment lists is a union (`add_segment` is idempotent by id). Attempts are capped at 32 (`CommitOptions`).
   - On any other error, return it. If the re-read itself fails too, the outcome is unknown and the CAS error is returned; a later read shows whether `f`'s effect landed.
9. Write the committed manifest bytes to `<root>/manifests/<sequence as 20-digit zero-padded decimal>.json` with `put_if_absent`. This happens only after the CAS is known to have succeeded (step 7, or step 8's nonce self-win check) — never for an attempt that lost or whose outcome is unknown. Before committing on top of an existing base at sequence `N`, `commit` also checks `manifests/<N>.json` and backfills the exact bytes read from `manifest.json` if absent. History never blocks a commit: an existing history object with different bytes (`manifest.json` rewritten outside the commit protocol at the same sequence) is kept, never overwritten, and a warning is logged; if the post-CAS history write fails, `commit` logs a warning and still returns `Ok` because the commit itself landed, and the next commit backfills the missing history object.

`f` must be **idempotent** and depend only on its input. If a third writer commits between the applied write and the re-read, the self-win goes undetected and `f` is applied once more on top. Idempotence makes that harmless.

The commit returns `sequence`. That is the identity callers record in `key_spec_completeness`, provenance and receipts. `StoreVersion` is only the CAS token.

**Large builds** pre-upload their segments with `put_content_addressed` and commit with an empty staged set, which keeps memory bounded (ADR 018). If GC claims a pre-uploaded segment before the commit lands (it is an orphan until then), the commit is refused. The builder re-uploads and retries once the claim clears.

## Snapshots (invariant 4)

`Snapshot::open(store, root)` reads the manifest once. Every segment read goes through `Snapshot::read_segment`, which:

- refuses ids that the pinned manifest does not reference
- checks the bytes against their blake3 id

`Snapshot::read_segment_range(id, range)` serves block-index and bitmap reads without loading a whole segment (ADR 018). It refuses the same ids, but a partial read cannot be checked against the content hash. `Snapshot::sequence()` is the pinned manifest's identity.

The view of a snapshot opened with `Snapshot::open` changes only through `Snapshot::reopen()`. Two things guarantee this:

- segments are immutable and content-addressed
- GC deletes only claimed segments, and only after retention

**Pinned historical snapshots (`Snapshot::open_at` and `retained_sequences`).** `Snapshot::open_at(store, root, sequence)` opens the committed manifest at `sequence` from `manifests/<20-digit>.json` (or from `manifest.json` when `sequence` is the current manifest and its history object has not been backfilled yet), returning `Ok(None)` when `sequence` is not retained and `Err` if the stored manifest's `sequence` disagrees with the requested one. Calling `reopen()` on a historical snapshot re-opens the same `sequence`. `retained_sequences(store, root)` returns the ascending list of sequences currently openable with `open_at`.
`tests/snapshot_isolation.rs` contains:

- **a stress test**: commits run while a reader uses a pinned snapshot; its view never changes.
- **a multi-writer variant** on the memory backend.
- **a deterministic step-through** of the build-in-progress window: segment uploaded, then manifest swapped.
- **a concurrent-reader test**: readers repeatedly `open` and read every referenced segment while an appender, a compactor, a re-adder of identical bytes, and **two concurrent GC runs** (with slowed deletes, different leases and batch sizes) all run. Abandoned claims from "dead" runs are seeded at the start, so the takeover path runs under contention too. The test asserts that every observed manifest is internally consistent: no referenced segment is claimed, and every referenced segment is readable. It also asserts that every abandoned claim is taken over exactly once. It fails if the claim refusal in `commit` is disabled, or if takeover is keyed to retention instead of the lease.

## Cache

`Cache::put_cached(input_hash, bytes)` writes `cache/<hash>.bin` and then the `cache/<hash>.meta.json` sidecar, `{created_at, payload_blake3}`. `Cache::get_cached(input_hash) -> Result<Option<(Bytes, Duration)>>` returns the payload and its age; `Cache::get_cached_at(input_hash, now)` measures the age to a caller's fixed clock instead (zero when the entry is newer). A missing or unparsable sidecar, or one that disagrees with the payload, counts as a miss. Store I/O errors are returned as errors, not hidden as misses.

## Garbage collection (ADR 013): claim, delete, scan, release

`gc(store, root, retention)`, or `gc_with(..., GcOptions { page_size, max_orphans, claim_lease, delete_batch })`.

**Two windows, unrelated to each other:**

- **Retention** is the snapshot-protection window. A segment is claimed only once its tombstone is older than retention, so a reader whose snapshot is younger than retention can read everything it references.
- **The claim lease** (`claim_lease`, default 1 hour) is a liveness bound. A claim protects the run that made it and records that run's lease (`lease_ms`). Another run treats the claim as abandoned (its run died) and takes it over only once the claim is older than the **larger** of the recorded lease and its own. So runs configured with different leases stay safe: a short-lease run cannot take over a long-lease run's live claim. Retention plays no part in takeover.

Each run draws a random nonzero **token** and works in four phases:

1. **Claim.** One commit moves every unreferenced segment whose tombstone is older than `retention` from `segment_tombstones` into `deleting`, stamped with the run's time, token and lease. It also takes over abandoned claims by re-stamping them. The claim set is recomputed from the winner's manifest on every rebase, so an id the winner re-referenced is never claimed. Claims already carrying the run's token count as its own, so a claim commit that landed but was reported as failed is not orphaned.
2. **Delete, fenced.** Files are deleted only after the claim CAS succeeds, in batches of `delete_batch` (default 64). Before deleting any claimed segment, GC prunes `manifests/`: let `D` be the claimed segment ids this run holds and `m` the greatest sequence among history manifests (`sequence < current.sequence`) that reference any id in `D` (`Manifest::references`); GC deletes every parsable history manifest with `sequence <= m` (`GcReport::manifests_pruned`), so the retained history is always a contiguous suffix `[oldest, current]` and never references a deleted segment. `manifest.json` is never deleted. Unparsable history objects (or objects whose stored `sequence` disagrees with their filename) are counted in `GcReport::unparsable_manifests` and left untouched rather than deleted blindly.
   - **Fence.** Immediately before each delete, the run checks whether half its lease has elapsed since just before the claim was stamped, on **either** clock. If so it stops (`fenced`). The two clocks are needed because they fail differently:
     - The monotonic clock (`Instant`) excludes host sleep: `CLOCK_UPTIME_RAW` on macOS, and `CLOCK_MONOTONIC` on Linux excludes suspend too. A holder that slept longer than its lease would otherwise wake with an almost-zero fence.
     - The wall clock includes the sleep. A forward wall-clock step only stops a run early, which is safe.

     The other half of the lease is margin for clock skew between hosts and for the delete call itself.
   - **Re-read.** The manifest is re-read, and only ids still claimed with the run's token are deleted (`claims_lost` counts the rest). This happens at every batch boundary, and before any delete once more than an eighth of the lease has passed since the last re-read on either clock. It is a second line of defence for when the fence's margin is not enough.
     - **Where it acts.** Only at those points. Within a batch, between re-reads, a run deletes from its last reading.
     - **What it would take to lose a claim there.** A takeover. Before the lease the holder recorded has expired, a takeover needs either a taker clock skewed more than half a lease ahead (a full lease ahead only for a holder that has just claimed), or a holder asleep for over half a lease. The fence and the eighth-of-a-lease re-read catch the sleeping holder when it wakes.
     - **Re-checking every delete.** `delete_batch: 1` re-checks before every delete, at the cost of one manifest read per delete. That closes even the skew case.
3. **Scan.** Unless fenced, `segments/` is streamed one page at a time. A file with no reference, tombstone or claim is an orphan, for example a pre-uploaded, uncommitted build segment. Orphans are never deleted on sight. At most `max_orphans` (default 10,000) are processed per run; the rest are reported in `orphans_deferred` and picked up by later runs.
4. **Release.** A final commit removes every claim carrying the run's token. Claims are released by token, never by timestamp. Claimed ids the run did not delete (it was fenced, or a delete failed) get their original tombstone back, so a later run deletes them (`released_undeleted`). Orphans are tombstoned. A builder that commits an orphan in the meantime clears its tombstone.

If a delete or the scan fails, the run still releases before returning the error. If the claim commit itself fails with an unknown outcome, the run attempts a release by token anyway.

**Why this is race-free.** GC claims only ids the winning manifest does not reference, and it claims them by CAS. Any writer whose base predates the claim loses its CAS and rebases onto a manifest that shows the claim. Any writer whose base includes the claim is refused. No run takes over a live claim before the lease its holder recorded has expired, whatever its own lease, and the holder stops deleting at half that lease on either clock. It also deletes only ids its most recent re-read still showed as claimed with its own token. So, within the clock contract below, no published manifest references a file a run may still delete.

`tests/gc.rs` covers each reproduced interleaving:

| Case | Interleaving |
|---|---|
| S1 | A writer re-adds X between GC's read and its claim CAS |
| S2 | A writer re-adds X between the claim and the first delete; it is refused, then succeeds after release |
| S3 | A writer re-adds X after the delete, before release |
| S6 | Two concurrent GC runs at retention 0 |
| S7 | Two GC runs at retention 50 ms, with GC1 stalled 60 ms between claim and delete |
| Mixed leases | A zero-lease GC2 inside GC1's claim-to-delete window |

The same file also covers:

- the fence
- claims lost to another run mid-delete, twice: at a batch boundary (`claims_lost_mid_run_are_not_deleted`, with `delete_batch: 1`), and within a default-size batch through the wall-clock re-read (`wall_clock_reread_within_a_batch_skips_lost_claims`)
- the two-clock fence, in the `gc.rs` unit tests: a run whose stamp is back-dated by half its lease while its monotonic clock starts now is fenced
- abandoned claims taken over only after the lease
- a claim commit that landed but was reported as a conflict (the token fix)
- a failed delete releasing its claims
- pre-upload-then-commit
- two-run orphan deletion
- paging and the orphan cap

`tests/review_probes.rs` covers the review probes the tests above do not:
- a GC run inside a writer's upload-to-CAS window (S4) and inside a builder's existence-check-to-CAS window (S5)
- the retry bound (S9)
- the lease rule with mixed leases and back-dated claims (P1a, P1e, P1f, P4a, P4b)
- the release after a claim commit with an unknown outcome (P2b)
- the documented residuals (P1g, P4c, P5) as `#[ignore]`d tests that assert they still reproduce
- a check that this section's defaults match the `GcOptions` constants

**Residual risk.** A run that pauses for longer than half the lease exactly between its fence check and its delete call can delete a segment after another run took the claim over, released it, and a writer re-added it. Leases without fencing tokens enforced by the store itself cannot close this window. Choose a lease much longer than any plausible pause; the 1 hour default is.

**Crashes.** If a run dies after claiming, its ids stay blocked (commits referencing them are refused) until a later run finds the claims older than the lease.

**Clocks.**
- **What is compared across hosts.** `since` and `claimed_at` are written by one host's wall clock and compared by another's. Clock skew between hosts shifts the effective retention and lease by the skew.
- **The fence.** It uses the run's own clocks, both of them: half the lease on either the monotonic or the wall clock stops the run.
- **The contract.** Skew between GC hosts must be well under half the lease. A taker whose clock runs more than half a lease ahead (a full lease only for a holder that has just claimed) of a holder's is **outside the contract**: it can see a live claim as abandoned and take it over. With the default batch size, the holder can then still delete, from its last reading, ids it lost within the current batch. `delete_batch: 1` closes even that.

## Memory (ADR 018)

`StoreExt::list_pages(prefix, page_size, f)` polls the `object_store` list stream and holds at most one page. GC uses it and caps the orphans it holds per run. `Store::list` returns a `Vec` because the contract requires it, so it materializes the whole listing; callers that need bounded memory use `list_pages`. Apart from the capped orphan set, GC holds only sets bounded by the manifest's own lists, which are already in memory.

**Join keys (B3).** `SegmentRef.right_sequence` is present only on a join key's segment: the right dataset's manifest sequence the pair index was built from, which the segment header repeats; the allow-list admits the `join` kind at storage version 2 only.
