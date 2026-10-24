# On-disk format, storage version 1

Versioned from the first commit. `bellwether-types::format::STORAGE_VERSION` is written into the manifest and every segment header. A binary that reads a newer version refuses with `Error::UnsupportedVersion` and never downgrades in place (ADR 017). A fixture written by v0.1 lives under `fixtures/format-v0.1/` once the format is frozen, and every later version must read it in CI.

```
<index-root>/
  bellwether.toml                source{kind, root, fingerprint_policy}, key_specs[]   (user-editable config)
  manifest.json                  storage_version, sequence (monotonic, +1 per commit; ADR 020),
                                 created_at (first creation, preserved), committed_at (this commit),
                                 commit_nonce (random u64 per commit attempt; detects an applied write reported as 412),
                                 source{kind, root, fingerprint_policy?},
                                 schema{columns[{name, data_type, nullable}], key_specs[]}
                                 rowgroup_space{next_id, files[{fingerprint, rowgroup_ids[], chunk_size?, chunks?[{start,end}]}],
                                                tombstones[{since, rowgroup_ids[]}]}   (retired row-group ids, ADR 013)
                                 (chunk_size and chunks are present only for byte-range sources and are immutable per file)
                                 key_spec_completeness{name -> sequence at which the spec became complete, that is, first covered every live row group (ADR 020)}
                                 segments[{id=blake3, key_spec=KeySpec::name(), rowgroup_range{start, end} (half-open), bytes}]
                                           (rowgroup_range = the row groups the segment's writer processed, including
                                            any that held no keys; the spec's coverage, and recorded in the segment too)
                                 segment_tombstones[{id, since}]   (unreferenced segments awaiting GC, ADR 013)
                                 deleting[{id, claimed_at, token, lease_ms}]
                                                                   (GC claims; a commit referencing a claimed id is refused;
                                                                    token = random u64 of the claiming run, release is by token;
                                                                    lease_ms = the claiming run's lease; a claim is abandoned once
                                                                    older than max(lease_ms, the taker's lease), never by retention)
                                 committed by conditional PUT (put_if_absent for first, put_if_version after)
                                 timestamps are RFC 3339 UTC
                                 unknown fields in manifest-owned objects are preserved on rewrite; an unknown KeySpec
                                 kind or an unknown field in KeySpec or FileFingerprint is refused with
                                 Error::UnsupportedVersion (adding one requires a storage_version bump; ADR 017)
  segments/<blake3>.seg          header{storage_version, encoding_version, key_spec, codec, bloom_hash}
                                 bloom_hash ∈ {"canonical", "parquet_plain"}; readers refuse any other value
                                 exact:    block index of sorted key blocks, one split-block bloom per block,
                                           key → bitmap offset; bitmaps: roaring, over global RowGroupId,
                                           always in the no-run serialized form (cookie 3a 30 00 00), never empty;
                                           readers refuse any other cookie or an empty keyed bitmap
                                 header also records each key column's dataset type (and the temporal ts type
                                           and unit); segments with differing recorded types never merge
                                 index records the row-group range the writer processed (inclusive; a merge
                                           records the hull of its inputs'), so a segment whose row groups held
                                           no keys still records them and two segments over different row groups
                                           never share bytes or an id; a writer not told records the range its
                                           keys reference
                                 bloom:    one split-block bloom per row group (the shape Parquet embeds),
                                           header field bloom_hash names the hash input; newtypes keep the two apart
                                 range:    (min, max) canonical bytes per row group
                                 temporal: exact layout plus key → [(ts, rowgroup_id)] chain + max_ts;
                                           latest/asof return every row group at the winning ts (ties)
  ledger/<blake3>.rec            Record (Evidence | Proposal | Replay | Certificate | Receipt), canonical JSON
                                 Receipt.certificate must resolve and have pass == true
  cache/<input-hash>.bin         footer stats, fanout tables, recorded workload key sets
                                 rebuildable; carries age; never cited without its hash
  cache/<input-hash>.meta.json   sidecar {created_at (RFC 3339), payload_blake3}; written after the .bin; an entry whose
                                 sidecar is missing, unparsable, or disagrees with the payload is a cache miss

file fingerprint = (path, size, etag: Option<String> (object-store etag, else RFC 3339 mtime), content_hash)
                   content_hash per fingerprint_policy: "tail64k" (default) = blake3(last 64 KiB, or whole file if smaller);
                   "full" = blake3(whole file). Only "full" detects an in-place patch outside the last 64 KiB (ADR 013).
                   Verifying the opened handle pins the inode, not the bytes; a write to the same inode during a read is undetected.
rowgroup id      = dense u32 assigned at first sighting of (fingerprint, index_in_file); never reused
```

## Key specs

`Exact{columns}`, `Bloom{columns, fpr}`, `Range{column}`, `Temporal{columns, ts_column}`. Composite is not a kind; every kind takes a column list and encodes it memcomparably (ADR 002). CSV and JSON lines sources define row groups as fixed byte-range chunks recorded in the manifest, so the rest of the system never knows the difference.

## Canonical encoding (ADR 016), encoding version 1

One byte encoding per Arrow type serves three uses: key ordering, bloom hashing, and reconcile row hashing.

| Arrow type | Encoding |
|---|---|
| Null | not indexed; in row hashing a single `0x00` null marker |
| Boolean | `0x01` + `0x00`/`0x01` |
| Int8..Int64 | `0x02` + widened to 64 bits, big-endian with sign bit flipped |
| UInt8..UInt64 | `0x03` + widened to 64 bits, big-endian |
| Float32/64 | `0x04` + widened to f64, IEEE bits with sign-aware flip (total order; a single canonical NaN sorts last) |
| Decimal128/256 | `0x05` + width tag (`0x10` = 16 bytes, `0x20` = 32 bytes) + value rescaled to the **dataset schema's** declared scale (a file at another scale is rescaled; a value not representable is an error, never rounded), then 16 (Decimal128) or 32 (Decimal256) bytes big-endian with sign bit flipped; width fixed by the dataset type |
| Utf8/LargeUtf8 | `0x06` + bytes with `0x00` escaped as `0x00 0xFF`, terminated by `0x00 0x00` |
| Binary | `0x07` + same escaping as Utf8 |
| Date32/64, Time, Timestamp | `0x08` + unit tag (`0x01` Date32, `0x02` Date64, `0x03` Time, `0x04` Timestamp) + time-unit tag (`0x01` s, `0x02` ms, `0x03` µs, `0x04` ns, `0x05` day) + Int64 encoding; timestamps normalized to UTC in the **dataset schema's** unit (never forced to nanoseconds, so year-9999 sentinels in ms/us columns do not overflow); `asof` takes its timestamp in that unit |
| Composite tuple | concatenation of element encodings; prefix seek works because each element is self-delimiting |

Row hash for reconcile: xxh3-128 (seed 0) over the concatenation of canonical encodings in a fixed column order (schema order minus ignored columns), each preceded by its column name length and bytes. The length is a 4-byte big-endian `u32` of the name's UTF-8 byte count; a null value is the single `0x00` marker; each value is encoded at its side's dataset type. The fixed order is the left dataset schema's, used for both sides. `ignored_columns` is recorded on the Certificate. `crates/bellwether-verify/tests/row_hash.rs` pins a golden value.

Bloom hash: xxhash64 over the canonical encoding, split-block filter per the Parquet specification. Embedded Parquet filters hash the physical plain encoding instead, so when one is reused the probe value is encoded per the filter's origin (ADR 008, amendment). Each filter records its origin.

## Catalog digest

The identity of the data a reader saw, cited by a `Replay` (its `inputs`) and by a `Certificate` (its provenance) so both name the same data the same way. One definition, `bellwether_source::catalog`, which is byte for byte the replay crate's `Catalog` digest (the replay crate's tests check the two on an unindexed folder and on a built index): BLAKE3 over the canonical JSON (`serde_json`, no whitespace) of

```
{"manifest_version":<string>,
 "schema":[[<name>,<type>,<nullable>],...],
 "files":[[<FileFingerprint>,[{"id":<u32>,"index_in_file":<u32>,"rows":<u64>,"bytes":<u64>,"byte_range":<[start,end] or null>,"stats":[<ColumnStats>,...]},...]],...],
 "tombstoned":[<row-group id>,...]}
```

with the fields in that order. `schema` is the dataset schema, one entry per column: its name, its type in the segment format's recorded spelling (core's `ColumnType`, as JSON) or else `arrow:` followed by Arrow's display form, and whether it is nullable. Each row group carries its global id and the source's report of it (`RowGroupMeta`: position, rows, compressed bytes, byte range, column statistics), so the same files under another dataset schema or another chunk size are other data.

- **Indexed** (`catalog::indexed`): `manifest_version` is the manifest's `sequence`; `files` are `rowgroup_space.files` in manifest order, each with its row groups in ascending id order (the manifest's ids, the source's reports); `tombstoned` is every id in `rowgroup_space.tombstones`, ascending and distinct.
- **Unindexed** (`catalog::unindexed`): `manifest_version` is `"0"`, the sequence of a manifest never committed (ADR 020); `files` are the discovered files sorted by path, with ids assigned densely from 0 in file order, then row-group order; `tombstoned` is empty. A certificate over two unindexed folders records manifest version `"0"` and cites both digests.

## Build-side memory (ADR 018 note)

The lookup bound is one block index plus the bitmaps for the keys asked. The build bound is one segment: a writer holds a segment's distinct keys until the segment is finished, because the segment's name is the hash of its bytes. The build crate therefore caps segment size (keys and bytes) and emits many segments rather than one large one; merge compacts them later.

## Statistics semantics

Signed zero: a stored or read minimum of +0.0 is widened to -0.0 and a maximum of -0.0 to +0.0 (the Parquet statistics reading rule), so a probe for either zero never prunes a row group holding the other. An equality probe for zero by a plan builder uses both encodings.

`ColumnStats.min`/`max` are bounds, not necessarily attained values; inexact Parquet statistics are accepted. Float bounds exclude NaN by Parquet writer convention; a NaN probe is therefore never pruned by range statistics. Deprecated Parquet `min`/`max` fields are used only when the column's sort order is SIGNED. The dataset schema reported by `Source::schema(root)` is the single source of column types for encoding; a supplied schema overrides inference for text sources.

## Pinned test vectors (encoding version 1)

Both the core encoder and any source-side stats encoder must reproduce these bytes exactly; a shared conformance test runs both over the same inputs.

| Value | Bytes |
|---|---|
| `Timestamp(µs) = 1` | `08 04 03 80 00 00 00 00 00 00 01` |
| `Int64 = 0` | `02 80 00 00 00 00 00 00 00` |
| `Int64 = -1` | `02 7f ff ff ff ff ff ff ff` |
| `Utf8 = "a"` | `06 61 00 00` |
| `Utf8 = "a\0b"` | `06 61 00 ff 62 00 00` |
| `Boolean = true` | `01 01` |

Additional vectors (decimal rescale, Decimal256, floats including the canonical NaN) are appended by the conformance test as they are agreed, never by one side alone.

## Planned for storage version 2 (ADR 026)

Not part of version 1 and not read by version 1 readers: (1) exact segments may carry, per key and per set bit of its bitmap, a row count, enabled by `counts: true` on the key spec; (2) scan entries carry per-column bytes so a plan's projection cost is a sum of chunk sizes; (3) manifests record per-row-group bytes and per-column bytes at build time so plans need not open footers. The format freeze happens after these exist, so that version 2 is the frozen version.

## Inference stability (2026-10-24)

The dataset type a reader infers for a column is part of the format, not of the reader. Upgrading a dependency must never change the inferred type of any column that an earlier reader accepted, even when the newer library can read an embedded type hint the older one ignored. When a smarter reader becomes available, the source crate keeps the earlier mapping and the new type is added deliberately, by a dated amendment here, with a conformance vector. Found during the Phase 3 upgrade, where parquet 59 began honouring the Arrow Duration hint on INT64 columns and the INT96 unit hint, which would have broken incremental builds on existing indexes.

