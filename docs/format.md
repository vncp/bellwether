# On-disk format, storage version 1

Versioned from the first commit. `bellwether-types::format::STORAGE_VERSION` is written into the manifest and every segment header. A binary that reads a newer version refuses with `Error::UnsupportedVersion` and never downgrades in place (ADR 017). A fixture written by v0.1 lives under `fixtures/format-v0.1/` once the format is frozen, and every later version must read it in CI.

```
<index-root>/
  bellwether.toml                source{kind, root, fingerprint_policy}, key_specs[]   (user-editable config)
  manifest.json                  storage_version, sequence (monotonic, +1 per commit; ADR 020),
                                 created_at (first creation, preserved), committed_at (this commit),
                                 commit_nonce (random u64 per commit attempt; detects an applied write reported as 412),
                                 source{kind, root, fingerprint_policy?, partitioning?}
                                           (partitioning: "hive" when name=value directories are columns,
                                            "template:<pattern>" when a path template's placeholders are; see
                                            "Partition columns"; absent for none)
                                 schema{columns[{name, data_type, nullable}], key_specs[],
                                        sidecars?[{kind, columns[], time_columns[]}]}   (see "Sidecar kinds")
                                 rowgroup_space{next_id, files[{fingerprint, rowgroup_ids[], chunk_size?, chunks?[{start,end}]}],
                                                tombstones[{since, rowgroup_ids[]}]}   (retired row-group ids, ADR 013)
                                 (chunk_size and chunks are present only for byte-range sources and are immutable per file)
                                 key_spec_completeness{name -> sequence at which the spec became complete, that is, first covered every live row group (ADR 020)}
                                 segments[{id=blake3, kind?, key_spec=KeySpec::name() or the sidecar's name,
                                           rowgroup_range{start, end} (half-open), bytes}]
                                           (rowgroup_range = the row groups the segment's writer processed, including
                                            any that held no keys; the spec's coverage, and recorded in the segment too;
                                            kind is absent for a key spec's segment, "bounds" or "temporal2" for a
                                            sidecar's; see "Segment kinds" for kinds a reader does not know)
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
  manifests/<sequence>.json      every committed manifest, byte for byte, named by its sequence as 20 zero-padded
                                 decimal digits; written with put_if_absent only after the commit's CAS succeeded,
                                 and backfilled from manifest.json by the next commit if that write failed; never
                                 overwritten and never blocking a commit; see "Manifest history"
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
                                 bounds, temporal2: sidecar segments, their own layout (magic "BWSIDECR"); see
                                           "Sidecar kinds"
                                 sketch:   sidecar segment, its own layout (magic "BWSKETCH"); approximate, never
                                           read by a planner; see "Sketch segments"
  ledger/<blake3>.rec            Record (Evidence | Proposal | Replay | Certificate | Receipt), canonical JSON
                                 Receipt.certificate must resolve and have pass == true
  ledger/<blake3>.sig.<key id>.json
                                 a detached ed25519 signature over the Receipt or Certificate named <blake3>
                                 (ADR 029): canonical JSON {signature_version: 1, record, kind, alg: "ed25519",
                                 key_id (blake3 of the public key), public_key, signed_at (RFC 3339 UTC ms), sig};
                                 signs "bellwether-signature-v1\0" ‖ kind ‖ 0x00 ‖ record hash (32 bytes) ‖ signed_at.
                                 Outside manifests and segments, not a record: record readers skip the name, so it
                                 is not a storage-version or record-version matter
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

**Manifest column types (amendment, 2026-10-24).** `schema.columns[].data_type` is Arrow's `Display` form of the type, which is Arrow-version dependent (contract v3, entry 9). The dependency upgrade to Arrow 59 changed the form of time units: Arrow 53 wrote `Timestamp(Microsecond, Some("UTC"))`, `Timestamp(Second, None)`, `Time64(Nanosecond)` and `Duration(Millisecond)`; Arrow 59 writes `Timestamp(µs, "UTC")`, `Timestamp(s)`, `Time64(ns)` and `Duration(ms)`. Every other type an index can record is spelled the same. Types that carry child fields (lists and list views, structs, maps, unions, run-end encoding, and dictionaries of them) were never recordable, because Arrow 53 could not parse its own display form of them. Arrow 59 can. Since the second 2026-10-24 amendment (real-data defect 1) the manifest records them by that spelling like any other column, so a dataset with a nested column is indexed on its flat columns; only a key column needs a canonical encoding, and a nested key column is refused naming the column. A nested column whose child fields carry metadata, which Arrow's spelling drops, does not read back as the same type and is refused naming the column. An Arrow 53 binary cannot read a manifest that records a nested column. A reader accepts both forms: the build crate's manifest reader rewrites an old temporal spelling itself when Arrow does not accept it, and a test reads every Arrow 53 spelling and an index whose manifest carries them. The first commit by a newer binary rewrites the spelling to the current form; the types, the segments, and the row-group ids do not change.

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

**Amendment, 2026-10-24 (Arrow 53 to 59).** The `arrow:` fallback spells a type with the display form of the Arrow the binary is built on, and Arrow 59 changed that form for types that carry fields. Every type with a segment-format spelling is unaffected, because its digest entry is `ColumnType` JSON, which is ours; that covers every integer, float, decimal, string, binary, date, time, and timestamp. Parquet-to-Arrow inference is also unchanged (see "Statistics semantics", inference amendment), so a duration column still reads as `Int64` and an INT96 timestamp as `Timestamp(ns)`, and their digests do not move.

The digests that do move are those of data with a list, struct, map, or other field-carrying column, which only the fallback can spell. Checked on Parquet folders written by pyarrow with timestamp, time, date, decimal, string, duration, INT96, and list columns, and on a CSV folder: only the folder with a list column moved. A pyarrow list column named its item field `element`, and its spelling changed:
- Arrow 53: `arrow:List(Field { name: "element", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })`
- Arrow 59: `arrow:List(Utf8, field: 'element')`

An item field named `item`, the Arrow default, is spelled `arrow:List(Utf8)`. A `Replay` or `Certificate` recorded before the upgrade over such data does not match a digest recomputed after it, and is re-recorded rather than treated as the same data.

The pinned digests in `bellwether_source::catalog`'s tests were refreshed with this amendment; their fixture has a `List(Utf8)` column. The replay crate's tests still check that the two definitions agree byte for byte.

**Downgrade is one-way.** Once a newer binary has committed to an index, a binary built on Arrow 53 refuses that index. The commit rewrites the manifest's type spellings (see "Manifest column types") into a form Arrow 53 does not parse. Strictly, this happens only when the dataset has a time or timestamp column; an index with neither is spelled the same by both. Nothing converts an index back. An index the newer binary has only read stays readable by both.

## SQL sources: the `sql://` identity scheme (2026-10-25, ADR 027)

A table is read through the same `Source` trait as a folder of files (`bellwether-source-sql`). Nothing in storage version 1 changes: a table never has an index (the engine builds none over a database), so these identities appear only in what a reconcile, `why`, or `fix` over a table records, through the catalog digest above and the source identity below.

**Root.** `sql://<engine>/<schema>.<table>[,<schema>.<table>...]` with `engine` one of `sqlite`, `duckdb`, `postgres`, `flightsql`, `mssql`. The connection and the unit configuration follow `?` (`db=`, `driver=`, `range=`, `units=`, `version=`, `snapshot=`) and are never part of a recorded root: a record names `sql://<engine>/<tables>` only, so a password in a connection string never reaches the ledger. Every table of one root holds the same columns and declared types.

**Units.** A unit plays a file of exactly one row group (`index_in_file` 0). Each table is cut, in order of preference, into its declared partitions (Postgres leaf partitions), else key ranges of `range=<column>` into at most `units=<n>` ranges plus one unit for its nulls, else one unit. Range boundaries are the column's values at ranks `k * count / n` in the engine's own order, so the ranges are `column < b1`, `b1 <= column < b2`, ..., `column >= bk`, and `column IS NULL`: every row in exactly one unit, which discovery checks (the units' row counts must sum to the table's `COUNT(*)`, else the cut is refused). Unit paths:

```
sql://<engine>/<schema>.<table>/all
sql://<engine>/<schema>.<table>/partition=<schema>.<child>
sql://<engine>/<schema>.<table>/<column>=null
sql://<engine>/<schema>.<table>/<column>=[<lo>,<hi>)        lo = -inf for the first, hi = +inf for the last
```

**Fingerprint.** `path` is the unit path; `size` is the unit's row count (a count from the engine, not bytes); `etag` is the unit's identity string; `tail_blake3` is BLAKE3 of the identity string's UTF-8 bytes. The identity string is

```
v1;rows=<count>;version=<hex>      with version=<column>: BLAKE3 hex of the canonical encoding of MAX(<column>) over the unit, or "null"
v1;rows=<count>;stats=<hex>        otherwise: Hash::of_record((rows, [ColumnStats, ...])), the unit's statistics as reported below
```

No engine this release reads exposes a per-table snapshot id, so ADR 027's second identity source is not used yet. The statistics checksum detects a change to the row count or to any column's minimum, maximum, null count, or byte count; an in-place update that keeps all of them is not detected by it, so a table updated in place should name a version column (one that increases on every write), or be read inside a snapshot.

**Statistics.** One aggregate query per unit gives each column `COUNT(*) - COUNT(col)` (the null count, exact) and, where the engine can order in the canonical order, `MIN` and `MAX`, encoded as the dataset type (ADR 016): bounds attained by the data, never a sample. Strings are bounded in UTF-8 byte order (`COLLATE "C"` in Postgres, `COLLATE BINARY` in SQLite, `COLLATE "binary"` in DuckDB); SQL Server and Flight SQL report no string bounds (`None`), since their order is not byte order. Float bounds follow "Statistics semantics" above (a NaN bound is dropped). `distinct_count` is `None` and `has_bloom` false. `bytes` per column is the unit's logical bytes: for fixed-width types the non-null count times the type's width, for strings and binaries the engine's octet length summed; the unit's `RowGroupMeta.bytes` is their sum. They are measured, not storage bytes, which is why costs over a table are measured (ADR 027) and never stored as exact. Planner statistics (`pg_stats`, `sqlite_stat4`, BRIN summaries, DuckDB zone maps, columnstore segments) are not used as bounds: they are samples, or cover block ranges and row groups that are not the unit.

**Source identity.** `SqlSource::identity` is `Hash::of_record((root, connection_identity, [(unit path, identity string), ...]))` in unit path order, where `connection_identity` is BLAKE3 of the connection with its credentials removed (a URI's `user:password@`, and query parameters or ODBC keys naming a password, token, secret, or key), so two databases holding a table of one name are two identities: the first implementation of contract v3 backlog #10. A certificate over a table cites it after the two catalog digests.

**Snapshot.** A run over a table holds one read transaction where the engine has a snapshot (`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` in Postgres, `BEGIN TRANSACTION` in DuckDB, a deferred read transaction in SQLite, `SNAPSHOT` isolation in SQL Server when the database allows it) and rolls it back at the end. Otherwise (`snapshot=reread`, Flight SQL, or SQL Server without snapshot isolation) every unit's identity, and the list of partitions, is read again when the run ends, and any difference is refused (`Error::SourceChanged`) before anything is recorded. A unit read returning another row count than its identity says is refused either way.

## Build-side memory (ADR 018 note)

The lookup bound is one block index plus the bitmaps for the keys asked. The build bound is one segment: a writer holds a segment's distinct keys until the segment is finished, because the segment's name is the hash of its bytes. The build crate therefore caps segment size (keys and bytes) and emits many segments rather than one large one; merge compacts them later.

## Statistics semantics

Signed zero: a stored or read minimum of +0.0 is widened to -0.0 and a maximum of -0.0 to +0.0 (the Parquet statistics reading rule), so a probe for either zero never prunes a row group holding the other. An equality probe for zero by a plan builder uses both encodings.

`ColumnStats.min`/`max` are bounds, not necessarily attained values; inexact Parquet statistics are accepted. Float bounds exclude NaN by Parquet writer convention; a NaN probe is therefore never pruned by range statistics. Deprecated Parquet `min`/`max` fields are used only when the column's sort order is SIGNED. The dataset schema reported by `Source::schema(root)` is the single source of column types for encoding; a supplied schema overrides inference for text sources.

**Amendment, 2026-10-24 (parquet 53 to 59).** The rules above hold unchanged on the new reader API. Two things changed upstream, and neither relaxes a rule:

- parquet 59 exposes a column order that is present in the footer but unknown to the reader (a later format's `ColumnOrder` union member) as `ColumnOrder::UNKNOWN`. The specification says statistics under an unknown order are ignored. The source therefore uses neither min nor max for such a column, deprecated or not. This is stricter than before, never looser: the column reports unknown bounds, which never prunes.
- The parquet 59 writer, when used with default properties, truncates byte-array statistics to 64 bytes. It writes a truncated prefix as the minimum and an incremented prefix as the maximum, and marks both inexact. The rule that bounds need not be attained values already covers this. It concerns files written by tools, tests, and the bench generator, not how statistics are read. The writer also no longer writes page-header statistics by default, and it folds Parquet bloom filters down to the smallest size that meets the configured false-positive rate. Neither affects what this reader trusts.

**Inference amendment, 2026-10-24 (parquet 53 to 59).** Parquet-to-Arrow type inference never changes across a dependency upgrade. A column's inferred type is recorded in the manifest and decides its canonical encoding, so the same files must yield the same types.

parquet 59 honours three embedded Arrow schema hints that parquet 53 did not. The source (`bellwether_source::load_reader_metadata`, used for data files and key files alike) keeps parquet 53's type for each:

| Column, as written | parquet 59 reads | The source reads (= parquet 53) |
|---|---|---|
| `INT64` with a `Duration` hint (pyarrow and arrow-rs write one for duration arrays) | `Duration(unit)` | `Int64` |
| `INT96` with a timestamp hint at a unit other than nanoseconds | `Timestamp(unit, tz)` | `Timestamp(ns)`, no zone |
| `INT96` with a nanosecond timestamp hint and a zone | `Timestamp(ns, tz)` | `Timestamp(ns, tz)`, unchanged |
| a decimal with a `Decimal32` or `Decimal64` hint | `Decimal32` or `Decimal64` | `Decimal128` at the same precision and scale |
| any of the above under a `Dictionary` hint (for example pyarrow `dictionary<duration[ms]>`) | `Dictionary(key, value)` | the plain rewritten type (`Int64`, `Timestamp(ns)`, `Decimal128`); parquet 53 dropped a dictionary hint whose value hint did not apply |

For the narrow decimals, parquet 53 actually panicked inside arrow-ipc ("Unexpected decimal bit width 32"). `Decimal128` is what it read without the hint.

A newer reader's type is adopted only deliberately, by a later amendment that names it. Tests pin every row on pyarrow-written files in `fixtures/hints/`.

Two mappings are new and change nothing that parquet 53 read: a Geometry or Geography column reads as `Binary`, and the always-null Unknown logical type reads as `Null` on every physical type.

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

Not part of version 1 and not read by version 1 readers: (1) exact segments may carry, per key and per set bit of its bitmap, a row count, enabled by `counts: true` on the key spec; (2) scan entries carry per-column bytes so a plan's projection cost is a sum of chunk sizes; (3) manifests record per-row-group bytes and per-column bytes at build time so plans need not open footers; (4) the version 1 sidecar kinds (`bounds`, `temporal2`, "Sidecar kinds" below) fold into the segment they ride beside: an exact or bloom segment's per-key value carries the per-row-group time bounds, and the bitemporal triples become a temporal layout, so one segment read answers what a key segment plus a sidecar answer in version 1. A version 2 reader still reads version 1 sidecars. The format freeze happens after these exist, so that version 2 is the frozen version.

## Inference stability (2026-10-24)

The dataset type a reader infers for a column is part of the format, not of the reader. Upgrading a dependency must never change the inferred type of any column that an earlier reader accepted, even when the newer library can read an embedded type hint the older one ignored. When a smarter reader becomes available, the source crate keeps the earlier mapping and the new type is added deliberately, by a dated amendment here, with a conformance vector. Found during the Phase 3 upgrade, where parquet 59 began honouring the Arrow Duration hint on INT64 columns and the INT96 unit hint, which would have broken incremental builds on existing indexes.

## Partition columns (storage version 1, 2026-10-24)

An index built with `--partitioning hive` records `source.partitioning = "hive"` and treats every directory component of a file's path **below the data root** of the form `name=value` as a column of that file: every row of the file has `value` in column `name`. The Parquet source reports it in each row group's statistics with `min = max = value`, zero bytes, and a null count of zero (all rows for a null value), and synthesizes it when the column is read, so the profiler sees it and every planner path prunes on it without reading a data page. An index without the field has no partition columns, and the scheme of an existing index never changes (a different one is refused, because it changes the dataset schema and every row group's statistics). Text sources refuse hive partitioning in this release.

**Path templates.** An index built with `--path-template "<pattern>"` records `source.partitioning = "template:<pattern>"`, the pattern exactly as given, so another template is another scheme and is refused. The pattern is the path of every data file below the root, `/`-separated, with `{name}` or `{name:type}` placeholders (`str`, `int`, or `date`; `{{` and `}}` are literal braces). A placeholder matches one or more characters other than `/` that its type accepts (`date` is exactly a valid `YYYY-MM-DD`, `int` the integers of the inference table). The whole path must match in exactly one way: a file that matches in none, or in more than one, is refused, and discovery names up to ten such files and counts the rest. Each placeholder is then a partition column exactly as a hive directory is (statistics `min = max = value`, zero bytes, synthesized on read, a stored column of the same name must agree), typed by the dataset schema when it declares the column, else by the placeholder's type, else by the inference table over every value. A template has no null spelling. A binary from before templates reads `"template:..."` as an unknown partitioning and refuses the index, never misreading it. Windows on a partition column prune by path values (`docs/time-series-capabilities.md`).

- A component splits at its first `=`. An empty name, an empty value, or a name repeated in one path is refused; the file name never counts; components of the root itself never count.
- Values are percent-decoded (`%2F` is `/`); a `%` not followed by two hex digits, or a value that does not decode to UTF-8, is refused. `__HIVE_DEFAULT_PARTITION__` is a null.
- Every file under the root has the same partition columns in the same order, else discovery refuses the folder.
- **Types.** A column the dataset schema declares takes that type (an exact Arrow cast from the string; a value that does not cast is refused). Otherwise the type is inferred over **every** value under the root, never the first file and never by a dependency's inference: `Int64` if every non-null value is an optional `-` and digits with no leading zero that fit `i64`; else `Date32` if every non-null value is a valid `YYYY-MM-DD`; else `Utf8` (also for a column whose every value is null). `007`, `+7`, `1.5`, `true`, and timestamps stay strings, because they would not print back as written. This table is part of the format (see "Inference stability").
- A file that also stores the column must agree with its path: each row group's footer bounds both equal the path value with a null count of zero (or, for a null value, a null count equal to the row count). Disagreeing or unknown statistics are refused, never trusted. The stored column is then read from the file.

## Plan order (2026-10-24)

A plan's entries are one per file and name their row groups as a set, read in ascending index. When a complete range key on a timestamp or date column exists (several: the one on the lookup's own time column), lookups order entries by the smallest row-group minimum among each entry's planned row groups, as the range key's segments record it (no minimum, every value null: last; ties by path), and the result names the column (`ordered_by`) only when reading entries in order, each entry's row groups ascending, visits every planned row group in non-decreasing minimum. Otherwise the result says the order is unspecified and why (two files overlap in time, or a file's own row groups run out of time order). The planned row groups and bytes never depend on the order.

## Manifest history (2026-10-24)

`manifests/<sequence>.json` holds each committed manifest's exact bytes, so a lookup can be planned against an earlier sequence (`--at-manifest N`) and return the plan it returned then, byte for byte. The copy is written with `put_if_absent` after the commit's CAS is known to have succeeded (never for an attempt that lost or whose outcome is unknown); a commit that cannot write it still succeeds, and the next commit backfills the base's copy from `manifest.json`. An existing copy with different bytes is kept and never overwritten (`manifest.json` was rewritten outside the commit protocol); history never blocks a commit. Garbage collection, before deleting any segment, deletes every history manifest at or below the greatest sequence that references a segment it deletes, so the retained sequences are always one contiguous run ending at the current one, and every retained sequence's segments exist. A history object that does not parse, or whose sequence disagrees with its name, is counted and left in place. A pinned lookup refuses a sequence outside the retained run and names the run; an index written before this amendment retains sequences from its first commit by a binary that writes history.

## Segment kinds (2026-10-24)

`segments[].kind` names what a segment is: absent for a key spec's segment, or a sidecar kind (`bounds`, `temporal2`). A reader that finds a `kind` it does not know, in `segments[]` or in `schema.sidecars[]`, still opens the manifest; it never plans with, merges, or counts toward coverage a segment of that kind, a build keeps the segment and the declaration unchanged, and the unknown kinds are reported (`bellwether index` lists them). This is the rule that lets a later version add a sidecar kind without a storage version bump. A binary that predates the field keeps both as unknown fields; it never plans with a sidecar's segments, because their `key_spec` is the sidecar's name, which no key spec has, and a build by such a binary drops them as undeclared, after which the sidecar is incomplete until rebuilt.

## Sidecar kinds (storage version 1, additive sidecar kinds, 2026-10-24)

A sidecar is declared in `schema.sidecars[]` as `{kind, columns, time_columns}` and named `<kind>:<columns joined by ,>@<time columns joined by ,>`. Its segments are listed in `segments[]` with that `kind` and that name as `key_spec`, are complete under that name in `key_spec_completeness` exactly as a key spec is (their recorded ranges cover every live row group), and are built over the row groups they do not cover, like a key spec. An incomplete `bounds` sidecar is not used (the plan is a superset, never short); a bitemporal lookup without a complete `temporal2` sidecar is refused.

- `bounds` (`--key exact:symbol@ts`, or `bloom:symbol@0.01@ts`, beside the exact or bloom spec): per key, per row group holding the key with at least one non-null time, the inclusive `[min, max]` of the time column over that key's rows in that row group. A window lookup `(keys, [lo, hi])` keeps a row group for a key only if that key's own range there overlaps the window; a row group where the key's times are all null holds no row of any window. The residual is `In` then `Between` on the time column.
- `temporal2` (`--key temporal2:id@valid,known`): per key, every distinct `(valid, known, row group)` triple over rows where neither time is null. The as-of answer for `(V, K)`, either bound optional, is per key the greatest `valid <= V` among triples with `known <= K`, then the greatest `known` at that valid time; every row group holding that pair is planned (ties are kept). Retired row groups never win. The residual is `In` over keys with a winner, `Between(valid, lowest winning valid, V or the highest)`, `Between(known, lowest winning known, K or the highest)`, `NotOlderThan` on valid per key, `NotOlderThan` on known per key, then `LatestPerKey` on valid: filters first, then the newest valid version, then the newest known version of it (contract-v3 entry 11's order). No new `Predicate` variant is needed.

Times are `i64` in the dataset column's unit, as temporal specs take them. Layout, all integers little-endian, `i64` two's complement:

```
preamble  magic "BWSIDECR" | storage_version u32 | encoding_version u32 | header_len u32
header    JSON {storage_version, encoding_version, kind, columns, time_columns, codec:"none", key_types?, time_types?, ..unknown}
          (key_types and time_types in the segment format's ColumnType spelling, present together when written from a dataset schema)
body      blocks; block = n u32 | n x entry; entry = key_len u32 | key | count u32 | count x record
          bounds record    = rg u32 | min i64 | max i64        (strictly ascending rg, min <= max)
          temporal2 record = valid i64 | known i64 | rg u32    (strictly ascending (valid, known, rg))
          keys strictly ascending across the file; count >= 1
index     entries u64 | has_rg u8 | rg_min u32 | rg_max u32 | block_count u32 |
          block_count x (first_key_len u32 | first_key | last_key_len u32 | last_key | off u64 | len u32 | n u32)
footer    index_offset u64 | index_len u64 | magic "BWSIDECR"
```

A block holds at most 1024 keys or 64 KiB of key bytes. Readers refuse a newer storage or encoding version from the preamble, an unknown kind or codec, and any offset, order, or range that does not hold. A lookup holds the block index, one block at a time, and the records of the keys asked (ADR 018); the build caps a sidecar segment's keys and bytes like any segment.

## Sidecar conformance vectors (storage version 1)

`crates/bellwether-core/tests/sidecar_conformance.rs` builds these from already-encoded keys (`"AAPL"` = `06 41 41 50 4c 00 00`, `"MSFT"` = `06 4d 53 46 54 00 00`, row-group range 0..4, no recorded types) and checks every row of this table byte for byte. The bounds sidecar holds AAPL: rg 0 `[1, 86400000000]`, rg 2 `[-1, 1]`; MSFT: rg 1 `[86400000000, 86400000000]`. The temporal2 sidecar holds AAPL: `(1, 1, rg 0)`, `(1, 86400000000, rg 2)`; MSFT: `(86400000000, 86400000000, rg 1)`.

| Vector | Bytes |
|---|---|
| `bounds record rg=0 min=1 max=86400000000` | `00 00 00 00 01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00` |
| `temporal2 record valid=1 known=86400000000 rg=2` | `01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00 02 00 00 00` |
| `bounds:symbol@ts blake3` | `56b28b58922841137a0d3b51f47293790dd8099934f56292360b683405863b95` |
| `temporal2:symbol@valid,known blake3` | `63dc33afbfe97a78dcd5003a3390061bca9c20fdf19e811a24c839170548d31c` |

## Sketch segments (storage version 1, additive sidecar kind, 2026-10-26)

`schema.sidecars[]` holds structures the planner never reads, beside the key specs it plans with. `sketch` is the first that is approximate: per item, per file, HyperLogLog registers and an optional MinHash signature, answering `count(distinct item)` and the overlap of two items with an error bound. It is declared `{kind: "sketch", columns: [items], time_columns: [], params: {"p": "<precision>", "minhash": "<k>"}}`, where an item is one column or a `+`-joined tuple (`symbol+ts`), and named `sketch:<items joined by ,>` (no `@`: it has no time column). `params` omits `minhash` when no signature is kept; the default precision is 14. Its segments are listed with `kind: "sketch"` and that name as `key_spec`, are complete under that name exactly as a key spec is, and are built over the row groups they do not cover. **No planner path reads a sketch segment**: a plan built from an approximate number could miss a row group (invariant 1). A binary that does not know the kind keeps the declaration and the segments (see "Segment kinds").

An item's value is its canonical bytes (ADR 016); a tuple is encoded like a composite key (ADR 014): a row whose leading column is null is not sketched and counts as a null, and a null trailing column is the `0x00` marker. Each value is hashed once with XXH3-128, seed `0x6265_6c6c_7765_7468` (the profiler's seed, so a column's merged registers equal the profiler's sketch of it). The low 64 bits feed the HyperLogLog: register index = the top `p` bits, rank = leading zeros of the remaining bits plus one, capped at `65 - p`. MinHash slot `i` is `mix64(h1 + i * (h2 | 1))`, wrapping, with `h1`/`h2` the low/high 64 bits and `mix64` SplitMix64's finalizer; the signature keeps each slot's minimum. Registers merge by maximum and slots by minimum, both lossless, so a file split across segments merges back to the sketch of the whole file.

One entry per file stretch: the build starts an entry whenever the file changes or a row-group id does not follow the previous one, so an entry's row groups always belong to one file. Layout, all integers little-endian:

```
preamble  magic "BWSKETCH" | storage_version u32 | encoding_version u32 | header_len u32
header    JSON {storage_version, encoding_version, kind:"sketch", items:[[col,..],..], item_types?:[[ColumnType,..],..],
                precision, minhash:k|null, hash:"xxh3_128", seed, codec:"none", ..unknown preserved}
body      entry*; entry = rg_start u32 | rg_end u32 | item* (header order)
          item    = rows u64 | nulls u64 | hll | minhash?
          hll     = 0x00 | n u32 | n x (index u32 | rank u8)    sparse, iff 4 + 5n < 2^p
                  | 0x01 | 2^p x rank u8                        dense, otherwise
          minhash = k x u64                                     present iff the header's minhash is k
footer    entries u32 | range_start u32 | range_end u32 | BLAKE3 of every byte before the checksum (32) | magic "BWSKETCH"
```

Readers refuse: a newer storage or encoding version, any kind, codec, hash, or seed but the ones above, a checksum mismatch (so every damaged byte is refused, never read as another estimate), entries that are empty (`rg_start >= rg_end`), out of order, overlapping, or outside `[range_start, range_end)` (the row groups the writer processed, recorded so that two segments over different row groups never share bytes), sparse indices out of order or range, a rank of 0 in a sparse list or above `65 - p` anywhere, the encoding the size rule does not pick, and an item whose `rows` disagrees with its registers (`rows = 0` exactly when every register is 0 and every slot is `u64::MAX`). A reader also refuses a segment whose `item_types` differ from the dataset's types for those columns, because its hashes describe other bytes.

Estimates. Distinct counts use Ertl's improved raw estimator (relative standard error about `1.04 / sqrt(2^p)`, 0.81 % at `p = 14`); the reported interval is three standard errors either side, clamped to `[1, rows]` when any row was sketched. Overlap is MinHash when both items keep signatures of one length (the share of agreeing slots, bounded by the Wilson score interval at three standard errors), else HyperLogLog inclusion-exclusion over the merged registers with bounds propagated from each estimate's interval. Items of different column types are refused (their values never compare equal), as are items at different precisions. Every number derived from a sketch is labelled `approximate` in every type and every output.

## Sketch conformance vectors (storage version 1)

`crates/bellwether-core/tests/sketch.rs` writes the reference segment (items `id` (Int64) and `id+tag` (Int64, Utf8), `p = 4`, `minhash = 2`; file 0 is one row group holding `(1, "a")` and `(2, null)`; file 1 is two row groups, `(1, "a")` then `(null, null)`; row groups 0..3) and checks every row of this table byte for byte.

| Vector | Bytes |
|---|---|
| `value_hash(Int64 1)` | `d6ef277c610adb0706d0067c5aa355d5` |
| `preamble` | `42 57 53 4b 45 54 43 48 01 00 00 00 01 00 00 00 f3 00 00 00` |
| `first entry, item id` | `00 00 00 00 01 00 00 00 02 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 00 00 00 00 00 00 00 02 0f 00 00 00 03 08 a9 ce 44 2c 92 04 07 de 13 b3 66 ea df fb b4` |
| `footer` | `02 00 00 00 00 00 00 00 03 00 00 00 ab 25 6d ba de e3 11 dd b0 80 2d e7 f2 40 53 1b a6 12 d6 4b 1e 04 dc d8 62 76 30 ad a5 67 a7 d0 42 57 53 4b 45 54 43 48` |
| `segment blake3` | `e84c9be5a3d4bdad2674f98364b2d2440e40795bf7f91c35a5c923429ddfd4f3` |
