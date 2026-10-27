# On-disk format, storage version 2 (frozen)

Versioned from the first commit. `bellwether-types::format::STORAGE_VERSION` (2) is written into the manifest and into the header of every `BWSEGMNT` segment, a bloom's `bounds` segment included. Two other segment layouts carry their own layout version, 1, in their preamble and header: a `sketch` segment (`BWSKETCH`, "Sketch segments") and a `bounds` or `temporal2` sidecar of an index written before M5 (`BWSIDECR`, "Sidecar kinds"), which no build writes since M5. A binary that reads a newer version refuses with `Error::UnsupportedVersion` and never downgrades in place (ADR 017). This binary writes storage version 2 and reads version 1 ("Reading storage version 1"). **Storage version 2 and record version 3 are frozen (M5, ADR 028):** a later change to the bytes of a segment, a manifest or a record is a new version with a new fixture directory, never an edit. The freeze came in two steps: the first storage version 2 release made the record version 3 shapes final, and the second froze the bytes, folding per-key time bounds into the exact value record and bitemporal versions into a `bitemporal` layout, and spelling a manifest column's `data_type` and a text source's `dialect` ("Segments, storage version 2", "Manifest, storage version 2"). `fixtures/format-v1/` (built by the last version 1 binary), `fixtures/format-v0.1-pre/` (built by the first storage version 2 release, before M5) and `fixtures/format-v0.1/` (built at M5, the frozen bytes) are read in CI by every later binary, and every plan and record hash they pin must replay identically (`crates/bellwether-cli/tests/format_compat.rs`).

```
<index-root>/
  bellwether.toml                source{kind, root, fingerprint_policy}, key_specs[]   (user-editable config)
  manifest.json                  storage_version, sequence (monotonic, +1 per commit; ADR 020),
                                 created_at (first creation, preserved), committed_at (this commit),
                                 commit_nonce (random u64 per commit attempt; detects an applied write reported as 412),
                                 source{kind, root, fingerprint_policy?, partitioning?, dialect?}
                                           (partitioning: "hive" when name=value directories are columns,
                                            "template:<pattern>" when a path template's placeholders are; see
                                            "Partition columns"; absent for none. dialect: a CSV source's
                                            {delimiter u8, header, quote u8}; see "Manifest, storage version 2")
                                 schema{columns[{name, data_type, nullable}], key_specs[],
                                        sidecars?[{kind, columns[], time_columns[], params?}]}
                                           (data_type: core's ColumnType JSON, or {"arrow": "<display>"};
                                            sidecars: kinds no planner reads, such as "sketch"; a manifest
                                            written before M5 also declares "bounds" and "temporal2" there,
                                            which the next build with work folds into key specs; see "Sidecar kinds")
                                 rowgroup_space{next_id, files[{fingerprint, rowgroup_ids[], rowgroup_rows[], rowgroup_bytes[],
                                                column_bytes?[], chunk_size?, chunks?[{start,end}]}],
                                                tombstones[{since, rowgroup_ids[]}]}   (retired row-group ids, ADR 013)
                                 (chunk_size and chunks are present only for byte-range sources and are immutable per file;
                                  rowgroup_rows, rowgroup_bytes and column_bytes are storage version 2: see "Manifest, storage version 2")
                                 key_spec_completeness{identity -> sequence at which the spec became complete, that is, first covered every live row group (ADR 020)}
                                 segments[{id=blake3, kind?, key_spec=KeySpec::identity() or the sidecar's name,
                                           rowgroup_range{start, end} (half-open), bytes,
                                           join? (a join key's segment only: {right_sequence, right_manifest, left_live}, what it pins; see "Join keys")}]
                                           (rowgroup_range = the row groups the segment's writer processed, including
                                            any that held no keys; the spec's coverage, and recorded in the segment too;
                                            kind is absent for a key spec's segment, "bounds" for a bloom's per-key
                                            bounds (key_spec = the bloom's identity) or a pre-M5 sidecar, "temporal2"
                                            for a pre-M5 sidecar, "sketch" for a sketch; see "Segment kinds" for
                                            kinds a reader does not know)
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
  segments/<blake3>.seg          header{storage_version, encoding_version, key_spec, codec, layout, bloom_hash}
                                 bloom_hash ∈ {"canonical", "parquet_plain"}; readers refuse any other value
                                 footer index_offset u64 | index_len u64 | checksum u64 | magic; every block,
                                           filter and value record a read touches has its own checksum
                                           (see "Segments, storage version 2")
                                 exact:    block index of sorted key blocks, one split-block bloom per block,
                                           key → value record: bitmap_len u32 | bitmap | [counts] | [bounds];
                                           bitmaps: roaring, over global RowGroupId, always in the no-run serialized
                                           form (cookie 3a 30 00 00), never empty; readers refuse any other cookie or
                                           an empty keyed bitmap; counts (spec `counts`): one LEB128 row count per set
                                           bit; bounds (spec `bounds`, `exact:symbol@ts`): one min i64 | max i64 per
                                           set bit, the key's time range there, or the all-null sentinel
                                           (i64::MAX, i64::MIN) where its times are all null
                                 header also records each key column's dataset type (and the temporal ts type
                                           and unit, and the bounds or bitemporal time types); segments with
                                           differing recorded types never merge
                                 index records the row-group range the writer processed (inclusive; a merge
                                           records the hull of its inputs'), so a segment whose row groups held
                                           no keys still records them and two segments over different row groups
                                           never share bytes or an id; a writer not told records the range its
                                           keys reference
                                 bloom:    one split-block bloom per row group (the shape Parquet embeds),
                                           header field bloom_hash names the hash input; newtypes keep the two apart
                                 range:    (min, max) canonical bytes per row group
                                 temporal: exact layout plus key → [(ts, rowgroup_id)] chain + max_ts after the
                                           counts; latest/asof return every row group at the winning ts (ties)
                                 bitemporal (temporal2:id@valid,known): exact layout whose value record is
                                           bitmap_len u32 | bitmap | count u32 | count x (valid i64 | known i64 | rg u32)
                                 bounds:   a bloom's per-key bounds (bloom:id@0.01@ts): the keyed layout under the
                                           bloom spec, value record bitmap_len u32 | bitmap | bounds, checksummed like
                                           every keyed record, listed with kind "bounds" under the bloom's identity;
                                           pre-M5 indexes also hold bounds and temporal2 sidecars in the version 1
                                           sidecar layout (magic "BWSIDECR"); see "Sidecar kinds"
                                 sketch:   sidecar segment, its own layout (magic "BWSKETCH"); approximate, never
                                           read by a planner; see "Sketch segments"
                                 text:     the exact layout keyed by tokens, or with bloom=<fpr> the bloom layout
                                           over tokens; header also records text_unicode; see "Text keys"
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

**Manifest column types (amendment, 2026-10-24).** In a storage version 1 manifest, and in a storage version 2 manifest written before M5, `schema.columns[].data_type` is Arrow's `Display` form of the type, which is Arrow-version dependent (contract v3, entry 9). From M5 a build writes it as core's `ColumnType` JSON (`{"type":"int64"}`, `{"type":"timestamp","unit":"microsecond","tz":"UTC"}`, `{"type":"decimal128","precision":10,"scale":2}`), or `{"arrow": "<display>"}` for a type without one (a nested or dictionary column), so a manifest no longer depends on an Arrow version's spelling; a reader accepts both forms, and the first commit with work by an M5 binary rewrites the string form. The rest of this paragraph describes the string form. The dependency upgrade to Arrow 59 changed the form of time units: Arrow 53 wrote `Timestamp(Microsecond, Some("UTC"))`, `Timestamp(Second, None)`, `Time64(Nanosecond)` and `Duration(Millisecond)`; Arrow 59 writes `Timestamp(µs, "UTC")`, `Timestamp(s)`, `Time64(ns)` and `Duration(ms)`. Every other type an index can record is spelled the same. Types that carry child fields (lists and list views, structs, maps, unions, run-end encoding, and dictionaries of them) were never recordable, because Arrow 53 could not parse its own display form of them. Arrow 59 can. Since the second 2026-10-24 amendment (real-data defect 1) the manifest records them by that spelling like any other column, so a dataset with a nested column is indexed on its flat columns; only a key column needs a canonical encoding, and a nested key column is refused naming the column. A nested column whose child fields carry metadata, which Arrow's spelling drops, does not read back as the same type and is refused naming the column. An Arrow 53 binary cannot read a manifest that records a nested column. A reader accepts both forms: the build crate's manifest reader rewrites an old temporal spelling itself when Arrow does not accept it, and a test reads every Arrow 53 spelling and an index whose manifest carries them. The first commit by a newer binary rewrites the spelling to the current form; the types, the segments, and the row-group ids do not change.

## Key specs

`Exact{columns, counts?, bounds?}`, `Bloom{columns, fpr, bounds?}`, `Range{column}`, `Temporal{columns, ts_column, counts?}`, `Bitemporal{columns, valid_column, known_column}` (storage version 2, M5; serialized with `"kind": "bitemporal"` and declared `temporal2:<columns>@<valid_column>,<known_column>`), and (phase 5) `Expr{name, expr, over, conformance}` ([Expression keys](#expression-keys-expr-phase-5)), and (phase 5) `Text{column, tokenizer, bloom?}` (see "Text keys"), and (phase 5) `List{column}` (B4; see "List keys"), and (phase 5) `Join{dataset, columns, right, right_columns, counts?}` (B3; see "Join keys"), and (phase 5) `Agg{columns, value, functions}` (B7; see "Agg keys"). Composite is not a kind; every kind takes a column list and encodes it memcomparably (ADR 002). CSV and JSON lines sources define row groups as fixed byte-range chunks recorded in the manifest, so the rest of the system never knows the difference. `counts` (storage version 2) is serialized only when true, so a spec without it has the same JSON as in version 1; a version 1 manifest that carries it is refused. `bounds` (a time column, storage version 2 at M5) is serialized only when set: on an exact spec its per-key, per-row-group time range is part of each value record; on a bloom spec, which has no per-key record, it is a `bounds` segment listed under the bloom. A bitemporal spec (`temporal2:id@valid,known`) holds every distinct `(valid, known, row group)` version per key in its own `bitemporal` layout. Key and time columns of one spec are all distinct. Before M5 both were sidecars declared in `schema.sidecars` ("Sidecar kinds").

**Key-spec identity (storage version 2).** `KeySpec::identity()` is the canonical string of every field of a spec, and the key under which the manifest lists its segments (`segments[].key_spec`) and its completeness (`key_spec_completeness`):

| Spec | Identity |
|---|---|
| `Exact{columns: [a, b]}` | `exact:a,b` |
| `Exact{columns: [a], counts: true}` | `exact:a,counts` |
| `Bloom{columns: [a], fpr: 0.01}` | `bloom:a@0.01` (the shortest decimal that reads back as the same `f64`) |
| `Range{column: ts}` | `range:ts` |
| `Temporal{columns: [id], ts_column: ts}` | `temporal:id@ts` |
| `Temporal{columns: [id], ts_column: ts, counts: true}` | `temporal:id@ts,counts` |
| `Exact{columns: [a], bounds: ts}` | `exact:a@ts` (with counts `exact:a@ts,counts`) |
| `Bloom{columns: [a], fpr: 0.01, bounds: ts}` | `bloom:a@0.01@ts` |
| `Bitemporal{columns: [id], valid_column: valid, known_column: known}` | `temporal2:id@valid,known` |
| `Expr{name: sym, expr: json_get(payload,'$.symbol'), over: Exact{counts: true}}` | `expr:sym=json_get(payload,'$.symbol'):exact,counts` (bloom: `...:bloom@0.01`) |
| `Text{column: body, tokenizer: words}` | `text:body,tokenizer=words` |
| `Text{column: body, tokenizer: trigram, bloom: 0.01}` | `text:body,tokenizer=trigram,bloom=0.01` |
| `List{column: tags}` | `list:tags` |
| `Join{dataset: orders, columns: [id], right: payables, right_columns: [order_id]}` | `join:orders.id=payables.order_id` (with `counts: true`: `...,counts`) |
| `Agg{columns: [account], value: amount, functions: [sum, count]}` | `agg:account@amount{sum,count}` (functions in the order sum, count, min, max) |

So `bloom:a@0.01` and `bloom:a@0.05` never share a segment list or a completeness entry. Two specs declared by a version 2 binary never share an identity: `,` and `@` separate its parts, so a new declaration (a key spec given to `index` that the index does not already declare, or a proposal's) naming a key or time column that contains either is refused (`exact:a,counts` could otherwise be `Exact{[a], counts}` or `Exact{["a,counts"]}`). For the same reason, because `,counts` ends a counted spec's identity, a new composite exact spec whose last key column is named `counts` is refused (list that column first). A spec an index already declares is read and indexed as it is, whatever its column names: a storage version 1 index on a column named `a,b` reads, plans, and upgrades like any other. Only two declared specs that actually share an identity are refused, which version 1 allowed only for two temporal specs whose time column holds an `@` (`temporal:id` on `ts@x` and `temporal:id@ts` on `x`, both `temporal:id@ts@x`): see "Reading storage version 1". Errors name a spec by its identity. `KeySpec::name()` is the display name and the spec's slot (`exact:a`, `bloom:a`, `temporal:id`): an index holds one spec per name, and declaring another spec under a declared name (another rate, counts on or off, another time column) rebuilds it and drops the old segments in the same commit. A storage version 1 manifest keyed both maps by name (one spec per name, so the rewrite is one-to-one); a version 2 reader rewrites those keys to identities when it reads one, and a version 1 manifest that no commit could upgrade yet (see "Manifest, storage version 2") is written back in names.

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

The identity of the data a reader saw, cited by a `Replay` (its `inputs`) and by a `Certificate` (its provenance) so both name the same data the same way. One definition, `bellwether_source::catalog`, which the replay crate's `Catalog` also calls for its digest (since M5; its tests check it on an unindexed folder and on a built index): BLAKE3 over the canonical JSON (`serde_json`, no whitespace) of

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

A table is read through the same `Source` trait as a folder of files (`bellwether-source-sql`). Nothing in the index format (storage version 1 or 2) changes: a table never has an index (the engine builds none over a database), so these identities appear only in what a reconcile, `why`, or `fix` over a table records, through the catalog digest above and the source identity below.

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

## Query engine sources: `clickhouse://` and `kusto://` (2026-10-26, ADR 033)

A query engine that owns its storage is reached through a `MeasuredEngine` adapter (`bellwether-source-engine`) and is a `Source` too, so its tables have an identity like a folder or a SQL table. Nothing in storage version 1 changes: an engine never has an index (the engine builds nothing inside it), so these identities and statistics appear only in records, through the fields below. Every field is additive with `skip_serializing_if`: a record written before them keeps its bytes and its hash (`crates/bellwether-source-engine/tests/conformance.rs`).

**Root.** `clickhouse://[user@]host[:port]/<database>[/<table>[,<table>...]]` and `kusto://<cluster>/<database>[/<table>...]`. Parameters follow `?`: `password=env:NAME` (ClickHouse; a secret is read from the environment only), `scratch=<storage folder URL>` (Kusto, where `.export` writes). A recorded root has neither the user nor any parameter: `clickhouse://host:port/db/table`.

**Units.** A unit plays a file of one row group. ClickHouse: one per partition of each MergeTree table, from the active parts in `system.parts`; path `<root>/<table>/<partition_id>`; size the partition's rows; identity string `v1;rows=<n>;blocks=<lo>-<hi>;mutation=<v>` (the lowest and highest block numbers of its parts and the highest mutation version), which an insert, a delete, or a mutation changes and a background merge does not. Kusto: one per extent, from `.show table <t> extents`; path `<root>/<table>/<extent id>`; identity string `v1;rows=<n>;extent=<extent id>` (an extent is immutable; a merge replaces extents, so a merge is seen as a change). In both, `etag` is the identity string and `tail_blake3` its BLAKE3.

**Engine identity.** `Hash::of_record((root, [(unit path, identity string), ...]))` with the units sorted by path, beside the unit count and the rows they hold (`EngineIdentity`). `why --twin-of` records it in the profile Evidence (`twin.identity`); `fix` reads it before scoring and again, on a fresh connection, after the exports, cites both in the Certificate's provenance, and refuses a change between them (`Error::SourceChanged`).

**Tags.** Every query bellwether submits runs under `bw-<run>-<role>`: `<run>` is 16 lowercase hex digits drawn per run (BLAKE3 of the clock, the process id, and a counter; the apply tier draws its own run), `<role>` names the query (`keys` for `workload record`, `p<i>-before` and `p<i>-after` for pair `i`). ASCII letters, digits, and `-` only, at most 96 bytes, so a tag is safe inside any engine's string literal. The engine logs it with the query (ClickHouse `query_id` = the tag; Kusto `ClientRequestId` = `bellwether;<tag>`) and statistics are read back by it, so a number in a record is always one the engine logged for a query bellwether submitted.

**Record fields.**

| Field | Meaning |
|---|---|
| `EvidenceBody::Profile.twin` (`TwinOf`) | `engine` (recorded root), `identity`, `units`, `engine_rows`, the lake's `sequence` and `lake_rows`, the key sample's `workload`, `keys`, the twin's prediction (`predicted_rows`, `predicted_row_groups`, `predicted_files`: the lake's rows, row groups, and files holding a sampled key, exact), and the `gates` compared against |
| `RewriteShape.pairs` / `.key` / `.keys` | the (before, after) query texts, the result columns each pair is reconciled by, the key sample's workload |
| `RunCost.measured` (`QueryStats`) | `tag`, `read_rows`, `read_bytes`, `peak_memory_bytes`, `cpu_ns?`, `duration_ns`, `result_rows`, `units_read?`, `units_total?`, as the engine's own log records them; such a cost is never `exact`: from record version 3 its `exactness` is `measured` with the model (`clickhouse.query_log`, `kusto.query_log`), which its Replay names too, and its `rows` is absent (the log's `read_rows` are rows read, not rows holding the keys) |
| `Replay.prediction`, `Receipt.prediction` (`Prediction`) | the twin Evidence, `predicted_rows`, `predicted_row_groups`, and `pairs`: one `PairPrediction` per pair in the proposal's order, each with the engine's `before_read_rows`, `after_read_rows`, `before_read_bytes`, `after_read_bytes` for that pair's queries and `before_ratio`, `after_ratio` = that pair's read rows / predicted rows as a decimal string with three places, rounded half to even, absent when nothing was predicted. Nothing is summed across pairs |
| `Receipt.attest_only` | `true`: the engine scored and verified and applied nothing (`manifest_after == manifest_before`); written only for a rewrite whose current costs are measured |

The ledger's rules (ADR 033 amendment of ADR 005): a Replay may carry inexact current costs only for a rewrite and only with its model named; a cost carrying `measured` is never `exact`; a receipt over a Replay with measured costs must be `attest_only`, leave the manifest where it was, and copy the Replay's `prediction` unchanged.

**Conformance vectors** (`crates/bellwether-source-engine/tests/conformance.rs` checks each):

| Vector | Value |
|---|---|
| tag, run `0123456789abcdef`, role `p0-before` | `bw-0123456789abcdef-p0-before` |
| ratio 1600 / 2, 400 / 1600, 2 / 3, 1 / 2000, 3 / 2000, 7 / 2000, 9 / 0 | `800.000`, `0.250`, `0.667`, `0.000`, `0.002`, `0.004`, absent |
| `tail_blake3` of `v1;rows=1000;blocks=1-3;mutation=0` | `97ddb63656c5d7e616b9f7d953d66097a154551af091837730c0ef76563f58c7` |
| identity of `clickhouse://h:8123/db/orders` with units `0` (`v1;rows=500;blocks=1-1;mutation=0`) and `1` (`v1;rows=1000;blocks=2-4;mutation=0`), listed `1` first (units sort by path, not by identity string) | canonical bytes `["clickhouse://h:8123/db/orders",[["clickhouse://h:8123/db/orders/0","v1;rows=500;blocks=1-1;mutation=0"],["clickhouse://h:8123/db/orders/1","v1;rows=1000;blocks=2-4;mutation=0"]]]`, hash `2dfe8d56adbc1ecb0e1c9f5ebd4bcc91de6dedde8eeafe233aef35fc6c0aa8b1` |
| a record version 3 `RunCost` with `measured` (run and tag `bw-0123456789abcdef-p0-after`, 2 keys, 1 file, 1 row group, 16,000 bytes, `exactness` measured by `clickhouse.query_log`; 400 rows read, 1,048,576 peak, 1.5 ms CPU, 2 ms, 2 result rows, 1 of 4 units) | hash `7487563e98460bee3490ac37be52a0849914e7503dc85e25372ad968f3688c01` |
| a record version 3 attest-only Receipt (evidence, proposal, replay, certificate `e…`, `f…`, `a…`, `c…`; manifest `0` to `0`; prediction 2 rows in 1 row group; pair 0 read 1,600 then 400 rows, 64,000 then 16,000 bytes; pair 1 read 5 then 3 rows, 200 then 100 bytes) | hash `86c7ffc92a990bf9235e23ea6a8dc21b48a4357279d2d2e8214890e089c69c34` |

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

## Segments, storage version 2

A segment is one file, `segments/<blake3 of its bytes>.seg`, readable from this section alone. Every integer is little-endian and every offset counts from the start of the file. `bytes` is a length-prefixed byte string, `len u32 | len bytes`. A checksum is XXH3-64, seed 0, stored as a `u64`. A key is the canonical encoding of its tuple ("Canonical encoding"), and keys compare as bytes.

```
segment             preamble | header | data | index | footer
preamble            magic "BWSEGMNT" | storage_version u32 | encoding_version u32 | header_len u32
header              header_len bytes of JSON: {storage_version, encoding_version, key_spec, codec, layout,
                    bloom_hash, key_types?, ts_type?, time_types?, value_type?, ..unknown fields, preserved}
data                the layout's blocks, at the offsets the index gives
index               entries u64 | has_rg u8 | rg_min u32 | rg_max u32 | block_count u32 |
                    block_count x index entry (of the layout)
footer              index_offset u64 | index_len u64 | checksum u64 | magic "BWSEGMNT"
                    (checksum over preamble ‖ header ‖ index)
```

- **Header.** `storage_version` and `encoding_version` equal the preamble's. `codec` is `"none"`. `layout` follows from the key spec's kind: `keyed` (exact, with or without bounds), `keyed_temporal` (temporal), `bitemporal` (bitemporal), `keyed_agg` (agg), `bloom_per_row_group` (bloom), `min_max` (range), `keyed_join` (join, "Join keys"); the one other pairing is `keyed` under a bloom spec with bounds (`bloom:a@0.01@ts`), which is that bloom's `bounds` segment (listed with `kind: "bounds"`, see "Bounds" below); any other pairing is refused. `bloom_hash` is `canonical`, or `parquet_plain` for a bloom segment holding a reused single-column Parquet filter (ADR 008). `key_types` (one per key column) and, for a temporal spec, `ts_type` are each key column's dataset type in core's `ColumnType` spelling, present together when the segment was written from a dataset schema; with them, an exact spec with bounds (and a bloom's `bounds` segment) records its bounds column's type and a bitemporal spec its valid and known columns' types in `time_types`, in that order. A storage version 1 header whose spec has bounds or is bitemporal is refused. `value_type` is present exactly on an agg segment written from a dataset schema, beside `key_types`: the value column's dataset type, which the cells were encoded and summed at (a decimal's scale is not visible in the bytes); a reader whose dataset type differs refuses the segment on open.
- **Index header.** `entries` counts keys (keyed layouts), range entries (`min_max`) or the sum of the filters' distinct-value estimates (`bloom_per_row_group`). `rg_min..=rg_max` is the row-group range the writer processed, inclusive (see the tree above); `has_rg` is 0, with both bounds 0, only for a segment that recorded no range.

The layouts:

```
keyed, keyed_temporal, bitemporal, keyed_agg (exact, temporal, bitemporal, agg specs; keyed also for a bloom's bounds segment)
  block             value records | bloom | directory, in that order; at most 1024 keys or 64 KiB of
                    key bytes per block; keys strictly ascending across the segment
  index entry       first_key bytes | last_key bytes | bloom_off u64 | bloom_len u32 | bloom_checksum u64 |
                    body_off u64 | body_len u32 | body_checksum u64 | n u32
  bloom             a split-block bloom filter (Parquet's layout) over the block's keys, hashed canonically
  directory (body)  n u32 | (n+1) key end offsets u32, the first 0 | key bytes |
                    n x (value_offset u64 | value_len u32 | value_checksum u64)
  value record      bitmap_len u32 | bitmap | [counts] | [bounds] | [chain] | [versions] | [cells]
  bitmap            roaring over global RowGroupId, in the no-run serialized form (cookie 3a 30 00 00),
                    never empty
  counts            spec has counts: one unsigned LEB128 u64 per set bit, in ascending row-group order
  bounds            exact spec with bounds, or a bloom's bounds segment: one (min i64 | max i64) per set bit, in ascending row-group
                    order: the bounds column's range over the key's rows in that row group, min <= max;
                    or the all-null sentinel (min = i64::MAX, max = i64::MIN: ff ff ff ff ff ff ff 7f
                    00 00 00 00 00 00 00 80) where every such row has a null time
  chain             temporal only: count u32 | has_max u8 | max_ts i64 | count x (ts i64 | rg u32);
                    the distinct (ts, row group) pairs of the key's non-null times, strictly ascending;
                    has_max = 1 and max_ts = the last ts when count > 0, else 0 and 0
  versions          bitemporal only: count u32 | count x (valid i64 | known i64 | rg u32); the key's
                    distinct versions over rows where neither time is null, strictly ascending by
                    (valid, known, rg), count >= 1, and their row groups are exactly the bitmap's;
                    valid is in the valid column's dataset unit and known in the known column's
                    (each as bounds take theirs: a timestamp in its unit, a Date32 in days, a
                    Date64 in milliseconds, an integer as stored), so the two clocks may differ
  cells             keyed_agg only: one cell per set bit, in ascending row-group order (see "Agg keys")

bloom_per_row_group (bloom specs)
  index entry       rg u32 | off u64 | len u32 | ndv u64 | checksum u64, rg non-decreasing (a lookup
                    unions every entry of one row group)
  data              that row group's split-block bloom filter, len bytes at off

min_max (range specs)
  block             n u32 | n x (rg u32 | min bytes | max bytes), ascending by (min, max, rg); at most
                    256 entries per block
  index entry       first_min bytes | max_of_max bytes | off u64 | len u32 | n u32 | checksum u64
```

- **Checksums.** A reader checks the footer's checksum when it opens a segment, and the checksum of every filter, directory and value record before it decodes one. A mismatch is refused as a corrupt segment (`corrupt segment: …`, the corrupt-index class), never decoded into an answer. A lookup still holds the index, one block and the values of the keys asked (ADR 018).
- **Counts** are present exactly when the header's `key_spec` has `counts: true`. Each is the number of rows in that row group whose key tuple is this key: at least 1, in its shortest LEB128 form (an over-long form or one past 64 bits is refused), and nothing may follow the last one (or the chain). Rows whose leading key column is null are not indexed and not counted; a trailing null element of a composite key is the `0x00` marker and counts under that tuple (ADR 014). A temporal count is the key's rows in the row group over all versions, including rows with a null time, which never win. `SegmentReader::counts(keys)` returns `None` for a segment without counts, never zero.
- **Merge** sums the counts of a (key, row group) only when the inputs' row groups are disjoint, as the build's contiguous groups are, and refuses inputs that both count one key in one row group. Its output is written at this build's storage version. A read that finds two listed segments of one counted spec both counting one (key, row group) refuses the index as corrupt rather than summing them.
- A segment header at storage version 1 that declares `counts`, `bounds` or a bitemporal spec is refused.
- **Bounds** are present exactly when the header's `key_spec` is an exact spec with `bounds`, or a bloom spec with `bounds` in the `keyed` layout. The latter is the bloom's `bounds` segment: a bloom has no per-key record, so its per-key bounds are a second segment of the bloom whose value records are `bitmap | bounds` (the exact spec's record without counts), checksummed like every keyed record. The manifest lists it with `kind: "bounds"` and the bloom's identity as `key_spec`, and the bloom is complete when its filters and its bounds both cover every live row group. Merge never merges a bloom's filters with its bounds. Times are `i64` in the bounds column's dataset unit (a timestamp in its unit, a `Date32` in days, a `Date64` in milliseconds), as temporal specs take them. A window lookup `(keys, [lo, hi])` keeps a row group for a key only when the key's own `[min, max]` there overlaps `[lo, hi]`; the sentinel overlaps no window (not even one ending at `i64::MAX`), because a row group where the key's times are all null holds no row of any window. `SegmentReader::bounds(keys)` returns the non-sentinel ranges and `None` for a segment without bounds; `SegmentReader::get` returns the stored record, sentinel included. A bounds record must be one pair per set bit, each `min <= max` or the sentinel; anything else is refused. **Merge** widens one (key, row group)'s bounds across inputs, `[min(a, c), max(b, d)]`, so the sentinel absorbs into any real range and stays only where every input's times are all null; it unites bitemporal versions and deduplicates them. Counts still refuse an overlapping (key, row group).
- **Bitemporal.** The as-of answer for `(V, K)`, either optional, is per key the greatest `valid <= V` among versions with `known <= K`, then the greatest `known` at that valid time; every row group holding that pair is planned (ties are kept), and retired row groups never win. The residual is that of the version 1 `temporal2` sidecar ("Sidecar kinds"). A row whose valid or known time is null is not a version (ADR 014); a key with no version is not stored.
- **Frozen (M5, ADR 028).** These are the bytes of storage version 2. The first storage version 2 release kept per-key bounds and bitemporal versions in the version 1 sidecar segments of "Sidecar kinds", declared in `schema.sidecars`, and refused the folded shapes by name (`Error::UnsupportedShape`, since removed); a binary of that release given an M5 index still refuses it with that error. This binary reads an index of that release as written ("Sidecar kinds": its sidecars plan as before) and folds them on its first build with work: each `bounds:<keys>@<ts>` sidecar becomes the bounds of the exact spec on those keys without bounds, else of the bloom spec on them without bounds, else a new `exact:<keys>@<ts>`; each `temporal2` sidecar becomes a bitemporal key spec; the folded specs are rebuilt in the folded layouts, the sidecar declarations and segments are dropped, and `bellwether index` lists each fold (`sidecars_folded`). A sidecar none of these fits is never discarded: a `temporal2` sidecar whose bitemporal spec name is taken by one on other clocks (`temporal2:sym@ts,known` beside `temporal2:sym@valid,known`), or a `bounds` sidecar whose exact spec already keeps bounds on another column (`bounds:sym@valid` beside `exact:sym@ts`), stays declared as a sidecar with its segments, is built like any target and still plans; `bellwether index` lists it (`kept_legacy`). A build with nothing to do writes nothing, and `bellwether index` names the folds it would make (`fold_pending`).

### Roaring bitmaps

The bitmap of a keyed value record (exact and temporal layouts) is a roaring bitmap of row-group ids in the portable serialization of the [Roaring format specification](https://github.com/RoaringBitmap/RoaringFormatSpec), in its form without run containers, as the `roaring` crate 0.10 (0.10.12 in `Cargo.lock`) writes it. Everything a reader needs is here; the specification is the reference. All integers are little-endian.

```
bitmap        cookie u32 = 12346 (3a 30 00 00) | n u32, the number of containers, at least 1 |
              n x (key u16 | cardinality-1 u16) | n x offset u32 | the n containers, in that order
key           the high 16 bits of the ids the container holds (id / 65,536); strictly ascending
cardinality   how many ids the container holds, 1 to 65,536; stored minus one
offset        where the container starts, in bytes from the cookie's first byte
container     cardinality <= 4,096: an array container, cardinality x u16, the low 16 bits of
              each id (id mod 65,536), strictly ascending: 2 x cardinality bytes
              cardinality > 4,096: a bitmap container, 1,024 x u64 (8,192 bytes): the id
              key x 65,536 + i is present exactly when bit (i mod 64) of word (i / 64) is set
```

So a key held in at most 4,096 row groups of one 65,536-wide range of ids has an array container for that range, and a key held in more has a bitmap container; a key held in row groups of several ranges has one container per range. The bitmap's length is its `bitmap_len`; in a counts record, the counts follow it in ascending id order, one per id, across all containers. Any other cookie (a run-container serialization, `3b 30`) is refused.

**Roaring bitmap vectors** (`crates/bellwether-core/tests/counts_conformance.rs` writes a counted exact segment holding `"BIG"` in each of row groups 0 to 4,999 and in 65,536 and 70,000, one row each, finds this bitmap in it right after its `bitmap_len`, decodes it with the rules above alone, and checks that this table lists exactly these vectors):

| Vector | Bytes |
|---|---|
| `"BIG"` bitmap: cookie, 2 containers, (key 0, 5,000 ids), (key 1, 2 ids), offsets 24 and 8,216 | `3a 30 00 00 02 00 00 00 00 00 87 13 01 00 01 00 18 00 00 00 18 20 00 00` |
| `"BIG"` container 0 (bitmap, ids 0 to 4,999) | 625 bytes `ff`, then 7,567 bytes `00` |
| `"BIG"` container 1 (array, ids 65,536 and 70,000) | `00 00 70 11` |

### Worked segment

The counted exact segment of "Counts conformance vectors", every byte of it. Offsets are hexadecimal, values decimal. `crates/bellwether-core/tests/counts_conformance.rs` checks that the bytes of these lines, in order, are the segment and that each offset is the count of bytes before it. The header is

```
{"storage_version":2,"encoding_version":1,"key_spec":{"kind":"exact","columns":["symbol"],"counts":true},"codec":"none","layout":"keyed","bloom_hash":"canonical"}
```

```text
0000 | 42 57 53 45 47 4d 4e 54                         | preamble  magic "BWSEGMNT"
0008 | 02 00 00 00                                     |           storage_version u32 = 2
000c | 01 00 00 00                                     |           encoding_version u32 = 1
0010 | a2 00 00 00                                     |           header_len u32 = 162
0014 | 7b 22 73 74 6f 72 61 67 65 5f 76 65 72 73 69 6f | header    JSON (header_len bytes), shown below
0024 | 6e 22 3a 32 2c 22 65 6e 63 6f 64 69 6e 67 5f 76 |
0034 | 65 72 73 69 6f 6e 22 3a 31 2c 22 6b 65 79 5f 73 |
0044 | 70 65 63 22 3a 7b 22 6b 69 6e 64 22 3a 22 65 78 |
0054 | 61 63 74 22 2c 22 63 6f 6c 75 6d 6e 73 22 3a 5b |
0064 | 22 73 79 6d 62 6f 6c 22 5d 2c 22 63 6f 75 6e 74 |
0074 | 73 22 3a 74 72 75 65 7d 2c 22 63 6f 64 65 63 22 |
0084 | 3a 22 6e 6f 6e 65 22 2c 22 6c 61 79 6f 75 74 22 |
0094 | 3a 22 6b 65 79 65 64 22 2c 22 62 6c 6f 6f 6d 5f |
00a4 | 68 61 73 68 22 3a 22 63 61 6e 6f 6e 69 63 61 6c |
00b4 | 22 7d                                           |
00b6 | 14 00 00 00                                     | value "AAPL": bitmap_len u32 = 20
00ba | 3a 30 00 00                                     |   bitmap: no-run cookie 3a 30 00 00
00be | 01 00 00 00                                     |   bitmap: containers u32 = 1
00c2 | 00 00 01 00                                     |   bitmap: container key u16 = 0, cardinality-1 u16 = 1
00c6 | 10 00 00 00                                     |   bitmap: container offset u32 = 16
00ca | 00 00 02 00                                     |   bitmap: row groups u16 = 0, 2
00ce | 03 01                                           |   counts: LEB128 = 3, 1
00d0 | 12 00 00 00                                     | value "MSFT": bitmap_len u32 = 18
00d4 | 3a 30 00 00                                     |   bitmap: no-run cookie 3a 30 00 00
00d8 | 01 00 00 00                                     |   bitmap: containers u32 = 1
00dc | 00 00 00 00                                     |   bitmap: container key u16 = 0, cardinality-1 u16 = 0
00e0 | 10 00 00 00                                     |   bitmap: container offset u32 = 16
00e4 | 01 00                                           |   bitmap: row groups u16 = 1
00e6 | ac 02                                           |   counts: LEB128 = 300
00e8 | 84 00 00 00 00 08 00 04 20 00 04 00 00 00 08 04 | block bloom: split-block filter over the block's keys (32 bytes)
00f8 | 00 40 00 80 02 10 00 00 00 10 00 80 04 01 00 00 |
0108 | 02 00 00 00                                     | directory n u32 = 2
010c | 00 00 00 00                                     |   key end offset u32 = 0
0110 | 07 00 00 00                                     |   key end offset u32 = 7
0114 | 0e 00 00 00                                     |   key end offset u32 = 14
0118 | 06 41 41 50 4c 00 00                            |   key "AAPL" (canonical encoding)
011f | 06 4d 53 46 54 00 00                            |   key "MSFT"
0126 | b6 00 00 00 00 00 00 00                         |   "AAPL" value_offset u64 = 182
012e | 1a 00 00 00                                     |   "AAPL" value_len u32 = 26
0132 | 5a 72 54 b2 31 02 a3 68                         |   "AAPL" value_checksum u64 = 0x68a30231b254725a
013a | d0 00 00 00 00 00 00 00                         |   "MSFT" value_offset u64 = 208
0142 | 18 00 00 00                                     |   "MSFT" value_len u32 = 24
0146 | ad 3f e7 2f a9 b9 bd 34                         |   "MSFT" value_checksum u64 = 0x34bdb9a92fe73fad
014e | 02 00 00 00 00 00 00 00                         | index     entries u64 = 2 (keys)
0156 | 01                                              |           has_rg u8 = 1
0157 | 00 00 00 00                                     |           rg_min u32 = 0
015b | 03 00 00 00                                     |           rg_max u32 = 3 (processed range 0..4, inclusive)
015f | 01 00 00 00                                     |           block_count u32 = 1
0163 | 07 00 00 00                                     |   block 0 first_key_len u32 = 7
0167 | 06 41 41 50 4c 00 00                            |   block 0 first_key "AAPL"
016e | 07 00 00 00                                     |   block 0 last_key_len u32 = 7
0172 | 06 4d 53 46 54 00 00                            |   block 0 last_key "MSFT"
0179 | e8 00 00 00 00 00 00 00                         |   block 0 bloom_off u64 = 232
0181 | 20 00 00 00                                     |   block 0 bloom_len u32 = 32
0185 | b4 5e 6b a1 66 72 e2 c7                         |   block 0 bloom_checksum u64 = 0xc7e27266a16b5eb4
018d | 08 01 00 00 00 00 00 00                         |   block 0 body_off u64 = 264 (the directory)
0195 | 46 00 00 00                                     |   block 0 body_len u32 = 70
0199 | 57 11 11 ff 08 67 f0 65                         |   block 0 body_checksum u64 = 0x65f06708ff111157
01a1 | 02 00 00 00                                     |   block 0 n u32 = 2
01a5 | 4e 01 00 00 00 00 00 00                         | footer    index_offset u64 = 334
01ad | 57 00 00 00 00 00 00 00                         |           index_len u64 = 87
01b5 | 73 28 5a 4f f8 8d fd e4                         |           checksum u64 = 0xe4fd8df84f5a2873 (preamble ‖ header ‖ index)
01bd | 42 57 53 45 47 4d 4e 54                         |           magic "BWSEGMNT"
```

The footer's checksum is XXH3-64 over bytes `0000..00b6` (preamble and header) followed by `014e..01a5` (the index); each other checksum covers exactly the bytes its offset and length name.

## Manifest, storage version 2

Each `rowgroup_space.files[]` entry gains three arrays:

- `rowgroup_rows`: rows of each row group, aligned with `rowgroup_ids`.
- `rowgroup_bytes`: compressed bytes of each row group (a text chunk's length), aligned with `rowgroup_ids`.
- `column_bytes`: compressed bytes of each column chunk, row-group-major: entry `i * columns + j` is row group `i`'s chunk of schema column `j`. It is 0 for a virtual partition column, and the whole array is absent for a text source, whose chunks have no per-column bytes.

A file whose arrays are present but do not align with its row groups or the schema's columns is refused. The build fills them for every file it adds. The first build with work by a version 2 binary over a version 1 manifest fills them for every tracked file from its footer (one footer read per file; a footer whose row-group count disagrees with the manifest is drift, ADR 013) and stamps `storage_version: 2`. A commit that fills nothing (compaction, garbage collection, removing a spec) over a version 1 manifest keeps it at version 1, in version 1's keys, so the arrays are absent only in a version 1 manifest. No commit ever lowers the version. A plan over a manifest with the arrays opens no Parquet footer: bytes come from `rowgroup_bytes`, and `locate --columns` sums `column_bytes` into each entry's `columns` and the plan's `projected_bytes`. Over a version 1 manifest (`--at-manifest` included), `locate --columns` reads each planned file's footer instead and says so ("manifest sequence N is a storage version 1 manifest, which records no per-column bytes, so the projected bytes were read from N Parquet footers"; `column_bytes_from_footers` in JSON). Counts (`count`, and the rows a plan through a counted spec reports) come from the segments.

**Size.** The arrays cost about 8 bytes of decimal text per (row group, column). An index over 10^5 row groups of 50 columns carries a manifest of about 40 MB; beyond that, externalized statistics are a storage version 3 question, not solved here.

**Column types (M5).** `schema.columns[].data_type` is core's `ColumnType` JSON, the spelling a segment header records (`{"type":"utf8"}`, `{"type":"timestamp","unit":"microsecond","tz":"UTC"}`, `{"type":"decimal128","precision":10,"scale":2}`, `{"type":"date32"}`), or `{"arrow": "<Arrow display>"}` for a type `ColumnType` has no spelling for or whose spelling reads back as another type: a nested column (never a key) or a dictionary column (a segment header records a dictionary key column as its value type, since the encoding sees only decoded values). A reader also accepts the storage version 1 string (Arrow's display form; see "Manifest column types"), which a manifest written before M5 carries; a build with work writes the JSON form. A new index infers each column's type over every file; a decimal column of different scales takes the widest precision and scale.

Every `ColumnType` spelling, one per variant (a timestamp's `tz` is absent without a zone, and is the zone string the dataset declares, an offset or an IANA name, otherwise); `crates/bellwether-core/tests/column_type_spellings.rs` checks that each Arrow type serializes to its row and reads back from it, and that this table lists exactly these:

| Arrow type | `ColumnType` JSON |
|---|---|
| `Boolean` | `{"type":"boolean"}` |
| `Int8` | `{"type":"int8"}` |
| `Int16` | `{"type":"int16"}` |
| `Int32` | `{"type":"int32"}` |
| `Int64` | `{"type":"int64"}` |
| `UInt8` | `{"type":"uint8"}` |
| `UInt16` | `{"type":"uint16"}` |
| `UInt32` | `{"type":"uint32"}` |
| `UInt64` | `{"type":"uint64"}` |
| `Float16` | `{"type":"float16"}` |
| `Float32` | `{"type":"float32"}` |
| `Float64` | `{"type":"float64"}` |
| `Decimal128(10, 2)` | `{"type":"decimal128","precision":10,"scale":2}` |
| `Decimal256(40, -2)` | `{"type":"decimal256","precision":40,"scale":-2}` |
| `Utf8` | `{"type":"utf8"}` |
| `LargeUtf8` | `{"type":"large_utf8"}` |
| `Utf8View` | `{"type":"utf8_view"}` |
| `Binary` | `{"type":"binary"}` |
| `LargeBinary` | `{"type":"large_binary"}` |
| `BinaryView` | `{"type":"binary_view"}` |
| `FixedSizeBinary(16)` | `{"type":"fixed_size_binary","size":16}` |
| `Date32` | `{"type":"date32"}` |
| `Date64` | `{"type":"date64"}` |
| `Time32(Second)` | `{"type":"time32","unit":"second"}` |
| `Time32(Millisecond)` | `{"type":"time32","unit":"millisecond"}` |
| `Time64(Microsecond)` | `{"type":"time64","unit":"microsecond"}` |
| `Time64(Nanosecond)` | `{"type":"time64","unit":"nanosecond"}` |
| `Timestamp(Second, None)` | `{"type":"timestamp","unit":"second"}` |
| `Timestamp(Millisecond, "UTC")` | `{"type":"timestamp","unit":"millisecond","tz":"UTC"}` |
| `Timestamp(Microsecond, "+01:00")` | `{"type":"timestamp","unit":"microsecond","tz":"+01:00"}` |
| `Timestamp(Nanosecond, "America/New_York")` | `{"type":"timestamp","unit":"nanosecond","tz":"America/New_York"}` |

The `{"arrow": …}` form is Arrow's display string, which depends on the Arrow version (the arrow-59 upgrade changed the time units'). `fixtures/format-v0.1/index-nested/` pins the spellings an Arrow 59 build writes, and every later binary must read them: `{"arrow": "List(Utf8, field: 'element')"}`, `{"arrow": "Struct(\"venue\": Utf8, \"lot\": Int32)"}`, `{"arrow": "Map(\"key_value\": non-null Struct(\"key\": non-null Utf8, \"value\": Int64), unsorted)"}` and `{"arrow": "Dictionary(Int32, Utf8)"}` (`crates/bellwether-cli/tests/format_compat.rs`).

**Source dialect (M5).** A CSV source records its dialect as `source.dialect: {delimiter, header, quote}`, the delimiter and quote as byte values (`{"delimiter": 44, "header": true, "quote": 34}` for a comma, a header row and `"`), beside `source.kind: "csv"`. A storage version 1 manifest spelled a dialect other than comma-with-header inside `kind` (`csv;delimiter=<byte>;header=<bool>`), which is still read; a build with nothing else to do does not respell it. A dialect beside a kind that has none, or a quote other than `"`, is refused.

The minimal version 2 manifest, in the canonical form `Manifest::to_vec` writes (sorted keys, two-space indent). `crates/bellwether-store/tests/manifest_v2.rs` reads it, writes it back byte for byte, checks that this section shows it exactly, and sweeps every byte of it:

```json
{
  "commit_nonce": 1,
  "committed_at": "2026-10-25T00:00:00.000Z",
  "created_at": "2026-10-25T00:00:00.000Z",
  "deleting": [],
  "key_spec_completeness": {
    "exact:symbol,counts": 1
  },
  "rowgroup_space": {
    "files": [
      {
        "column_bytes": [
          50,
          0,
          30,
          0
        ],
        "fingerprint": {
          "etag": "2026-10-25T00:00:00.000000000Z",
          "path": "/data/day=2026-01-01/part-0.parquet",
          "size": 1000,
          "tail_blake3": "0000000000000000000000000000000000000000000000000000000000000000"
        },
        "rowgroup_bytes": [
          60,
          40
        ],
        "rowgroup_ids": [
          0,
          1
        ],
        "rowgroup_rows": [
          4,
          2
        ]
      }
    ],
    "next_id": 2,
    "tombstones": []
  },
  "schema": {
    "columns": [
      {
        "data_type": {
          "type": "utf8"
        },
        "name": "symbol",
        "nullable": true
      },
      {
        "data_type": {
          "type": "date32"
        },
        "name": "day",
        "nullable": true
      }
    ],
    "key_specs": [
      {
        "columns": [
          "symbol"
        ],
        "counts": true,
        "kind": "exact"
      }
    ]
  },
  "segment_tombstones": [],
  "segments": [
    {
      "bytes": 300,
      "id": "1111111111111111111111111111111111111111111111111111111111111111",
      "key_spec": "exact:symbol,counts",
      "rowgroup_range": {
        "end": 2,
        "start": 0
      }
    }
  ],
  "sequence": 1,
  "source": {
    "fingerprint_policy": "tail64k",
    "kind": "parquet",
    "partitioning": "hive",
    "root": "/data"
  },
  "storage_version": 2
}
```

## Counts conformance vectors

`crates/bellwether-core/tests/counts_conformance.rs` writes a counted exact segment on `symbol` (row groups 0..4, no recorded types) holding `"AAPL"` in row groups 0 (3 rows) and 2 (1 row) and `"MSFT"` in row group 1 (300 rows). It finds each value record below exactly once in the segment's bytes, followed in the directory by its offset, length and checksum, and checks that this table lists exactly these vectors.

| Vector | Bytes |
|---|---|
| `"AAPL"` key | `06 41 41 50 4c 00 00` |
| exact value `"AAPL"`, row groups {0, 2}, counts [3, 1] | `14 00 00 00 3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 02 00 03 01` |
| exact value `"MSFT"`, row group {1}, counts [300] | `12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00 ac 02` |

The same test checks that every segment in `fixtures/format-v1/`, `fixtures/format-v0.1-pre/` and `fixtures/format-v0.1/` is named by the blake3 of its bytes.

## Folded layout conformance vectors

`crates/bellwether-core/tests/folded_conformance.rs` writes three segments over `symbol` (row groups 0..4, no recorded types) and checks every row of this table byte for byte: it finds each value record exactly once in its segment, followed in the directory by its offset, length and checksum, and checks each segment's blake3.

- `exact:symbol@ts,counts`: `"AAPL"` in row group 0 (three rows, times `1`, `86400000000` and null) and row group 2 (one row, null time, so the all-null sentinel); `"MSFT"` in row group 1 (two rows, times `-1` and `1`).
- the `bounds` segment of `bloom:symbol@0.01@ts`: the same rows; its header is `{"storage_version":2,"encoding_version":1,"key_spec":{"kind":"bloom","columns":["symbol"],"fpr":0.01,"bounds":"ts"},"codec":"none","layout":"keyed","bloom_hash":"canonical"}` and its records are the exact spec's without counts.
- `temporal2:symbol@valid,known`: `"AAPL"` versions `(1, 1)` in row group 0 and `(1, 86400000000)` in row group 2; `"MSFT"` version `(86400000000, 86400000000)` in row group 1.

| Vector | Bytes |
|---|---|
| `"AAPL"` key | `06 41 41 50 4c 00 00` |
| `exact:symbol@ts,counts` value `"AAPL"`: row groups {0, 2}, counts [3, 1], bounds [1, 86400000000] and the all-null sentinel | `14 00 00 00 3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 02 00 03 01 01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00 ff ff ff ff ff ff ff 7f 00 00 00 00 00 00 00 80` |
| `exact:symbol@ts,counts` value `"MSFT"`: row group {1}, counts [2], bounds [-1, 1] | `12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00 02 ff ff ff ff ff ff ff ff 01 00 00 00 00 00 00 00` |
| `bloom:symbol@0.01@ts` bounds segment value `"AAPL"`: row groups {0, 2}, bounds [1, 86400000000] and the all-null sentinel | `14 00 00 00 3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 02 00 01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00 ff ff ff ff ff ff ff 7f 00 00 00 00 00 00 00 80` |
| `bloom:symbol@0.01@ts` bounds segment value `"MSFT"`: row group {1}, bounds [-1, 1] | `12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00 ff ff ff ff ff ff ff ff 01 00 00 00 00 00 00 00` |
| `temporal2:symbol@valid,known` value `"AAPL"`: (1, 1, rg 0), (1, 86400000000, rg 2) | `14 00 00 00 3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 02 00 02 00 00 00 01 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00 02 00 00 00` |
| `temporal2:symbol@valid,known` value `"MSFT"`: (86400000000, 86400000000, rg 1) | `12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00 01 00 00 00 00 60 d7 1d 14 00 00 00 00 60 d7 1d 14 00 00 00 01 00 00 00` |
| `exact:symbol@ts,counts` segment blake3 | `74acef3b4710583ea99596f607094102ef917fbcdae93cf774ec3930c62f41bf` |
| `bloom:symbol@0.01@ts` bounds segment blake3 | `083eb7d2da5c3589bb2a290b634e66346561d8e78fb6135a8906aa8b5b57a775` |
| `temporal2:symbol@valid,known` segment blake3 | `0771c6c1083255732be1d9b1d870cd3c9f24063320be2e87f4d27cdc2d48df0a` |

The same test also checks, over a Parquet file of seven row groups with null keys, null times, the extreme `i64` times and a key whose times are all null in one row group: that each value record the writer builds from the file's row groups equals one computed row by row from the file; that a window plan through the stored bounds is exactly the row groups whose per-key range overlaps the window, never one holding only the sentinel (a window ending at `i64::MAX` included), and so never misses a row in the window; that a build split in two merges back to the same bytes; that merge widens one row group's bounds across inputs, the sentinel absorbing into any real range; and that every byte value at every body position, and every truncation, of each folded layout (a bloom's `bounds` segment included) is refused or reads the clean value, never a panic. `crates/bellwether-cli/tests/bloom_bounds_corruption.rs` rewrites the value records of a built index's bloom `bounds` segment in place, keeping its file name, and checks that a window `locate` is refused as a corrupt index rather than planned.

## Reading storage version 1

- **Segments.** A version 1 segment has the 24-byte footer without a checksum and no other checksums; an exact value record is the bare bitmap and a temporal one the length-prefixed bitmap and the chain. It never stores counts: `SegmentReader::counts` returns `None`, and its value bytes are never read as counts. Sidecar segments (`BWSIDECR`, "Sidecar kinds") are unchanged in version 2; since M5 only an index written before M5 holds them.
- **Manifests.** Keys are rewritten from names to identities on read ("Key-spec identity"). Without the arrays, a plan opens each planned file's footer, as version 1 did. `storage_version` stays 1 until a commit: `bellwether index` with nothing to do commits nothing and leaves the index at version 1, readable by a version 1 binary; the first build with work fills the arrays, commits at version 2 and says so (`manifest upgraded to storage version 2`). Every plan of the version 1 index pinned to its earlier sequences (`--at-manifest`) is unchanged by the upgrade. Its specs are read as they were declared, whatever their column names. Two of them that share an identity (two temporal specs whose time column holds an `@`) are read under `v1-shared-identity:<name>` keys, which no identity equals, so neither plans; every build refuses before reading or writing anything, naming both specs in their JSON form and the step that upgrades the index: re-declare the one whose key columns have no `,` or `@` under its name with another time column (`--key '{"kind":"temporal","columns":["id"],"ts_column":"ts"}'`; a spec declared under an existing name replaces it), or, when both key columns hold one, move the index folder aside and index again. A storage version 2 manifest with two such specs is refused as a corrupt index: no build commits one.

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

A plan's entries are one per file and name their row groups as a set, read in ascending index. When a complete range key on a timestamp or date column exists (several: the one on the lookup's own time column), lookups order entries by the smallest row-group minimum among each entry's planned row groups, as the range key's segments record it (no minimum, every value null: last; ties by path), and the result names the column (`ordered_by`). When reading entries in order, each entry's row groups ascending, visits every planned row group in non-decreasing minimum, the plan has no `order`. Otherwise (two files overlap in time, or a file's own row groups run out of time order) the plan's `order` (M5) lists every planned `(entry, row group)` pair in non-decreasing minimum, interleaving files, so a reader that follows it reads in time order. Without a complete range key the result says the order is unspecified and why. The planned row groups and bytes never depend on the order.

## Manifest history (2026-10-24)

`manifests/<sequence>.json` holds each committed manifest's exact bytes, so a lookup can be planned against an earlier sequence (`--at-manifest N`) and return the plan it returned then, byte for byte. The copy is written with `put_if_absent` after the commit's CAS is known to have succeeded (never for an attempt that lost or whose outcome is unknown); a commit that cannot write it still succeeds, and the next commit backfills the base's copy from `manifest.json`. An existing copy with different bytes is kept and never overwritten (`manifest.json` was rewritten outside the commit protocol); history never blocks a commit. Garbage collection, before deleting any segment, deletes every history manifest at or below the greatest sequence that references a segment it deletes, so the retained sequences are always one contiguous run ending at the current one, and every retained sequence's segments exist. A history object that does not parse, or whose sequence disagrees with its name, is counted and left in place. A pinned lookup refuses a sequence outside the retained run and names the run; an index written before this amendment retains sequences from its first commit by a binary that writes history.

## Segment kinds (2026-10-24)

`segments[].kind` names what a segment is: absent for a key spec's segment, or a sidecar kind: `bounds` (a bloom's per-key bounds, a storage version 2 `keyed` segment listed under the bloom's identity, or a sidecar of an index written before M5), `temporal2` (a sidecar of an index written before M5), `sketch` ("Sketch segments"). A reader that finds a `kind` it does not know, in `segments[]` or in `schema.sidecars[]`, still opens the manifest; it never plans with, merges, or counts toward coverage a segment of that kind, a build keeps the segment and the declaration unchanged, and the unknown kinds are reported (`bellwether index` lists them). This is the rule that lets a later version add a sidecar kind without a storage version bump. A binary that predates the field keeps both as unknown fields; it never plans with a sidecar's segments, because their `key_spec` is the sidecar's name, which no key spec has, and a build by such a binary drops them as undeclared, after which the sidecar is incomplete until rebuilt.

## Sidecar kinds (storage version 1, additive sidecar kinds, 2026-10-24)

**Since M5 no build declares a `bounds` or `temporal2` sidecar**: per-key bounds are part of an exact spec's value record or a bloom's `bounds` segment, and bitemporal versions are a key spec of their own ("Segments, storage version 2"). This section describes the sidecars an index written before M5 holds, which this binary reads and plans with as written until its first build with work folds them (a sidecar no key spec can take stays a sidecar, "Segments, storage version 2", Frozen), and their `BWSIDECR` layout. A bloom's `bounds` segment is not in this layout: it is a storage version 2 `keyed` segment with a checksum on every record.

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

### Sidecar proposal vectors

**Recorded lookback runs (a limit of record version 3).** A workload run's `lookback` is a length in the unit of the column it was measured on, and the run does not record that column: replay takes it to be the column the current configuration plans the run on (`Replayer::planned_time_column`), and converts the window outward (the lower bound rounded down, the upper bound up) to a candidate's column when the candidate plans on another, in replay and in `fix`'s verify alike. A run whose current configuration plans it on no time column is measured on the candidate's column. Recording the column is a new field, so it waits for record version 4.

Record version 3 proposes the specs storage version 2 declares since M5 (ADR 028): a key whose exact spec carries per-key `bounds`, a bitemporal key, and a key with specs applied beside it (`ProposalBody::Key.sidecars`). Their records were final from the first storage version 2 release, before M5 made them declarable. `bellwether-types`' `sidecar_proposal_vectors_match_docs_format_md` writes these three proposals, checks that this block lists exactly their canonical JSON, one record per line, and reads each line back to the same record:

```json
{"record":"proposal","record_version":3,"evidence":["eda5d831cf40b9fa97713a4eb5eec65e322a1a05ab2c8dad5ce1f7907e10236c"],"proposed_by":"agent","created_at":"2026-10-25T00:00:00.000Z","body":{"kind":"key","spec":{"kind":"exact","columns":["symbol"],"bounds":"ts"}}}
{"record":"proposal","record_version":3,"evidence":["eda5d831cf40b9fa97713a4eb5eec65e322a1a05ab2c8dad5ce1f7907e10236c"],"proposed_by":"agent","created_at":"2026-10-25T00:00:00.000Z","body":{"kind":"key","spec":{"kind":"bitemporal","columns":["id"],"valid_column":"valid","known_column":"known"}}}
{"record":"proposal","record_version":3,"evidence":["eda5d831cf40b9fa97713a4eb5eec65e322a1a05ab2c8dad5ce1f7907e10236c"],"proposed_by":"agent","created_at":"2026-10-25T00:00:00.000Z","body":{"kind":"key","spec":{"kind":"exact","columns":["symbol"],"counts":true},"sidecars":[{"kind":"bloom","columns":["symbol"],"fpr":0.01,"bounds":"ts"}]}}
```

`fix` applies each of them since M5: the spec, and any specs beside it, are built, verified and receipted together. A binary of the first storage version 2 release refuses these specs in a manifest or a segment header with `Error::UnsupportedShape`.

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

Float items count canonical bytes, not SQL equality: `0.0` and `-0.0` are two values, and so is every distinct NaN payload, where SQL `count(distinct)` counts one zero (and engines differ on NaN). A float item's distinct count can therefore exceed the SQL answer by up to one for the zeros plus the number of NaN payloads beyond the first. Uniqueness is never asserted from a sketch: a file is reported as repeating a value only when its estimate times `1 + 3σ` (σ the relative standard error) is still below its row count, so a file of `r` rows must repeat more than `r · 3σ / (1 + 3σ)` of them to be flagged (24 of 1,000 rows at precision 14). `cardinality` reports that floor for a file of the average size as `duplicate_floor`, and the CLI prints it.

## Sketch conformance vectors (storage version 1)

`crates/bellwether-core/tests/sketch.rs` writes the reference segment (items `id` (Int64) and `id+tag` (Int64, Utf8), `p = 4`, `minhash = 2`; file 0 is one row group holding `(1, "a")` and `(2, null)`; file 1 is two row groups, `(1, "a")` then `(null, null)`; row groups 0..3) and checks every row of this table byte for byte.

| Vector | Bytes |
|---|---|
| `value_hash(Int64 1)` | `d6ef277c610adb0706d0067c5aa355d5` |
| `preamble` | `42 57 53 4b 45 54 43 48 01 00 00 00 01 00 00 00 f3 00 00 00` |
| `first entry, item id` | `00 00 00 00 01 00 00 00 02 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 00 00 00 00 00 00 00 02 0f 00 00 00 03 08 a9 ce 44 2c 92 04 07 de 13 b3 66 ea df fb b4` |
| `footer` | `02 00 00 00 00 00 00 00 03 00 00 00 ab 25 6d ba de e3 11 dd b0 80 2d e7 f2 40 53 1b a6 12 d6 4b 1e 04 dc d8 62 76 30 ad a5 67 a7 d0 42 57 53 4b 45 54 43 48` |
| `segment blake3` | `e84c9be5a3d4bdad2674f98364b2d2440e40795bf7f91c35a5c923429ddfd4f3` |

## Expression keys (`expr`, phase 5)

An `expr` key indexes a value derived from dataset columns by a closed, deterministic expression: the operator it serves is a filter or join key that is a function of a column (`json_get(payload,'$.symbol') = 'S-R0007-1'`, `date_trunc(ts,day) = ...`). It is a key-spec kind of its own (not a sidecar): `schema.key_specs[]` holds

```
{"kind":"expr", "name":<virtual column>, "expr":<normalized expression>,
 "over":{"kind":"exact","columns":[<name>],"counts"?:true} | {"kind":"bloom","columns":[<name>],"fpr":<f64>},
 "conformance":[{"expr":<closed expression>, "value":<lowercase hex> | null}, ...]}
```

named `expr:<name>=<expr>:exact` or `expr:<name>=<expr>:bloom`, and identified (storage version 2, the key of its segments and completeness) as `expr:<name>=<expr>:exact`, `expr:<name>=<expr>:exact,counts`, or `expr:<name>=<expr>:bloom@<fpr>`. `name` is an identifier that is not a dataset column. The store's key-spec allow-list holds `expr`, and the spec an `expr` key wraps is checked against the same list, so a reader without the kind refuses the manifest with the typed shape error (ADR 017), never a serde error or a silent drop on rewrite. A reader built before this kind refuses such a manifest, and so does this binary for a storage version 1 manifest that names it: the kind ships with storage version 2.

**Segments.** An `expr` key's segments are ordinary segments in the layout of `over` (the exact or bloom bytes above, nothing added), listed in `segments[]` with the `expr` spec's identity as `key_spec` and no `kind`. With `:exact,counts` the exact segment stores, per derived value and row group, its rows (the storage version 2 counts layout), so `bellwether count --keys` with a key file named by the expression answers `count(*) where <expr> in (...)` from the index, and a `locate` plan states its rows. The key column is the virtual column, encoded at the expression's value type with the canonical encoding. The segment header's key spec is the whole `expr` spec, so a segment carries its expression and conformance vectors. The build reads the expression's input columns one row group at a time and derives the virtual column for that row group only (ADR 018); a row whose value is null is not indexed (ADR 014).

**Expression language.** Six functions, string and integer literals, column references; no user code (`bellwether_core::expr` documents each function's types and value). `json_get(s, path)` gives a JSON value at a path as text (a string's text, a number's literal token as written, `true`/`false`; null for a JSON null, an object, an array, a missing path, or a document that is not valid JSON; a repeated member takes its first occurrence). `lower(s)` is the Unicode lowercase mapping as Rust's `str::to_lowercase` computes it (per-character default lowercase, the multi-character `İ` mapping, the final-sigma rule). `substr(s, start, len)` counts Unicode scalar values from a 1-based start. `cast(x, int64|utf8|timestamp_s|ms|us|ns)` converts strictly (a string that is not an integer is null). `date_trunc(t, second|minute|hour|day|month|year)` floors in UTC. `hash(x, ...)` is xxhash64 (seed 0) over the arguments' concatenated canonical encodings, as `UInt64`.

**Normalized form.** Function names and keywords lowercase, no whitespace, columns bare when an identifier (`[A-Za-z_][A-Za-z0-9_]*`) else double-quoted, strings single-quoted with `'` doubled, integers in decimal, JSON paths `$` then `.key` (identifier), `["key"]` (other, `"` and `\` escaped by `\`), or `[n]`; `date_trunc('day', ts)` normalizes to `date_trunc(ts,day)`. A lookup matches an `expr` key only when its expression normalizes to the key's expression; a lookup on any other expression, on the virtual column's name, or on an expression beside another column is refused, naming the `expr` keys that exist. An `expr` key answers point lookups (`locate`) only; `latest`, `asof`, window, and lookback lookups on an expression are refused. The residual is `ExprIn {expr, values}`: the expression recomputed per row, kept when its canonical value is one of `values`; a null value matches nothing.

**Conformance vectors.** An `expr` spec carries every vector below for each function its expression calls, and a spec that does not record every vector this binary pins for a function its expression calls (none at all, or only another function's) is refused at `index`, at segment open, and at plan, naming the missing vector. The consequence: adding a vector to a function later makes every existing `expr` key over that function refuse until it is declared again and rebuilt, so after the format freeze adding a vector is a format change. Every reader (segment open, spec check, build, plan, price, verify) recomputes them and refuses the key, naming both values, when this binary computes another: an evaluator whose behaviour moved (for example a changed Unicode table under `lower`) refuses rather than misses rows (invariant 1). A character whose mapping a later Unicode version adds is outside what the vectors see. `crates/bellwether-core/tests/expr_conformance.rs` checks every row of this table against the code's table, and every value against the evaluator. Values are canonical encodings (ADR 016) in lowercase hex; spaces are for reading only.

| Function | Closed expression | Value |
|---|---|---|
| `json_get` | `json_get('{"symbol":"SYM1","px":1.50}','$.symbol')` | `0653594d 310000` |
| `json_get` | `json_get('{"px":1.50}','$.px')` | `06312e35 300000` |
| `json_get` | `json_get('{"a":{"b c":[10,"x\u00e9"]}}','$.a["b c"][1]')` | `0678c3a9 0000` |
| `json_get` | `json_get('{"a":1,"a":2}','$.a')` | `06310000` |
| `json_get` | `json_get('{"a":"\ud83d\ude00"}','$.a')` | `06f09f98 800000` |
| `json_get` | `json_get('{"a":true}','$.a')` | `06747275 650000` |
| `json_get` | `json_get('{"a":null}','$.a')` | `null` |
| `json_get` | `json_get('{"a":[1]}','$.a')` | `null` |
| `json_get` | `json_get('{"a":1','$.a')` | `null` |
| `lower` | `lower('AbC-Z')` | `06616263 2d7a0000` |
| `lower` | `lower('ÀB')` | `06c3a062 0000` |
| `lower` | `lower('İ')` | `0669cc87 0000` |
| `lower` | `lower('ΣΑΣ')` | `06cf83ce b1cf8200 00` |
| `substr` | `substr('héllo',2,3)` | `06c3a96c 6c0000` |
| `substr` | `substr('abc',5,2)` | `060000` |
| `cast` | `cast('007',int64)` | `02800000 00000000 07` |
| `cast` | `cast('-12',int64)` | `027fffff ffffffff f4` |
| `cast` | `cast('1.5',int64)` | `null` |
| `cast` | `cast(42,utf8)` | `06343200 00` |
| `cast` | `cast(86400000000,timestamp_us)` | `080403 80000014 1dd76000` |
| `date_trunc` | `date_trunc(cast(90061000001,timestamp_us),day)` | `080403 80000014 1dd76000` |
| `date_trunc` | `date_trunc(cast(90061000001,timestamp_us),hour)` | `080403 80000014 f46b0400` |
| `date_trunc` | `date_trunc(cast(-1,timestamp_us),day)` | `080403 7fffffeb e228a000` |
| `date_trunc` | `date_trunc(cast(6307200000000,timestamp_us),month)` | `080403 800004a2 e0a32000` |
| `date_trunc` | `date_trunc(cast(6307200000000,timestamp_us),year)` | `080403 80000000 00000000` |
| `hash` | `hash('SYM1')` | `03 29185bcc 2d6b9e2a` |
| `hash` | `hash('SYM1','X1')` | `03 2be2127e 8956e278` |
| `hash` | `hash(1,'a')` | `03 e51a8fcd c38e4db3` |
## Text keys (storage version 2, B5)

A text key indexes the tokens of one string column (`Utf8`, `LargeUtf8`, `Utf8View`, or a dictionary of one) and serves the text operators `contains` (`like '%x%'`), `has` (`has_token`), and prefix match (`has_prefix`), never a key lookup. It is a key spec of its own, `{"kind": "text", "column": c, "tokenizer": "words" | "trigram", "bloom"?: fpr}` in `schema.key_specs`, named `text:<column>,tokenizer=<tokenizer>` (`KeySpec::name()`, its slot: a words and a trigram key coexist on one column), with identity `KeySpec::identity()` = the name, plus `,bloom=<fpr>` for a bloom layout; its segments are ordinary key-spec segments (`segments[].kind` absent, `key_spec` = the identity) and complete under the identity. A reader that does not know the kind refuses the manifest with `Error::UnsupportedVersion` (ADR 017), so text keys ride on storage version 2; a tokenizer this build does not know is refused the same way.

**Tokens.** A value is case-folded first: each code point is replaced by its Unicode full case folding (`CaseFolding.txt`, statuses C and F), one code point at a time, with no context, so folding a concatenation folds each part and a substring of a value folds to a substring of the folded value. `ß` folds to `ss`, `ς` and `Σ` to `σ`, the Kelvin sign to `k`, `İ` to `i` + U+0307. The residual folds the same way, so plan and residual always agree.

Text is **not Unicode-normalized**: folding maps case, never composition, so a precomposed `é` (U+00E9, NFC) and `e` followed by a combining acute (U+0065 U+0301, NFD) are different text. `contains("école")` in NFC does not match the NFD spelling, and `has_prefix("e")` matches the NFD word `école` (it starts with `e`) but not the NFC one. Normalize the column and the arguments to one form before indexing if both forms occur.

- `words`: the words of the folded value at Unicode word boundaries (UAX #29, `unicode_words`), each stored as the canonical `Utf8` encoding of the word (ADR 016).
- `trigram`: every 3-byte window of the folded value's UTF-8 bytes, each stored as the canonical `Binary` encoding of the 3 bytes (a window may cut a multi-byte character). A folded value shorter than 3 bytes has no trigram.

A null value has no token and matches nothing (ADR 014); the empty string is a value with no token.

**Segment.** Without `bloom`, the exact layout with tokens as keys: per distinct token, the roaring bitmap of the row groups holding a row with that token. With `bloom`, the bloom layout: one split-block bloom filter per row group over that row group's tokens, at the given false-positive rate (ADR 008). The header carries one more field, `text_unicode: {case, words}`, naming the tables tokens were folded and split with: the pinned crate and version that carries each (`"unicase 2.10.0"`, `"unicode-segmentation 1.13.3"`), never the toolchain's tables. A text segment without it, or a non-text segment with it, is corrupt; a reader whose tables differ refuses the segment with a typed error whose next step is `bellwether index <root>`, and `index` rebuilds exactly the text keys whose segments record other tables (only their segments are dropped; the ledger and every other key are kept), so one key never holds tokens from two table versions. `index` reads one segment header per text key to tell (a key's segments always share one table version), and its result lists each key it rebuilt this way with the old and new tables (`text_rebuilt`). This happens only after a deliberate upgrade of either crate, which the workspace pins to one exact version.

**Lookups.** For an argument `a`, folded to `f`:

| Spec | `has_token(a)` (`f` one word) | `contains(a)` | `has_prefix(a)` (`f` non-empty) |
|---|---|---|---|
| words | row groups of word `f`: exact | not served | row groups of any word with prefix `f`: exact |
| words, bloom | candidates: row groups whose filter may hold `f` | not served | not served |
| trigram (bloom or not) | candidates: row groups holding every trigram of `f` | the same | the same |

An argument shorter than 3 bytes once folded has no trigram, so a trigram key plans every row group for it, and says so. Several serving specs are intersected; a run's several values are united. Exact means every planned row group holds a matching row; candidates mean a planned row group may hold none, so the residual is mandatory. The residual is `Text{column, operator, values}`: keep a row whose folded value matches one of the folded values.

## Text key conformance vectors (storage version 2)

`crates/bellwether-core/tests/text_keys.rs` checks these byte for byte.

| Vector | Bytes |
|---|---|
| `fold("ẞİΣ\u{212A}ς")` | `ssi` U+0307 `σkσ` |
| words key `grösse` | `06 67 72 c3 b6 73 73 65 00 00` |
| trigram key `61 c3 b6` (`aö`) | `07 61 c3 b6 00 00` |
| word prefix probe `st` | `06 73 74` |
| words of `hello, wörld! can't 3.5 日本` | `hello`, `wörld`, `can't`, `3.5`, `日`, `本` |
| `text:body,tokenizer=words` over row groups `["Hello, World"]`, `[null, "Straße"]` (one segment, block of 3 keys), blake3 | `9ec49da81fbc14f0851824f2cf1c9e0d5b4073518f07e8e7fe7506bb7c73e7e1` |
| `text:body,tokenizer=trigram`, the same rows, blake3 | `61059108b1be52592ddf632085b14b055dc4640b736ff3a2c3e5896d82872e57` |

## List keys (B4, `list:<column>`, 2026-10-26)

`KeySpec::List { column }` (JSON `{"kind": "list", "column": "<c>"}`, named `list:<c>`) is an inverted index over a list column: per distinct **element** value, the row groups holding a row whose list contains that value. It is a key spec of its own, not a sidecar: it is declared in `schema.key_specs`, its segments are ordinary key-spec segments (`segments[].kind` absent, `key_spec` = `list:<c>`), and it is complete under its name like any key spec (ADR 012). A reader that does not know the kind refuses the manifest with the typed unknown-kind error (`key-spec or fingerprint shape in storage`), never a serde error and never by dropping the declaration on a rewrite (ADR 017); the kind is admitted by storage version 2 (WP14), which every commit by a binary that knows it writes.

- **Column.** An Arrow `List<T>` or `LargeList<T>` whose item type `T` has a canonical encoding (the table above), or a `Map<K, V>`, indexed by its keys `K`. A fixed-size list, a list view, a list of a nested type, or a flat column is refused with the reason.
- **Bytes.** The exact layout (`layout: "keyed"`), unchanged: sorted blocks of element keys, one split-block bloom per block, element key to a no-run roaring bitmap over global `RowGroupId`. An element key is the canonical encoding of one element at the dataset's element type (a file `List<Int32>` under a dataset `List<Int64>` encodes at Int64; a value that does not convert fails the row group, all or nothing). The header's `key_spec` is the list spec and its `key_types` holds one entry, the element type; a segment whose recorded element type differs from the dataset's is refused on open.
- **Nulls and empty lists (ADR 014).** A null list, an empty list, and a null element are not indexed. An empty list holds no element, so no membership probe can match it, and a null probe matches nothing (`array_contains(x, NULL)` is unknown, never true); the index is therefore exact without them. A row group whose lists are all null or empty is still covered: the writer records the range it processed. `contains_all` with no value is refused, because "contains all of nothing" holds for every row, null and empty lists included, which the index does not record. A null queried value is dropped and counted by `contains_any` and refused by `contains_all` (ADR 014, amendment 2026-10-26).
- **Duplicates.** An element repeated within a list, or across rows of one row group, is one key with that row group once.
- **Lookups.** `contains_any(values)` plans the union over the queried values of each value's row groups; `contains_all(values)` plans their intersection. A float zero is probed in both encodings. The residual is `Predicate::ContainsAny { column, values }` (the queried encodings, both zeros listed) or `Predicate::ContainsAll { column, values }` (one group per queried value, both zeros in one group), which the reader applies per row: `contains_all`'s plan keeps a row group holding every value in different rows. A list key serves only these two lookups, never a lookup on the column's value as a whole, and no other kind serves them; without a complete list key on the column a membership lookup reads every live row group.
- **Memory (ADR 018).** As the exact layout: a lookup holds one segment's block index, one block at a time, and one bitmap per present queried value. The build counts a row group's distinct element keys and their bytes against the segment caps.

Conformance vectors (`crates/bellwether-core/tests/list.rs`): a `list:tags` segment over `List<Utf8>` built from row group 0 `[AAPL, MSFT, AAPL]`, `[]`; row group 1 null, `[MSFT, null]`; row group 2 `[IBM]`; row group 3 `[]`, with processed range 0..4 and default writer options, written in the storage version 2 segment format (its footer checksum included). It holds three keys.

| Vector | Bytes |
|---|---|
| element key `AAPL` | `06 41 41 50 4c 00 00` |
| `AAPL` bitmap {0} | `3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 00 00` |
| `MSFT` bitmap {0, 1} | `3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00` |
| `list:tags blake3` | `972f0deeeb42ea46c6402160dae27f6ac7dfd87102ad9480e83798db78ad232d` |

## Join keys (B3, `join:<left>.<cols>=<right>.<cols>`, 2026-10-26)

`KeySpec::Join { dataset, columns, right, right_columns, counts? }` (JSON `{"kind": "join", "dataset": "orders", "columns": ["id"], "right": "payables", "right_columns": ["order_id"]}`, `"counts": true` only when set) is the key overlap of a dataset pair on one column pair, for semi-joins, anti-joins, and `in (subquery)` between them. Its name is `join:<dataset>.<columns joined by ,>=<right>.<right columns joined by ,>` (`join:orders.id=payables.order_id`); its identity adds `,counts` when it counts. It is a key spec of its own, admitted by storage version 2 like `list` (a version 1 manifest declaring it, or a join key with an unknown field, is refused with the typed shape error), declared in the **left** dataset's `schema.key_specs`, with ordinary key-spec segments (`segments[].kind` absent, `key_spec` = its identity).

- **Datasets.** The left dataset is the data root, and `dataset` must be its folder's name. The right dataset is the folder named `right` beside the data root (`<parent of the root>/<right>`), read through its own index at its default location; a symbolic link there must resolve to a folder in that same parent, and one that resolves anywhere else is refused naming where it points (a join key reads only a dataset in the folder that holds the left one). A dataset name is one folder name of letters, digits, `_`, and `-`; a column named by a new join key may not contain `,`, `@`, or `=`, and its last right column may not be named `counts`, so the name parses one way.
- **Built from both exact keys.** A join key is not built row group by row group. `bellwether index` on the left builds it whole from the complete exact keys on the join columns in both indexes (`exact:<columns>` in the left, `exact:<right columns>` in the right; with `counts`, both counted), by a merge join over their sorted key streams that holds one block per input segment (ADR 018). A missing exact key on either side is refused (`not_indexed`, naming the key to declare), never guessed. Join columns pair only when their canonical encodings compare: signed integers of any width with signed integers, unsigned with unsigned, strings with strings, binaries with binaries, decimals of one width and scale, dates, times, and timestamps of one unit. A float column is refused: `-0.0` and `0.0` are equal in SQL and encode differently, and engines disagree on `NaN`.
- **Split by key range (ADR 018).** The pair is written as segments capped like any other key's: a segment is finished before the shared key that would take it past the build's `max_segment_keys` keys or `max_segment_bytes` bytes held (a key whose own record exceeds a cap gets a segment of its own), so a build holds one open pair segment whatever `|L ∩ R|` is, and uploads each as it is finished. The segments hold disjoint, ascending key ranges, each shared key in exactly one; each covers the whole left row-group range, carries the same pins, and counts the one-sided keys of its stretch of the merge, so the one-sided counts sum over the segments to the pair's. Readers take the segments together (a key held twice is refused as corrupt). `why` prices a join key from the same segments built in memory, all of them within the cap on an index built only to price (1 GiB by default); a pair past it is refused, naming the `bellwether index` command that builds it into the index segment by segment, never truncated.
- **Pinned (ADR 020).** Every segment's header records what its pair was built from: `left_sequence`, the left manifest whose exact segments it read (provenance); `left_live`, the identity of the left's live row-group set it covers (BLAKE3 over each live id as 4 little-endian bytes, ascending); `right_sequence` and `right_manifest`, the right manifest it read by sequence and by content (BLAKE3 of the manifest bytes as stored, so the same sequence of a right index deleted and rebuilt is another manifest). The manifest entry repeats the last three (`segments[].join`, absent for every other segment, unknown fields kept per ADR 017), and a reader refuses a segment whose header and entry disagree, or segments of one key pinned differently. The commit that writes it records the key complete. A later left commit keeps it complete only while the left's live row groups are exactly the pinned set (a commit that adds a key or compacts changes none); a commit that adds, changes, or removes a left file leaves it incomplete: **stale on the left**. A right dataset whose current manifest is not `right_manifest` makes it **stale on the right**: committed again (a later sequence), or deleted and rebuilt (another manifest at a sequence not later). A right dataset with files its index does not cover yet is stale too (its index is). A stale join key is never planned with (`stale_join`, naming the side); `bellwether index` on the left rebuilds it, refusing before it commits anything if the right manifest changes while it builds, and `bellwether join verify` reports each join key's state.
- **Nulls (ADR 014).** A tuple with a null element equals nothing: exact keys never index a null leading column, and a stored composite tuple with a trailing null marker is never paired and counts as one-sided on its side. A null probe matches nothing.
- **Live row groups.** Each side's bitmaps and counts keep only its live row groups; a key held only in retired ones (ADR 013) is absent from that side.

The segment is the keyed layout with a pair per key, `layout: "keyed_join"`:

```
header       the storage version 2 header, with key_spec the join key, key_types the left columns'
             types, right_key_types the right columns' (both or neither), left_sequence, left_live,
             right_sequence, right_manifest (hashes as 64 hex digits)
keys         the keys both sides hold, strictly ascending, canonical encodings at the left types;
             blocks, block blooms and directories as the exact layout
value record left record | right record, each  bitmap_len u32 | bitmap | [counts]  as the exact
             layout's (left bitmap over left row-group ids, right bitmap over right ids, both
             non-empty; counts present exactly when the key counts), nothing after
index        the keyed index (entries = shared keys; rg_min..rg_max the left row groups covered),
             then left_only u64 | right_only u64: the keys on one side only, per side
```

The recorded row-group range is the left's (every live left row group when built); right ids never widen it. Every region is checksummed as in the exact layout (the one-sided counts by the footer's checksum over the index), so a damaged byte is refused, never read as another pair. A join segment is rebuilt whole (all of a key's segments in one commit) and never merged; a key lookup on it is refused.

**Lookups.** `semijoin(keys)`, with keys on the left columns: per key both sides hold, its row groups on each side. The plan is two steps: the left row groups holding a matched key (what `left ⋉ right` reads for these keys), then the right row groups holding one, each with an `In` residual over the matched keys on its side's columns; with counts, each step's rows are exact. Invariant 1 holds per side: no row of a matched key is outside its side's plan, and a key one side lacks has no row in the semi-join (an anti-join's keys are the asked keys that did not match). `overlap_stats()`: `|L ∩ R|` (shared keys), `|L \ R|` and `|R \ L|` (the index's one-sided counts), and per shared key the most and the sum of its row groups on each side (the mean is the sum over the shared keys); with counts, the rows each side keeps and the rows of the inner join (per shared key, left rows times right rows). Both read the pair index alone, no data file.

Conformance vectors (`crates/bellwether-core/tests/join.rs`): the counted pair of `join:orders.id=payables.order_id` built from a left exact key (`Int64`) holding `1` in left row groups 0 (3 rows) and 2 (1 row), `2` in 1 (2 rows), `3` in 3 (1 row), and a right exact key (`Int32`) holding `1` in right row groups 10 (1 row), `3` in 11 (4 rows) and 12 (1 row), `4` in 12 (2 rows); left live 0..4, right live 10..13, `left_sequence` 5, `right_sequence` 7, `right_manifest` the BLAKE3 of `7u64` as 8 little-endian bytes, default writer options, no cap (one segment). It holds `1` and `3`; `2` and `4` are one-sided.

| Vector | Bytes |
|---|---|
| shared key `1` | `02 80 00 00 00 00 00 00 01` |
| pair value of `1` | `14 00 00 00 3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 02 00 03 01 12 00 00 00 3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 0a 00 01` |
| one-sided counts (index suffix) | `01 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00` |
| pinned pair blake3 | `d2756f7a53e0fdc1030c05b2e0766dc0569e608fca65dbd2c2a5751af7fe57d7` |
## Agg keys (B7, `agg:<key>@<value>{...}`, 2026-10-26)

`KeySpec::Agg { columns, value, functions }` (JSON `{"kind": "agg", "columns": [...], "value": "<v>", "functions": [...]}`, named `agg:<columns>@<v>`, identity `agg:<columns>@<v>{<functions>}`) stores, per key value per row group, the aggregates of one value column: the operator `sum/count/min/max(value) group by key where key in (...)`. `functions` lists each of `sum`, `count`, `min`, `max` at most once, in that order, with `count` always among them (the rows and the nulls are always stored); the CLI accepts any order and writes the canonical one. It is a key spec of its own, not a sidecar: declared in `schema.key_specs` (`key_spec_fields`: `kind`, `columns`, `value`, `functions`), its segments ordinary key-spec segments (`segments[].kind` absent, `key_spec` = the identity), complete under its identity (ADR 012). A reader that does not know the kind refuses the manifest with the typed unknown-kind error, never by dropping the declaration on a rewrite (ADR 017); the kind is admitted by storage version 2. The name has no functions, so declaring another function set redefines the key: it is rebuilt and the old segments dropped in the same commit.

- **Beside a counted exact key.** An agg key is declared, planned, proposed, and scored only beside `exact:<columns>,counts` on the same columns: the counts are what prove which row groups an aggregate answers. Without it the declaration, the proposal, and the score are refused, naming the spec to declare first.
- **Value column.** Any column with a canonical encoding that is not a key column. `sum` needs an integer, `Decimal128`, or float column; a `Decimal256`, string, or other column is refused for `sum` with the spec to declare instead (`{count,min,max}`).
- **Bytes.** The keyed layout's blocks, bloom, and directory, unchanged, with `layout: "keyed_agg"`; the header carries `key_types` and `value_type` (the value column's dataset type). The value record is `bitmap_len u32 | bitmap | cells`: one cell per set bit, in ascending row-group order, and nothing after the last.

```
cell             rows LEB128 | nulls LEB128 | [sum] | [min] | [max]
rows             rows of the key in the row group, at least 1, shortest LEB128 form
nulls            among them, rows whose value is null, at most rows
sum              present exactly when functions has sum:
                   01 | i128 LE            an integer, or a Decimal128 unscaled at the dataset scale; exact
                   02 | f64 LE | f64 LE    a float: the Neumaier-compensated running sum and its
                                           compensation; never exact (model agg.kahan_f64); once the
                                           running sum is infinite or NaN it is kept as IEEE 754
                                           makes it (inf + 1 = inf, inf + -inf = NaN) and the
                                           compensation stops adding
                 a cell whose values are all null stores 01 and an i128 zero, whatever the type
min, max         each present exactly when functions has it and nulls < rows: bytes (len u32 | len
                 bytes), the canonical encoding of the least (greatest) non-null value at the
                 dataset type; min <= max as bytes
```

- **Nulls (ADR 014, amendment 2026-10-26).** A row whose leading key column is null is not indexed and in no cell, as in every keyed kind. A null value is counted in `rows` and `nulls`, never summed or compared: `count(value)` is `rows - nulls`, and `sum` of a key whose values are all null is reported as null, not zero.
- **Values at the dataset type.** Each value is converted to the value column's dataset type before it is summed or compared (ADR 016): a decimal is rescaled to the dataset scale exactly, or the row group fails, all or nothing (never rounded); integers sum exactly as `i128`, and a sum past 128 bits within one row group fails the build, naming the spec, the key and the row group, and the spec to declare instead (without `sum`). Minimum and maximum compare canonical bytes, which order as the typed values do: `-0.0 < +0.0`, NaN the greatest float. Two float cells are the same cell when their bytes are (a NaN sum equals the same NaN sum), which is how `fix`'s certificate compares the cells it checks.
- **Sums past 128 bits at lookup time.** Each row group's sum fits in 128 bits, but a key's sums added across its row groups may not. A lookup that asks for `sum` of such a key is refused, naming the spec, the value column and the key, and the functions to ask instead; a lookup of the same key that does not ask for `sum` (`count`, `min`, `max`) adds no sums and is answered. A reconcile whose agg side holds such a key reads that side's rows instead (`agg_not_used` says so) and totals them exactly as wide integers.
- **Merge.** Cells of a key are interleaved by row group only when the inputs' row groups are disjoint, as the build's contiguous groups are; two inputs that both aggregate one (key, row group) are refused, never combined. A read that finds two listed segments of one agg key both holding a cell for one (key, row group) refuses the index as corrupt.
- **Lookups.** An aggregate by columns `C` of value `v` reads one complete agg key on `v` whose columns `P` are among `C` and which stores every function asked, beside `exact:C,counts`. Per matching row group, the lookup key's count there is compared with the cell of its projection onto `P`: equal, the cell answers it (every row the cell aggregates is a row of the lookup key); larger (a finer lookup over a coarser agg key, `P ⊂ C`), the row group is residual and read; smaller, or no cell, is a corrupt index, refused. When `P = C` every matching row group is answered and no data file is opened. Rows are always exact, from the counts.
- **Reconcile.** A side whose fresh index holds a complete agg key on exactly the reconcile key, storing the sum of its one `Decimal128` total at the side's dataset type, is folded from the cells instead of its rows, when no version rule, row filter, or expression scope selects among a key's rows, every compared column is a key column (so the total itself is ignored, compared only through its exact sum), totals are grouped by nothing or by the key, and the agg key's rows are every row of the side (a side with null leading keys is read instead). The certificate's `agg` lists each side so served (`AggServed`); the reconcile's `agg_not_used` lists each side that holds such a key and was read, with the reasons and the next step (for a compared column, the `ignore` list that drops it; for a stale index, the `index` run).
- **Memory (ADR 018).** As the exact layout: a lookup holds one segment's block index, one block at a time, and the cells of the keys asked; the build holds one row group's cells per key and counts their bytes against the segment caps.

## Agg key conformance vectors (storage version 2)

`crates/bellwether-core/tests/agg.rs` writes `agg:account@amount{sum,count,min,max}` over `account: Utf8`, `amount: Decimal128(18, 6)`, with default writer options and processed range 0..3, from row group 0 (`a` 1.500000, `a` null, `b` -2.000000), row group 1 (`a` 0.250000, a null account with 9.000000, which is not indexed), and row group 2 (`b` null). It holds two keys and is pinned byte for byte (`PINNED_HEX`); its header is

```
{"storage_version":2,"encoding_version":1,"key_spec":{"kind":"agg","columns":["account"],"value":"amount","functions":["sum","count","min","max"]},"codec":"none","layout":"keyed_agg","bloom_hash":"canonical","key_types":[{"type":"utf8"}],"value_type":{"type":"decimal128","precision":18,"scale":6}}
```

| Vector | Bytes |
|---|---|
| `a` bitmap {0, 1} | `14 00 00 00` then `3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00` |
| `a` cell, row group 0: 2 rows, 1 null, sum 1.500000, min = max 1.500000 | `02 01 01 60 e3 16 00 00 00 00 00 00 00 00 00 00 00 00 00 12 00 00 00 05 10 80 00 00 00 00 00 00 00 00 00 00 00 00 16 e3 60 12 00 00 00 05 10 80 00 00 00 00 00 00 00 00 00 00 00 00 16 e3 60` |
| `a` cell, row group 1: 1 row, sum 0.250000 | `01 00 01 90 d0 03 00 00 00 00 00 00 00 00 00 00 00 00 00 12 00 00 00 05 10 80 00 00 00 00 00 00 00 00 00 00 00 00 03 d0 90 12 00 00 00 05 10 80 00 00 00 00 00 00 00 00 00 00 00 00 03 d0 90` |
| `b` cell, row group 0: sum -2.000000 | `01 00 01 80 7b e1 ff ff ff ff ff ff ff ff ff ff ff ff ff 12 00 00 00 05 10 7f ff ff ff ff ff ff ff ff ff ff ff ff e1 7b 80 12 00 00 00 05 10 7f ff ff ff ff ff ff ff ff ff ff ff ff e1 7b 80` |
| `b` cell, row group 2: 1 row, 1 null (integer zero sum, no min or max) | `01 01 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00` |
| `agg:account@amount{sum,count,min,max}` blake3 | `9fb9f7147c731d0d55220d5f3b0c4f4182267b4a33aac26ff956bb6effb6d609` |
