# bellwether-core

Work package WP1. This crate holds canonical key encoding, split-block bloom filters, the segment writer and reader, temporal version chains, and segment merge. It owns invariants 1 and 5 and implements ADR 001, 002, 008 (with its amendment), 014, 016, 017, and 018.

| Module | What it does |
|---|---|
| `encoding` | Canonical memcomparable bytes for every Arrow scalar type, always relative to the dataset type. Provides `KeyColumns` (row-at-a-time key encoding that converts each column to its dataset type and skips null rows), `ts_value_as`, `encode_nullable_as` (row hashing), and `prefix_bound`. |
| `bloom` | Parquet split-block bloom filter (SBBF), generic over its hash kind: `Sbbf<CanonicalHash>` or `Sbbf<PlainHash>`. The `BloomHash` header value names the kind. |
| `segment` | `SegmentWriter` (`for_dataset` for Arrow batches), `SegmentReader<R: ReadAt>`, `FileRange`, and `EncodedSegment::path()` = `segments/<blake3>.seg` |
| `temporal` | `latest` and `asof` across every segment of a key spec, plus `VersionChain`, `VersionHit`, and `is_stale` |
| `merge` | `merge(&[SegmentReader], &WriterOptions)` |
| `expr` | The `expr` key language (`json_get`, `lower`, `substr`, `cast`, `date_trunc`, `hash`): `Expr::parse` and the normalized form (`normalize`), `Typed::eval` (one batch's derived column, row at a time, JSON scanned without a tree and capped at `MAX_JSON_DEPTH`), the pinned `VECTORS` and `verify_vectors`, and the `KeySpec::Expr` rules (`spec`, `check_spec`, `typed_spec`, `with_derived`, `suggested_name`). A segment of an `expr` key is the inner exact or bloom layout over the virtual column; its header carries the expression and vectors, which a reader recomputes before opening it. |
| `sidecar` | The `BWSIDECR` layout of the `bounds` and `temporal2` sidecars of an index written before M5 (since M5 no build writes it; a bloom's `bounds` segment is a storage version 2 `keyed` segment); writer (`SidecarWriter`), reader (`SidecarReader<R: ReadAt>`), `bitemporal_winner` (shared with the `bitemporal` layout), and `measure` |
| `sketch` | `sketch` sidecar segments (approximate, never planned with): `FileSketcher` (one file's per-item HyperLogLog and optional MinHash), `SketchWriter`/`SketchReader` (one entry per file stretch, BLAKE3-sealed), `SketchTotals` (folds entries; `cardinality(i, exact)` with its interval, `copresent`), `overlap` (MinHash or HyperLogLog Jaccard with bounds), `value_hash`, `validate`, `item_types` |
| `hll` | The one HyperLogLog (Ertl's improved raw estimator, `p` in 4..=18), shared by the profiler's fanout pass and the `sketch` sidecar; `merge` is the register-wise maximum (lossless union) |
| `text` | Text keys (B5): full case folding (`unicase`, pinned) and the words (UAX #29, `unicode-segmentation`, pinned) and trigram tokenizers, the tables a segment records (`UnicodeTables`), the residual's row test (`matches`, `matches_any`), how a text spec serves `has_token`, `contains`, `has_prefix` (`probe`, `Shape`), row-group selection (`select`), and exact token counts (`token_stats`) |

Tests:

| File | Covers |
|---|---|
| `tests/no_false_negatives.rs` | Invariant 1, including files whose decimal scale, decimal width or timestamp unit differs from the dataset's |
| `tests/temporal_stale_rejection.rs` | Invariant 5 |
| `tests/encoding_order.rs` | Property tests: typed order, dataset-type conversion, decimal and timestamp rules |
| `tests/parquet_bloom_compat.rs` | Byte compatibility with the `parquet` crate's filters |
| `tests/segment_format.rs` | Format rules |
| `tests/corrupt_sweep.rs` | Every truncation and a byte-flip sweep never panic |
| `tests/conformance.rs` | The pinned test vectors of `docs/format.md`, through both the scalar and the Arrow encoder paths; the test also requires the document's table to list exactly these vectors |
| `tests/expr_conformance.rs` | Every `expr` conformance vector reproduces, each function has at least two, and `docs/format.md` lists exactly the code's table |
| `tests/expr_language.rs` | Differently spelled equal expressions normalize equal and near misses do not; parse and type refusals; each function property-tested against an independent oracle (`serde_json` for `json_get`) |
| `tests/expr_segments.rs` | `expr` segments: round trip, nulls (ADR 014), a brute-force oracle, truncation and byte-corruption sweeps, and a header whose vector or expression changed refused typed |
| `tests/sidecar_conformance.rs` | Pinned byte and record conformance for the `bounds` and `temporal2` sidecar segments of an index written before M5, `measure`, `add_batch` atomicity, and corruption sweeps |
| `tests/folded_conformance.rs` | Storage version 2 folded layouts (M5): the pinned `exact:symbol@ts,counts` value records with the all-null sentinel, the records of the `bounds` segment of `bloom:symbol@0.01@ts` and the `temporal2:symbol@valid,known` records, their directory checksums and the three segments' BLAKE3 (the table in `docs/format.md`, "Folded layout conformance vectors"); the sentinel overlaps no window; merge widens bounds with the sentinel absorbing and unites versions; an oracle over a Parquet file (value records equal to ones computed row by row, window plans exactly the overlapping row groups and never short, a split build merging back to the same bytes); every byte value at every body position and every truncation of each folded layout, a bloom's `bounds` segment included |
| `tests/text_keys.rs` | Text keys: a property test of every lookup through every text spec against a brute-force oracle with its own case folding (multi-byte UTF-8, sharp s, the sigmas, dotted and dotless i, the Kelvin sign, decomposed accents, CJK, NUL, nulls), round trip and merge, nulls (ADR 014), the Unicode-tables refusal, and the conformance vectors of `docs/format.md` |
| `tests/text_corrupt_sweep.rs` | Corrupt text segments never panic: every header byte and a sample of the rest in the gate, every truncation, position, and byte value under `--ignored` |
| `tests/sidecar_oracle.rs` | Oracle property tests for `bounds`, `temporal2`, and `bitemporal_winner` across row groups, nulls, composite keys, and sentinel timestamps |
| `tests/sketch.rs` | `sketch` segments: round trip against a brute-force oracle (rows, nulls, distinct counts inside the interval; property test); split files merge losslessly; the HyperLogLog error bound over 312 trials at precisions 10, 12, and 14 (1 to 300,000 distinct values) against exact counts; MinHash and HyperLogLog overlap intervals against the exact Jaccard; nulls per ADR 014; determinism; layout rules behind the checksum; every-byte corruption refused; the `docs/format.md` conformance table byte for byte |
| `tests/counts_conformance.rs` | Storage version 2 counts: the pinned value records and their directory checksums (the table in `docs/format.md`, "Counts conformance vectors"); every segment of every fixture (`format-v1`, `format-v0.1-pre`, `format-v0.1` and its CSV index) named by its hash, read by the reader of its magic, and counted (and bounded) only when it stores counts (bounds); merge sums disjoint counts and refuses overlapping ones; every byte value at every body position of a counted exact and temporal segment never panics or reads a wrong count |

Benchmark: `cargo bench -p bellwether-core --bench lookup` measures single-key lookup latency against segment size.

## Encoding (`docs/format.md`, encoding version 1)

**The dataset schema decides every byte.** `Source::schema` is the single source of column types. A value read from a file is first converted to its column's dataset type, then encoded.

- Two files holding 1.50 as `Decimal128(10,2)` and 1.500 as `Decimal128(10,3)` encode identically under a dataset `Decimal128(20,3)`.
- A conversion that would lose information is an error, never a silent miss or a rounding.
- `SegmentWriter::for_dataset(spec, schema, opts)` carries the dataset types and records them in the segment header (see below). `add_batch` refuses to run without them.
- `add_batch` is all or nothing. The batch is encoded into a local buffer first, and its keys are recorded only if every row converts. On error nothing from the batch is recorded and the writer stays usable, so a build that skips the row group never publishes part of it.

The byte values below are those of `docs/format.md`, agreed with WP2 (`bellwether-source`), whose statistics encoder has to match byte for byte. `tests/conformance.rs` checks the document's pinned vectors.

| Dataset type | Bytes |
|---|---|
| Boolean | `01` then `00`/`01` |
| Int8..Int64 | `02` then i64 BE, sign bit flipped |
| UInt8..UInt64 | `03` then u64 BE |
| Float16..Float64 | `04` then f64 bits with sign-aware flip; one canonical NaN, sorting last; `-0.0 < +0.0` |
| Decimal128(p,s) | `05 10` then 16 bytes BE, sign bit flipped, at scale `s` |
| Decimal256(p,s) | `05 20` then 32 bytes BE, sign bit flipped, at scale `s` |
| Utf8, LargeUtf8, Utf8View | `06` then bytes with `00` escaped as `00 FF`, then terminator `00 00` |
| Binary variants | `07` then the same escaping |
| Date32 | `08 01 05` then days |
| Date64 | `08 02 02` then ms |
| Time32/Time64(u) | `08 03 <u>` then time of day in unit `u` |
| Timestamp(u, tz) | `08 04 <u>` then the UTC instant in unit `u` |

Time-unit tags `<u>`: `01` s, `02` ms, `03` µs, `04` ns, `05` day. Every temporal payload is an i64, BE with the sign bit flipped. Times and timestamps keep the dataset unit, so a year-9999 sentinel in a millisecond or microsecond column encodes without overflow. For example, `Timestamp(µs)` 1 encodes as `08 04 03 80 00 00 00 00 00 00 01`.

Conversion rules, file type to dataset type:

- **Tag.** The tag always comes from the dataset type. A `UInt32` file column under a dataset `Int64` encodes with `02`.
- **Integers.** They convert across widths and signedness, range-checked against the dataset type.
- **Floats.** They convert across widths. A value not exactly representable in a Float16 or Float32 dataset type is an error, except NaN.
- **Decimals.** They rescale to the dataset scale. The rescale must be exact and fit the dataset precision. The width (16 or 32 bytes) is the dataset type's.
- **Times and timestamps.** They convert to the dataset unit: to a finer unit by checked multiplication, to a coarser unit only when the division is exact. The time zone is ignored, because Arrow values are already UTC instants; a value without a zone is taken as UTC.
- **Dates.** Date32 and Date64 convert into each other exactly.
- **Strings and binary.** The three string types are interchangeable, and so are the binary types. A `FixedSizeBinary(n)` dataset type requires length `n`. Strings and binary never convert into each other.
- **Other families.** A conversion across any other families, such as an integer into a decimal, is an error.
- **Nulls (ADR 014, amended).** Nulls are not indexed per tuple, not per column. A row whose leading key column is null is not indexed. A null in a trailing key column is encoded as the `00` null marker, so the tuple stays prefix-seekable by its leading columns, and a composite key fully replaces its leading single-column key for prefix lookups. A probe tuple that contains a null matches nothing (SQL three-valued logic); `encoding::tuple_has_null` recognizes one. Row hashing (`encode_nullable_as`) uses the same marker.

Duration, Interval, and nested types have no encoding in version 1 and are rejected.

## Bloom filters (ADR 008 amendment)

Structure and sizing are byte-identical to Parquet's embedded filters. `tests/parquet_bloom_compat.rs` writes a file with the `parquet` crate and compares the bitsets byte for byte.

The hash input is not identical:

- Bellwether's filters hash canonical bytes (`CanonicalHash`).
- Parquet's embedded filters hash the PLAIN physical value (`PlainHash`, built by `bloom::parquet_plain`).
- The two are distinct types, and `Sbbf<H>` takes only its own kind, so a filter cannot be probed with the wrong hash. The zero-copy probe over a raw bitset, which carries no kind, is crate-private.

Every segment header carries `bloom_hash` (`docs/format.md`):

- `canonical` for keyed and keyed-temporal block filters
- `canonical` or `parquet_plain` for bloom segments
- `canonical` for range segments, which have no filters

A missing or unknown value is refused on open.

A bloom segment with `parquet_plain` filters (from `SegmentWriter::insert_filter`) covers one column. It answers only `lookup_hashes::<PlainHash>`, and `lookup` with canonical keys is an error, never a silent miss.

## Segment format (`docs/format.md`, storage version 2)

All integers are little-endian.

```
preamble   "BWSEGMNT" | storage_version u32 | encoding_version u32 | header_len u32
header     JSON {storage_version, encoding_version, key_spec, codec, layout, bloom_hash,
                 key_types?, ts_type?, time_types?, ..unknown}
body       blocks (layout-specific)
index      entries u64 | has_rg u8 | rg_min u32 | rg_max u32 | block_count u32 | entries
footer     index_offset u64 | index_len u64 | checksum u64 | "BWSEGMNT"
```

- **Versions before JSON.** The versions sit in a fixed binary preamble, so a newer file is refused with `Error::UnsupportedVersion` before the header is parsed (ADR 017). The JSON header repeats them and must agree with the preamble. Unknown header fields are kept in `SegmentHeader::extra`, and merge carries them through when every input agrees, refusing otherwise.
- **Checksums (storage version 2).** Every region a read touches carries an XXH3-64 checksum (`segment::checksum`): the footer covers the preamble, header and index; each index entry covers its filter, block or row-group region; each directory entry covers its value record. The reader verifies each before decoding it, so a corrupt byte is `Error::Corrupt`, never a wrong answer. A storage version 1 segment (24-byte footer, no checksums) is still read; this build writes only version 2.
- **Codec.** The codec is `"none"` in storage versions 1 and 2, and any other codec is refused.
- **Recorded types.** A segment built with `for_dataset` records each key column's dataset type (`key_types`) and, for temporal specs, the timestamp column's type and unit (`ts_type`). The spelling is Bellwether's own `ColumnType`, for example `{"type":"decimal128","precision":10,"scale":2}` or `{"type":"timestamp","unit":"millisecond","tz":"UTC"}`, so the frozen format does not depend on Arrow's serde. A dictionary column is recorded as its value type. Keys encoded under different types would split silently, so:
  - segments whose recorded types differ never merge, and the temporal fold refuses them
  - `SegmentReader::open_for_dataset` (or `check_dataset`) refuses a segment whose recorded types differ from the dataset schema's, or which records none
  - a segment written from already-encoded keys (`SegmentWriter::new`) records no types
- **Merge output versions.** Merge writes its output with this build's storage and encoding versions, whatever versions its inputs carried.
- **Recorded row-group range.** The index prelude's `has_rg | rg_min | rg_max` is the range of row groups the writer processed, inclusive, when the caller sets it with `SegmentWriter::set_rowgroup_range`. A builder that claims coverage by this range must set it. Otherwise it is the smallest range holding every row group a key references.
  - With the range set, a segment whose row groups held no keys still records them, and two segments that processed different row groups never share bytes, and so never share an id.
  - Every referenced row group must lie inside a set range; `finish` refuses otherwise.
  - Merge records the hull of its inputs' ranges. For inputs that record only their keys' row groups, that is the same range as before.
- **Footer-located index.** The emitters write blocks in key order as they fill, so merge never builds an in-memory map of its output keys. Opening by range needs four reads: preamble, header, footer, index. Allocations made from on-disk counts are capped by the bytes actually present.
- **One layout per key-spec kind:**
  - **Exact.** Sorted key blocks of at most 1024 keys or 64 KiB of key bytes. Each block is written as:
    1. its value records
    2. an SBBF over its canonical keys at 1% FPP
    3. a key directory: `n`, `n+1` end offsets, the key bytes, then `n` × `(value_offset u64, value_len u32, value_checksum u64)`

    A value is `bitmap_len u32 | bitmap`, a roaring bitmap over global `RowGroupId`, always in the no-run serialized form (cookie `3a 30 00 00`) and never empty. A reader refuses any other cookie, an empty bitmap, or a bitmap that does not fill its length, so corruption is an error rather than a wrong or empty answer. (A version 1 exact value is the bare bitmap.)

    **Counts.** For a spec with `counts` (`exact:a,counts`), the bitmap is followed by one shortest-form LEB128 row count per row group in it, ascending, each at least 1: the rows of that row group whose key tuple is this key (`SegmentWriter::insert_rows`). `SegmentReader::counts(keys)` returns them, or `None` for a segment that stores none, never zeros. Merge sums the counts of one (key, row group) only across inputs over disjoint row groups and refuses otherwise.

    **Bounds (M5).** For an exact spec with `bounds` (`exact:a@ts`, `exact:a@ts,counts`), one `min i64 | max i64` per row group follows the counts: the bounds column's range over the key's rows there, in the column's dataset unit, or the all-null sentinel `(i64::MAX, i64::MIN)` (`ALL_NULL_BOUNDS`) where every such row has a null time (`SegmentWriter::insert_timed`). `SegmentReader::bounds(keys)` leaves the sentinel out, so it overlaps no window; `get` returns it as stored. Merge widens one (key, row group)'s range across inputs, the sentinel absorbing into any real range. The header records the bounds column's type in `time_types`.

    **A bloom's bounds (M5).** A bloom has no per-key record, so a bloom with `bounds` (`bloom:a@0.01@ts`) keeps them in a second segment, its `bounds` segment (`SegmentWriter::bloom_bounds_for_dataset`): the header names the bloom spec with layout `keyed` (`Layout::admits`), each value record is `bitmap_len u32 | bitmap | bounds` (the exact record without counts, `ValueShape::of(spec, layout)`), checksummed in its directory entry like every keyed record. The manifest lists it with `kind: "bounds"` under the bloom's identity. Merge refuses to merge it with the bloom's filters.
  - **Temporal.** The exact layout, except that each value is the length-prefixed bitmap, then the counts when the spec stores them (rows over all versions, null times included), then a version chain: `count u32 | has_max u8 | max_ts i64 | count × (ts i64, rg u32)`, ascending and deduplicated. The bitmap includes row groups whose rows have a null timestamp, because membership is still true. The chain leaves them out, because they never win.
  - **Bitemporal (M5, `temporal2:a@valid,known`).** The exact layout, each value the length-prefixed bitmap and `count u32 | count × (valid i64 | known i64 | rg u32)`: the key's distinct versions over rows where neither time is null, strictly ascending, their row groups exactly the bitmap (`SegmentWriter::insert_triple`; `SegmentReader::triples`). A key with no version is not stored. Merge unites and deduplicates the versions. The header records both clocks' types in `time_types`.
  - **Bloom.** One SBBF per row group over that row group's distinct keys, sized from the spec's `fpr` with Parquet's formula. Merge ORs the filters of one row group when they are the same size and keeps them as separate entries otherwise, and a lookup unions over them.
  - **Range.** One `(min, max)` per row group, sorted by `(min, max, rg)`, in blocks of 256 entries. Bounds may come from statistics, which exclude NaN, so a probe that includes the canonical NaN matches every row group. Signed zero follows the Parquet statistics reading rule: a stored minimum of +0.0 is widened to -0.0 and a maximum of -0.0 to +0.0, on write and on merge, so a probe for either zero never prunes a row group holding the other.
- **Lookups.**
  - `lookup` takes canonical keys.
  - `lookup_range` compares full encoded keys.
  - `lookup_prefix` selects every extension of a leading-column prefix.
  - `lookup_prefix_range(lo, hi)` selects keys whose leading columns lie between two prefixes, inclusive. This is `Between` on the leading column of a composite key.
- **Temporal results are index-wide.** `temporal::latest` and `temporal::asof` fold every segment of the key spec with `VersionHit::combine`. `SegmentReader::latest` and `SegmentReader::asof` answer for one segment only. Ties are kept: a `VersionHit { ts, row_groups }` holds every row group with a version at the winning timestamp. A key whose versions all have null timestamps has no `latest`. `ts` and `asof`'s argument are in the dataset column's unit.

## List keys (B4, `list:<column>`; `docs/format.md`, "List keys")

`list` holds the inverted index over a list column: per distinct element (its canonical encoding at the dataset's element type), the row groups holding a row whose list contains it, in the exact layout unchanged. A column qualifies when it is a `List<T>` or `LargeList<T>` of a type with a canonical encoding, or a `Map<K, V>` by its keys (`list::check_list_column` says why anything else does not). `SegmentWriter` adds a row group's distinct elements; a null list, an empty list, and a null element add nothing (ADR 014); a duplicate element is one key.

- `list::contains_any(readers, values)`: the union of each value's row groups over every segment of the key.
- `list::contains_all(readers, groups)`: the intersection, one group per queried value (both encodings of a float zero in one group); no group at all is refused.
- `list::distinct_elements(readers)`: the exact distinct elements over every segment, merged in key order so an element in several segments counts once; holds one block per segment. `why` reports it as the `list` column.
- `list::row_element_keys(array, row, element)`: one row's element encodings, for the residual (`Predicate::ContainsAny` / `ContainsAll`).

Tests (`tests/list.rs`): round trip, the pinned conformance vectors, nulls and empty lists, float zero and NaN, type conversion and refusals, an every-byte corruption sweep, and a property test of both lookups and `distinct_elements` against a brute-force oracle over random multi-segment layouts with duplicates.

## Lookup cost and memory bound (ADR 018)

`SegmentReader::open` holds the header and the block index, which is two keys and five integers per block of up to 1024 keys. A keyed lookup of `k` keys:

1. sorts and deduplicates the keys
2. binary-searches the block index for each key
3. probes that block's bloom
4. binary-searches the block's key directory only when the bloom passes

That is O(k log n). The lookup holds the index, one block at a time, and the bitmaps or version hits for the keys asked. `tests/segment_format.rs::single_key_lookup_reads_one_block_and_one_value` checks this on a 200k-key segment: one present key costs exactly three range reads (bloom, directory, value).

The one-index-plus-one-block bound is a lookup bound. Building is bounded differently (`docs/format.md`, build-side memory):

- `SegmentWriter` holds the segment's distinct keys until `finish`. Version chains are buffered and compacted in amortized O(n log n), never by quadratic insertion.
- Every writer, merge included, buffers the whole output segment's bytes, because the segment id is the blake3 of those bytes.
- Merge reads one block per input at a time.

There is no `unsafe` in this crate, so it does not create memory maps itself. A caller can hand `SegmentReader` any `AsRef<[u8]>`, such as a `bytes::Bytes` or a map it created, and the reader borrows ranges from it without copying. `FileRange` reads each range with a positioned read.

Merging segments whose processed ranges are not adjacent records the hull of their ranges, which claims row groups none of the inputs processed. The build crate merges only contiguous groups and takes coverage from the manifest, so it is unaffected; any other caller must merge adjacent ranges only or set the range explicitly after the merge.

## Sidecar segment format (`docs/format.md`, storage version 1)

Since M5 no build writes this layout: per-key bounds of an exact spec, a bloom's `bounds` segment and bitemporal versions are the storage version 2 layouts above. An index written before M5 holds both kinds as sidecars, which read as before until its first build with work folds them. Sidecar segments (`segments/<blake3>.seg`, magic `"BWSIDECR"`) store per-key timestamp metadata in two kinds:

- `bounds` (`SidecarKind::Bounds`, named `bounds:<cols>@<ts>`): per key, per row group holding the key with a non-null timestamp, the inclusive `[min, max]` of `ts` over that key's rows in that row group.
- `temporal2` (`SidecarKind::Bitemporal`, named `temporal2:<cols>@<valid>,<known>`): per key, the sorted distinct `(valid, known, rg)` triples over rows where neither clock is null (ADR 014). `bitemporal_winner` selects, among triples with `known <= known_at` and `valid <= valid_at`, the greatest `valid`, breaking ties by greatest `known`, and keeps every row group holding that `(valid, known)`.

All integers are little-endian and `i64` values are two's complement:

```
preamble  magic "BWSIDECR" | storage_version u32 | encoding_version u32 | header_len u32
header    JSON {storage_version, encoding_version, kind, columns, time_columns, codec:"none", key_types?, time_types?, ..unknown}
body      blocks of <= 1024 keys or 64 KiB of key bytes; block = n u32 | n x (key_len u32 | key | count u32 | count x record)
          bounds record    = rg u32 | min i64 | max i64        (strictly ascending rg, min <= max)
          temporal2 record = valid i64 | known i64 | rg u32    (strictly ascending (valid, known, rg))
index     entries u64 | has_rg u8 | rg_min u32 | rg_max u32 | block_count u32 | block_count x (first_key | last_key | off u64 | len u32 | n u32)
footer    index_offset u64 | index_len u64 | magic "BWSIDECR"
```

Sidecar memory bound (ADR 018): a lookup holds the block index, one block at a time, and the results for the keys asked; `SidecarWriter` holds every distinct `(key, rg)` (for `bounds`) or `(key, valid, known, rg)` (for `temporal2`) until `finish`, and the build crate caps segment size with `sidecar::measure`.

## Sketch segment format (`docs/format.md`, "Sketch segments")

A `sketch` segment (`segments/<blake3>.seg`, magic `"BWSKETCH"`) holds, per sketched item (a column or a `+`-joined tuple, encoded like a composite key, ADR 014) and per file, the row count, the null count, the HyperLogLog registers (sparse or dense), and optionally a MinHash signature of `k` slots. It is a sidecar declared in `schema.sidecars[]` with kind `sketch`; **no planner path reads it**: every number it yields is approximate (`ApproxCardinality`, `ApproxOverlap`, `approximate: true`), and an approximate number in a plan would break invariant 1. The build crate's planner never opens these segments (its `tests/sketch.rs` proves plans are equal with and without them).

A footer checksum (BLAKE3 over every byte before it) makes every damaged byte a typed refusal rather than a silently different estimate. Merging is exact: registers by maximum, slots by minimum, so the union of a file's stretches equals sketching the file whole. Intervals are three standard errors (`SKETCH_CONFIDENCE`) either side, clamped to `[1, rows]`.

Sketch memory bound (ADR 018): a writer holds one open entry (`2^p` registers and `k` slots per item) and the finished entries encoded; `max_entry_bytes` bounds one entry so the build caps a segment's entries and bytes. A reader holds the segment bytes and decodes one entry at a time; `SketchTotals` holds `2^p` registers and `k` slots per item, whatever the file count.
