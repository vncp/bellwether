# bellwether-source

WP2. Implements `bellwether_types::Source` for Parquet and for CSV / JSON lines, plus file fingerprinting. Owns ADR 007 (Parquet-native) and the fingerprint half of ADR 013.

## What it provides

| Item | Purpose |
|---|---|
| `ParquetSource<S = LocalFs>` | Real row groups. The footer is parsed once per fingerprint and cached. `row_groups` reports rows, compressed bytes, and per-column `ColumnStats`: canonical min/max, null count, distinct count when the writer recorded one, and `has_bloom` when the footer declares a bloom filter for that column chunk. `read_columns` decodes only the requested column chunks of one row group. |
| `ChunkedTextSource<S = LocalFs>` | CSV (`TextFormat::Csv`) and JSON lines (`TextFormat::JsonLines`). Row groups are newline-aligned byte-range chunks, 64 MiB by default (`with_chunk_size`). The first `row_groups` call per file scans each chunk once, counting rows and computing statistics for `with_stats_columns(..)`, and caches the result. |
| `with_schema(SchemaRef)` (both sources) | Supplies the dataset schema. `schema(root)` returns it, and statistics are encoded as its column types. For text sources it replaces inference when parsing, for both formats. |
| `fingerprint(path)`, `fingerprint_with(storage, path)`, `fingerprint_with_policy(storage, path, policy)` | `(path, size, etag_or_mtime, blake3(content))` per `docs/format.md`. Metadata and content hash both come from one opened handle. |
| `FingerprintPolicy { Tail64K, Full }`, `with_fingerprint_policy` (both sources) | Which bytes the content hash covers: the last 64 KiB (default) or the whole file, streamed. This is `bellwether.toml`'s `fingerprint_policy`. A fingerprint and every verification against it must use the same policy. Under `Full`, `FileFingerprint::tail_blake3` carries the whole-file hash. |
| `drift(old, new) -> bool` | True when any fingerprint component differs. |
| `catalog::{digest, indexed, unindexed, schema_identity}` | The catalog digest (`docs/format.md`, "Catalog digest"): the identity of the data a reader saw — the dataset schema and every row group's id and report — from a manifest (`indexed`) or from discovered files at manifest version `"0"` (`unindexed`). The replay crate's `Catalog` computes its digest with `digest` too, so there is one definition. The type spelling comes from the caller (core's `ColumnType`), since this crate does not depend on core. |
| `stats::{overlap_depth, time_correlation, batch_stamp, partner_order, cap_distinct, is_row_id}` | Decisions over row-group statistics, defined once so that the profiler's lints and the benchmark's planted flags compute them identically (`docs/rule-catalog.md` states the definitions and thresholds). <ul><li>`overlap_depth` gives a date, timestamp, or integer column's overlap depth (covered length over union length, with up to 4 sentinel ranges excluded) and its within-file sortedness.</li><li>`time_correlation` gives a column's cross-day concordance with a time column, or `None` over fewer than two days.</li><li>`batch_stamp` is the whole `batch_stamp_key` decision: a files gate, a ratio against a composite, and concordance, every constant defined there.</li><li>`partner_order` chooses the composite partner both composite rules judge against (a row id refused unless the workload supplies it), with `is_row_id` and `cap_distinct` (an estimate capped at the row count).</li></ul> |
| `Storage`, `LocalFs` | The byte-access seam: `list`, `head`, `read_range`, and `open`. `open` returns a parquet `ChunkReader` **plus the metadata of the opened handle**. Only the local filesystem is implemented. |
| `canonical::{Canonical, DefaultCanonical}` | The canonical encoder used for `min`/`max` bytes (see below). |
| `text::csv_ends_in_quotes(bytes, delimiter)` | csv_core's quoting rules as a scanner: whether a chunk parsed from a record boundary ends inside a quoted field. |
| `is_within(path, dir)`, `ChunkedTextSource::with_excluded(dir)` | Whether a listed path is `dir` or below it, split at the platform's separators (`\` as well as `/` on Windows; a Unix name may hold `\`). Text discovery skips an excluded directory (an index inside the data root), and the facade's folder check uses the same test. |
| `ParquetSource::column_bytes` | Compressed column-chunk bytes per row group and column, ready for the planned `ColumnStats.bytes` field. |
| `ChunkedTextSource::byte_range(rg)` | A text row group's byte range, ready for the planned `RowGroupMeta.byte_range` field. |

## Semantics worth knowing

- **Encoding as the dataset type.** Statistics follow the ADR 016 table in `docs/format.md`, encoding version 1. Every value is encoded as the dataset column type: the `with_schema` type, or the file's own type when none is given.
  - Decimals rescale exactly to the dataset precision and scale. The width comes from the dataset type (`05 10` + 16 bytes, or `05 20` + 32 bytes).
  - Times and timestamps convert exactly to the dataset unit (`08 <kind> <unit>` + i64), so year-9999 sentinels in ms/µs columns do not overflow.
  - The tag always comes from the dataset type. Ints are range-checked against it, and Float32/Float16 dataset types require exact representability.
  - A value that cannot be represented exactly makes that bound unknown (`None`), never a rounded value. The same applies to types with no encoding (duration, interval, nested). Unknown is always safe for pruning.
- **Bounds, not values.** `min`/`max` are bounds; inexact Parquet statistics are accepted and `is_*_exact` is not consulted.
  - Float bounds exclude NaN (the Parquet writer convention, applied to text too), so a NaN probe must never be pruned by range statistics. An all-NaN row group has unknown bounds.
  - Parquet float bounds follow the format's reading rules: a NaN bound becomes unknown, a min of +0 widens to -0, and a max of -0 widens to +0. The canonical encoding keeps ±0 distinct, so this is needed for an attained -0.0 to stay inside a +0.0 lower bound.
  - Deprecated Parquet `min`/`max` fields are used only when the column's sort order is SIGNED. For files without column orders, the order is derived from the logical type, so old parquet-mr UTF8/UINT statistics are dropped.
  - Deprecated `min`/`max` are **never** used for `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` columns, even signed ones such as FLBA decimals. Old parquet-mr compared those bytes as signed, and PARQUET-251 wrote corrupt binary statistics.
  - `ColumnStats` in `bellwether-types` should carry this note when the contract is next revised; this crate does not modify it.
- **Unrequested text columns.** `ColumnStats` is reported for every schema column in schema order. Columns outside `stats_columns` have `min`, `max`, and `null_count` set to `None`.
- **Chunk boundaries.** Chunk `k+1` starts just after the first `\n` at or after `start_k + chunk_size - 1`. Chunks are contiguous, cover the file, never split a line, and every chunk but the last is at least `chunk_size` bytes.
  - A chunk longer than 4× `chunk_size` (a very long line, or CR-only line endings) is an `Error::Source`, never an unbounded read.
- **Chunk size is identity.** Boundaries depend on the file bytes and `chunk_size`.
  - `read_columns` errors unless the recomputed chunk has exactly `rg.bytes` bytes and decodes to exactly `rg.rows` rows. This catches many chunk-size mismatches, **but not all**.
  - **Residual gap until `RowGroupMeta.byte_range` lands.** Length and row count do not pin the chunk's start offset. Collisions are common for fixed-width lines, not one unlucky alignment: on 10-byte JSON lines the reviewer measured 1,496 of 32,220 (build, read) chunk-size pairs that return the wrong rows without an error. One example is a 720-byte file built at chunk size 64 and read at 128, where row group 5 (bytes 350..420) and the last 128-size chunk (650..720) are both 70 bytes and 7 rows.
  - Only a recorded byte range closes this, and `read_columns` must compare both its start and its end once it exists (`byte_range()` is ready). **WP4 must build the source with the chunk size recorded in the manifest and persist each row group's byte range** (`docs/format.md` already records `chunk_size` and `chunks`).
- **CSV header guard.** With `with_schema` and a header row, the header names must equal the schema's names in order. CSV maps columns by position, so a reordered header would otherwise silently swap columns of the same type.
- **Quoted newlines are detected.** Each CSV chunk is scanned with csv_core's quoting rules, the parser arrow-csv uses (`csv_ends_in_quotes`):
  - A quote opens a quoted field only at the start of a field.
  - Inside a quoted field, `""` is an escaped quote, a quote followed by the delimiter or a terminator (`\r`, `\n`) closes the field, and a quote followed by any other byte drops back to an unquoted field.
  - A quote in the middle of an unquoted field (an inch mark, `5'10"`) is literal.

  A chunk that ends inside a quoted field is rejected with `Error::Source`, never parsed into phantom rows. Chunks start at record boundaries by induction, so a chunk that passes parses to exactly the rows the whole file would. `tests/chunks.rs` checks this differentially against arrow on randomized inputs at every chunk size, including the reviewer's inch-mark file at all 38 chunk sizes.
- **Text typing.** With `with_schema`, both formats parse with that schema; for example `ts` is a timestamp in CSV and JSON lines alike. Without it, the schema is inferred per file from a newline-aligned head of at most about 1 MiB (1000 records), independent of chunk size, and inferred types **may differ between formats**: CSV infers ISO timestamps as `Timestamp`, while JSON lines keeps them as `Utf8`. A later chunk whose values do not fit fails with `Error::Source`.
- **Drift safety.** Every footer or layout load, and every `read_columns` call, first opens the file and verifies the fingerprint on the opened handle: size, etag/mtime, and the content hash under the source's `FingerprintPolicy`. Reads then go through that same handle.
  - This catches a stale cache serving a rewritten file whenever the rewrite changes the size, the mtime/etag, or the hashed bytes (`tests/fingerprint.rs` covers a same-size, same-mtime rewrite inside the tail).
  - A rename-replace between check and read is caught too.
- **Drift limits (inherent to ADR 013).**
  - **Tail coverage.** Under the default `Tail64K`, the content hash covers only the last 64 KiB. An in-place patch outside it that keeps the size and mtime is **not** detected. `drift()` returns false and `read_columns` serves the patched bytes (reproduced at offset 109 of a 480 KB file). `FingerprintPolicy::Full` detects it (`full_policy_catches_a_patch_outside_the_tail`).
  - **Inode, not bytes.** Verifying the opened handle pins the inode (or object version), not its bytes. An in-place write to the same inode during a read goes undetected under either policy.
  - **Cost.** Each `read_columns` re-verifies. Under `Tail64K` that is one extra 64 KiB read, which on object stores is **one extra ranged GET per `read_columns`**. Under `Full` it is a streamed read of the whole file, so reserve `Full` for small or critical files.
- **mtime.** For local files, `FileFingerprint.etag` is the mtime as RFC 3339 UTC with nanosecond precision. Touching a file without changing its bytes therefore counts as drift.
- **Discovery.** Recursive and sorted. Names starting with `.` or `_` (such as `_SUCCESS`) are skipped. Parquet lists `*.parquet`, CSV lists `*.csv`, JSON lines lists `*.jsonl` and `*.ndjson`, and also `*.json` when the file's first byte is `{` and every line (blank lines aside) parses as one JSON object (`sniff_json_lines`). A `*.json` file that starts otherwise, or whose first line is not one object (a pretty-printed document such as an index's own `manifest.json`), is not listed. One whose first line is one object but a later line is not is JSON lines with a damaged line, and is refused by path and line number rather than left out of the dataset. A folder with no file of the format is refused with the accepted extensions (`TextFormat::accepted`). Without `with_schema`, `schema(root)` uses the first file in sorted order.
- **`read_columns` returns file-native types.** With `with_schema`, Parquet `read_columns` returns each file's own Arrow types, while `schema(root)` reports dataset types. A consumer that encodes batch values (WP4 keys, WP8 row hashes) must encode them **as the dataset type**, `canonical::encode_as(array, row, dataset_type)`. Otherwise a value from a scale-2 decimal file will not match stats or probes at the dataset's scale 4.
- **Unknown columns** in `read_columns` return `Error::KeySpec`. The returned batch has exactly the requested columns, in the requested order. An empty request returns a zero-column batch carrying the row count.
- **Partition columns (`with_hive_partitions(root)`).** Every `name=value` directory below the data root is a column of the files beneath it (`docs/format.md`, "Partition columns"). `row_groups` reports it with `min = max = value`, zero bytes, and a null count of zero (`__HIVE_DEFAULT_PARTITION__` is a null: all rows null); `read_columns` synthesizes it (one value per row, at the dataset type), and a request for only partition columns decodes no data page. A column the dataset schema declares takes that type by an exact cast, and a value that does not cast is refused. Without a dataset schema the type is inferred over **every** value under the root, by the source's own fixed table, never by the first file and never by a dependency's inference (an Arrow upgrade cannot retype a column): `Int64` if every non-null value is a canonical integer (`-7`, `0`, `42`, but not `007` or `+7`), else `Date32` if every one is a valid `YYYY-MM-DD`, else `Utf8`. File columns are typed by their footers, so no string inference exists for them to share; this table is the partition columns' equivalent. A file that also stores the column must show, in every row group's footer statistics, the same value with no nulls; anything else (including unknown statistics) is refused. A root's own `name=value` components, a file not under the root, an empty name or value, a repeated name, a bad `%` escape, and files with different partition columns under one root are refused. Text sources do not take partition columns in this release.

## Guidance for WP4 (build) and other consumers

- **Always supply the dataset schema with `with_schema`.** It fixes the column types used for statistics encoding, parses CSV and JSON lines identically, and enables the CSV header check. Inference is for exploration only: inferred types can differ between files and between formats.
- **Build each text source with the `chunk_size` recorded in the manifest**, and persist each row group's byte range. Compare both ends on read once `RowGroupMeta.byte_range` exists.
- **Construct sources with the manifest's `fingerprint_policy`** (`with_fingerprint_policy`). Fingerprints and verifications must agree on it.
- **Encode batch values as the dataset type**, with `canonical::encode_as(array, row, dataset_type)`, not the file type.

## Memory bound (ADR 018)

- Parquet `row_groups` holds one footer.
- Parquet `read_columns` holds the projected column chunks of one row group, decoded into a single batch. `read_column_batches` (`Source`, WP22b) decodes the same chunks at most `max_rows` rows at a time and hands each batch over before decoding the next, so a caller holds one batch whatever the row group's width; reconcile reads this way (8,192 rows). Text and SQL sources stream it too; the trait's default hands over one whole row group.
- Text `row_groups` holds one chunk's raw bytes (at most 4× `chunk_size`) and one decoded batch of at most 8192 rows, projected to the stats columns.
- Text `read_columns` holds one chunk's raw bytes and its projected columns.
- Caches are bounded by bytes, with FIFO eviction: footers 256 MiB (`ParquetMetaData::memory_size`), text layouts 64 MiB, and text scans 64 MiB.
- Fingerprinting and verification hold at most 64 KiB (`Tail64K`) or one 1 MiB window (`Full`, which streams the file through BLAKE3).

**What "only the projected chunks" means physically.** The decoder consumes only the projected chunk's bytes (`tests/projection.rs` asserts every consumed range lies inside it). `LocalFs`'s `File` reader uses a `BufReader` with 8 KiB read-ahead, so the OS may read up to one buffer past a chunk. An object-store `Storage::Reader` must implement `get_read(start)` as bounded ranged reads, never as one open-ended GET to EOF.

## Fixtures: `fixtures/source/`

Small files with known statistics. They are intended for every crate, not only this one.

| File | Exercises |
|---|---|
| `multi_rg.parquet` | 3 row groups (4, 4, 2 rows); `Int64`, `Utf8`, `Decimal128(10,2)` |
| `timestamps.parquet` | `Timestamp(µs, "UTC")` (unsorted) and `Date32`; 2 row groups |
| `nulls.parquet` | nulls in the `key` column, including a row group where `key` is all-null; `UInt32`, `Float64` with negatives |
| `bloom.parquet` | embedded split-block bloom filter on `sku` (`has_bloom = true`), none on `id` |
| `edge.parquet` | NaN in `Float64`, including an all-NaN row group; `UInt32` ≥ 2^31; non-ASCII `Utf8`; `Decimal128(38,4)` (FIXED_LEN_BYTE_ARRAY) |
| `events.csv` | 12 rows with header, a null `name`, no trailing newline |
| `events.jsonl` | the same rows as JSON lines, with `name` omitted for nulls |
| `expected.json` | row-group stats for all seven files. Text files are indexed with `chunk_size = 64`, the dataset schema `id: Int64, name: Utf8, amount: Float64, ts: Timestamp(µs)`, and all columns as stats columns. |

`min`/`max` in `expected.json` are lowercase hex of the canonical bytes. For the Parquet files, rows and statistics are computed from the in-memory data, independently of the Parquet footer. For the text files, `expected.json` is a golden snapshot cross-checked by hand-written assertions. To regenerate (deterministic, byte-identical):

```text
cargo test -p bellwether-source --test generate_fixtures -- --ignored
```

## Known limitations

- **Unify the encoder.** The canonical encoder in `src/canonical.rs` is a local copy that must be unified with `bellwether-core::encoding` at integration. The byte values that `docs/format.md` leaves open were agreed with WP1:
  - Decimal width tag: `0x10` / `0x20`.
  - Temporal time-unit tag: `0x01` s, `0x02` ms, `0x03` µs, `0x04` ns, `0x05` day. Date32 always uses `0x05` and Date64 always uses `0x02`.
  - Conversion rules: the tag comes from the dataset type; ints are range-checked; float narrowing must be exact; `FixedSizeBinary(n)` requires length n; no Utf8↔Binary conversion; Date32↔Date64 converts exactly; the zone is ignored.
  - Vector pinned on both sides: `Timestamp(µs) 1 = 08 04 03 80 00 00 00 00 00 00 01`.

  Swap the encoder in through `with_canonical`, or replace `DefaultCanonical`.
- **Quoted newlines** in CSV are rejected, not supported (see above).
- **Async readers.** Only `LocalFs` is implemented, and the reader is synchronous (`ChunkReader`). An async `AsyncFileReader` path for object stores arrives with `bellwether-store`.
- **Bloom probing.** `has_bloom` reports presence only. Probing an embedded filter must hash Parquet's PLAIN physical encoding, not canonical bytes. Use `bellwether_core::bloom::parquet_plain::{hash_i32, hash_i64, hash_f32, hash_f64, hash_bytes}`: xxh64 with seed 0 over the PLAIN little-endian value, or the raw bytes for a byte array.
- **Never copy an embedded Parquet filter into a bellwether bloom segment.** Segments hash canonical bytes, so a copied filter would give silent false negatives (invariant 1). An embedded filter can only be probed in place, with the PLAIN hashes above.
