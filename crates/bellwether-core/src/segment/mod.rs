//! Segment files: `segments/<blake3>.seg` (docs/format.md, storage version
//! 2; version 1 segments are read, never written).
//!
//! # Layout
//!
//! All integers are little-endian. A length-prefixed byte string is a `u32`
//! length followed by the bytes.
//!
//! ```text
//! preamble   magic "BWSEGMNT" | storage_version u32 | encoding_version u32 | header_len u32
//! header     JSON {storage_version, encoding_version, key_spec, codec, layout, bloom_hash,
//!                  key_types?, ts_type?, ..unknown}
//! body       layout-specific blocks (below)
//! index      entries u64 | has_rg u8 | rg_min u32 | rg_max u32 | block_count u32 | entry*
//! footer     index_offset u64 | index_len u64 | checksum u64 | magic "BWSEGMNT"
//! ```
//!
//! The footer's checksum ([`checksum`], XXH3-64) covers the preamble, the
//! header and the index; every index entry carries the checksum of the
//! filter, block or row-group region it points at, and every directory
//! entry the checksum of its value record. A reader verifies each before
//! decoding it. A storage version 1 segment has the 24-byte footer
//! ([`FOOTER_LEN`]) and no checksums.
//!
//! `has_rg | rg_min | rg_max` is the row-group range the segment records:
//! the row groups its writer processed, inclusive, when the writer was
//! told ([`SegmentWriter::set_rowgroup_range`]; merge records the hull of
//! its inputs' ranges), otherwise the smallest range holding every row
//! group a key references. A builder that claims coverage by a range sets
//! it, so a segment whose row groups held no keys still records them, and
//! two segments over different row groups never share bytes or an id.
//!
//! The versions sit in the fixed preamble so a reader refuses a newer file
//! with `Error::UnsupportedVersion` before parsing anything else (ADR 017);
//! the header repeats them and must agree. The only codec in version 1 is
//! `"none"`; any other is refused. Every header carries `bloom_hash`, naming
//! what the segment's filters hash (ADR 008 amendment, `docs/format.md`):
//! `canonical` for keyed and keyed-temporal block filters, `canonical` or
//! `parquet_plain` for bloom segments, and `canonical` for range segments,
//! which have no filters. A missing or unknown value is refused on open.
//!
//! A segment written from a dataset schema ([`SegmentWriter::for_dataset`])
//! records the dataset type of each key column (`key_types`) and, for
//! temporal specs, of the timestamp column with its unit (`ts_type`), in the
//! core-owned [`ColumnType`] spelling. Keys encoded under different types
//! would split silently, so segments whose recorded types differ never merge
//! or fold together, and [`SegmentReader::open_for_dataset`] refuses a
//! segment whose recorded types differ from the dataset's. Merge writes its
//! output with this build's storage and encoding versions.
//!
//! **Keyed** (exact) and **keyed temporal** (temporal). Keys are sorted
//! canonical encodings, grouped into blocks of at most
//! [`WriterOptions::block_keys`] keys. Each block is written as its value
//! records, then its SBBF bitset over the block's keys, then its key
//! directory: `n u32 | (n+1) key end offsets u32 | key bytes | n x
//! (value_offset u64, value_len u32, value_checksum u64)`. An index entry is
//! `first_key | last_key | bloom_off u64 | bloom_len u32 | bloom_checksum u64
//! | body_off u64 | body_len u32 | body_checksum u64 | n u32`. A value record
//! is `bitmap_len u32 | bitmap`, a roaring bitmap over global `RowGroupId`;
//! then, for a spec with `counts`, one shortest-form LEB128 row count per
//! set bit in ascending row-group order; then, for temporal segments, the
//! version chain (see [`crate::temporal::VersionChain::write_to`]). The
//! bitmap is always in roaring's no-run serialized form (cookie
//! `3a 30 00 00`), fills its bytes exactly, and is never empty; a reader
//! refuses anything else. (Version 1: no checksums, and an exact value
//! record is the bare bitmap.)
//!
//! **Bloom per row group** (bloom). One SBBF per row group over that row
//! group's distinct keys, sized from the spec's `fpr`; structure and sizing
//! are those of a Parquet embedded filter. Index entry: `rg u32 | off u64 |
//! len u32 | ndv u64 | checksum u64`. A row group may have more than one entry (merge keeps
//! filters of different sizes); a lookup unions over them.
//!
//! **Min/max** (range). One `(min, max)` per row group, sorted by
//! `(min, max, rg)`, in blocks of `rg u32 | min | max` entries. Index entry:
//! `first_min | max_of_max | off u64 | len u32 | n u32 | checksum u64`. Bounds may come
//! from statistics, which exclude NaN, so a probe that includes the
//! canonical NaN matches every row group. Stored bounds follow the Parquet
//! signed-zero reading rule: a +0.0 minimum is stored as -0.0 and a -0.0
//! maximum as +0.0, so a probe for either zero never prunes a row group
//! holding the other.
//!
//! # Lookups
//!
//! [`SegmentReader::lookup`] takes canonical keys. Range-style lookups
//! compare full encoded keys ([`SegmentReader::lookup_range`]), select every
//! extension of a leading-column prefix ([`SegmentReader::lookup_prefix`]),
//! or select keys whose leading columns lie between two prefixes
//! ([`SegmentReader::lookup_prefix_range`]). Temporal `latest` and `asof` on
//! one reader answer for that segment only; the index-wide answer is
//! [`crate::temporal::latest`] and [`crate::temporal::asof`] over every
//! segment of the key spec.
//!
//! # Lookup cost and memory (ADR 018)
//!
//! A keyed lookup of k keys sorts them, then per key does a binary search
//! of the block index, a bloom probe of that block, and a binary search of
//! the block's directory: O(k log n). It holds the index, one block, and
//! the bitmaps for the keys asked. Bloom lookups probe every row group's
//! filter, one at a time. Range lookups walk blocks in `min` order and stop
//! at the first block whose smallest `min` exceeds the query.

mod format;
mod reader;
mod writer;

pub use format::{
    checksum, footer_len, types, ColumnType, FileRange, Layout, ReadAt, RecordedUnit,
    SegmentHeader, CODEC_NONE, FOOTER_LEN, FOOTER_LEN_V2, MAGIC, PREAMBLE_LEN,
};
pub use reader::SegmentReader;
pub use writer::{
    segment_path, validate_spec, EncodedSegment, KeyValue, SegmentWriter, WriterOptions,
};

pub(crate) use reader::{BlockView, Index};
pub(crate) use writer::{stored_bounds, BloomEmitter, KeyedEmitter, RangeEmitter};
