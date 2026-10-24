//! Core index structures for Bellwether.
//!
//! - [`encoding`]: the canonical memcomparable byte encoding (ADR 002, 016).
//! - [`bloom`]: split-block bloom filters per the Parquet specification
//!   (ADR 008).
//! - [`segment`]: the `segments/<blake3>.seg` writer and reader; key to
//!   roaring bitmap over global `RowGroupId` (ADR 001).
//! - [`temporal`]: per-key version chains, `latest` and `asof` (invariant 5,
//!   ADR 014).
//! - [`merge`](mod@merge): merge N segments of one key spec into one.
//!
//! Invariants owned here: 1 (no false negatives in pruning,
//! `tests/no_false_negatives.rs`) and 5 (stale-version rejection,
//! `tests/temporal_stale_rejection.rs`).
//!
//! Memory bound (ADR 018): a lookup holds one segment's block index, one
//! block at a time, and the bitmaps or version hits for the keys asked.

#![forbid(unsafe_code)]

pub mod bloom;
pub mod encoding;
pub mod merge;
pub mod segment;
pub mod temporal;

pub use merge::merge;
pub use segment::{EncodedSegment, SegmentReader, SegmentWriter, WriterOptions};
pub use temporal::{VersionChain, VersionHit};
