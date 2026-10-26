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
//! - [`sidecar`]: `bounds` and `temporal2` sidecar segments.
//! - [`hll`]: the HyperLogLog sketch the profiler and the `sketch` sidecar
//!   share.
//! - [`sketch`]: `sketch` sidecar segments (per item, per file HyperLogLog
//!   registers and MinHash signatures). Approximate; no planner reads them.
//!
//! Invariants owned here: 1 (no false negatives in pruning,
//! `tests/no_false_negatives.rs`) and 5 (stale-version rejection,
//! `tests/temporal_stale_rejection.rs`).
//!
//! Memory bound (ADR 018): a lookup holds one segment's block index, one
//! block at a time, and the bitmaps, version hits, bounds, or triples for the
//! keys asked.
#![forbid(unsafe_code)]
// Proper names (HyperLogLog, MinHash, BLAKE3) read better without backticks.
#![allow(clippy::doc_markdown)]

pub mod bloom;
pub mod encoding;
pub mod hll;
pub mod merge;
pub mod segment;
pub mod sidecar;
pub mod sketch;
pub mod temporal;

pub use merge::{count_live_keys, merge};
pub use segment::{EncodedSegment, SegmentReader, SegmentWriter, WriterOptions};
pub use sidecar::{
    bitemporal_winner, BitemporalHit, EncodedSidecar, RgBounds, SidecarDef, SidecarHeader,
    SidecarKind, SidecarReader, SidecarWriter, Triple,
};
pub use temporal::{VersionChain, VersionHit};
