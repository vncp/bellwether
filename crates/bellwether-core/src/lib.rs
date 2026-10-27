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
//! - [`expr`]: the expression language of `expr` keys: parse, normalize,
//!   type-check, evaluate one batch at a time, and conformance vectors.
//! - [`hll`]: the HyperLogLog sketch the profiler and the `sketch` sidecar
//!   share.
//! - [`sketch`]: `sketch` sidecar segments (per item, per file HyperLogLog
//!   registers and MinHash signatures). Approximate; no planner reads them.
//! - [`text`]: text keys (B5): case folding, the words and trigram
//!   tokenizers, the residual's row test, and the planning rule.
//! - [`list`]: list keys (B4), an inverted index over a list column's
//!   elements, and the `contains_any` / `contains_all` lookups.
//! - [`join`]: join keys (B3), the keys two datasets share on one column
//!   pair: the pair build from both sides' exact segments, `semijoin`, and
//!   `overlap_stats`.
//! - [`agg`]: agg keys (B7), per-key, per-row-group aggregates of one value
//!   column, and the rule that decides which row groups they answer.
//! - [`tombstone`]: tombstones on temporal and bitemporal keys (B8): the
//!   `keyed_tombstone` liveness entries, the row test, and the pruning
//!   rule of `latest`/`asof` with `live`.
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

pub mod agg;
pub mod bloom;
pub mod encoding;
pub mod expr;
pub mod hll;
pub mod join;
pub mod list;
pub mod merge;
pub mod render;
pub mod segment;
pub mod sidecar;
pub mod sketch;
pub mod temporal;
pub mod text;
pub mod tombstone;

pub use merge::{count_live_keys, merge, MergedKeys};
pub use segment::{EncodedSegment, SegmentReader, SegmentWriter, WriterOptions};
pub use sidecar::{
    bitemporal_winner, BitemporalHit, EncodedSidecar, RgBounds, SidecarDef, SidecarHeader,
    SidecarKind, SidecarReader, SidecarWriter, Triple,
};
pub use temporal::{VersionChain, VersionHit};
