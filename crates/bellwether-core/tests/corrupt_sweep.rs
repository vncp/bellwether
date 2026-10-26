//! Corrupt input never panics. Segments of every layout (exact, temporal,
//! bloom with canonical and `parquet_plain` filters and with a repeated row
//! group, range, and a typed temporal segment whose header records key and
//! timestamp types and whose keys include null-trailing tuples) are
//! corrupted and then opened and exercised through every read path, the
//! temporal fold, and merge alone and paired with a clean segment. Each must
//! return a value or an error. Two strategies run:
//!
//! - every truncation, and a byte-flip sweep at every position: fixed values
//!   plus `orig ^ 0x01`, `orig ^ 0x10`, and `orig + 1`, which turn roaring's
//!   no-run cookie into the run cookie;
//! - every byte value at every position. By default this covers the bodies
//!   of two small segments, a one-block exact segment and a typed temporal
//!   segment (the byte-flip sweep covers every header byte), so the whole
//!   binary stays under 10 s in a debug build; setting
//!   `BELLWETHER_FULL_SWEEP=1` runs it over every byte of every segment
//!   above.
//!
//! Kept in its own test binary because it silences the panic hook, on the
//! sweeping thread only, while corrupted input is exercised.
#![allow(clippy::unwrap_used, missing_docs, clippy::cast_possible_truncation)]

mod common;

use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Once;

use bellwether_core::bloom::{parquet_plain, PlainHash, Sbbf};
use bellwether_core::segment::PREAMBLE_LEN;
use bellwether_core::{merge, temporal, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{KeySpec, RowGroupId};
use common::*;

const TAG_CYCLE: [&str; 4] = ["a", "\0", "", "a\0"];

fn opts() -> WriterOptions {
    WriterOptions {
        block_keys: 2,
        range_block_entries: 2,
        ..WriterOptions::default()
    }
}

fn segments() -> Vec<(&'static str, Vec<u8>)> {
    let mut out = Vec::new();
    for (name, spec) in [
        ("exact", exact()),
        ("temporal", temporal()),
        ("bloom", bloom(0.1)),
        ("range", range()),
    ] {
        let mut w = SegmentWriter::new(spec, opts()).unwrap();
        for i in 0..10i64 {
            let k = if name == "range" {
                id_key(i)
            } else {
                key(i, TAG_CYCLE[usize::try_from(i % 4).unwrap()])
            };
            let rg = RowGroupId(u32::try_from(i % 4).unwrap());
            if name == "temporal" {
                let ts = if i % 3 == 0 { None } else { Some(i % 2) };
                w.insert_version(&k, ts, rg).unwrap();
            } else {
                w.insert(&k, rg).unwrap();
            }
        }
        out.push((name, w.finish().unwrap().bytes));
    }
    // reused Parquet filters of three different sizes (32, 64, 128 bytes)
    let mut w = SegmentWriter::new(
        KeySpec::Bloom {
            columns: vec!["id".into()],
            fpr: 0.1,
            bounds: None,
        },
        opts(),
    )
    .unwrap();
    for rg in 0..3u32 {
        let mut f = Sbbf::<PlainHash>::with_ndv_fpp(2 + u64::from(rg) * 60, 0.1).unwrap();
        f.insert_hash(parquet_plain::hash_i64(i64::from(rg)));
        w.insert_filter(RowGroupId(rg), 1, &f).unwrap();
    }
    out.push(("bloom_plain", w.finish().unwrap().bytes));
    // a merged bloom segment that repeats a row group (different filter sizes)
    let filters = |n: i64| {
        let mut w = SegmentWriter::new(bloom(0.1), opts()).unwrap();
        for i in 0..n {
            w.insert(&key(i, "m"), RowGroupId(2)).unwrap();
        }
        SegmentReader::open(w.finish().unwrap().bytes).unwrap()
    };
    out.push((
        "bloom_repeat_rg",
        merge(&[filters(2), filters(120)], &opts()).unwrap().bytes,
    ));
    out.push(("typed_temporal", typed_temporal(&TYPED_ROWS)));
    out
}

/// One row of a typed segment: `(id, tag, ts, row group)`.
type TypedRow = (Option<i64>, Option<&'static str>, Option<i64>, usize);

/// Rows for typed segments: null-trailing tuples, a null leading column (not
/// indexed), and a null timestamp.
const TYPED_ROWS: [TypedRow; 5] = [
    (Some(1), Some("a"), Some(3), 0),
    (Some(1), None, Some(4), 0),
    (Some(2), None, None, 1),
    (None, Some("b"), Some(1), 1),
    (Some(1), Some("\0"), Some(9), 2),
];

/// A temporal segment written from the dataset schema, so its header records
/// key and timestamp types, holding `rows`.
fn typed_temporal(rows: &[TypedRow]) -> Vec<u8> {
    let groups = rows.iter().map(|r| r.3).max().map_or(0, |m| m + 1);
    let mut layout: Layout = vec![Vec::new(); groups];
    for &(id, tag, ts, rg) in rows {
        layout[rg].push(Row {
            id,
            tag: tag.map(String::from),
            ts,
        });
    }
    build(&temporal(), &layout, &[], &opts()).remove(0).bytes
}

/// The small segments the default every-byte-value sweep covers: a one-block
/// exact segment and a typed temporal segment holding a null-trailing tuple.
fn small_segments() -> Vec<(&'static str, Vec<u8>)> {
    let mut w = SegmentWriter::new(exact(), WriterOptions::default()).unwrap();
    w.insert(&key(1, "a"), RowGroupId(1)).unwrap();
    w.insert(&key(2, "\0"), RowGroupId(2)).unwrap();
    vec![
        ("small_exact", w.finish().unwrap().bytes),
        ("small_typed_temporal", typed_temporal(&TYPED_ROWS[..2])),
    ]
}

/// Where a segment's JSON header ends and its body begins.
fn body_start(bytes: &[u8]) -> usize {
    let header_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    PREAMBLE_LEN + usize::try_from(header_len).unwrap()
}

thread_local! {
    static QUIET: Cell<bool> = const { Cell::new(false) };
}

/// Runs `f` with panics caught and their messages silenced on this thread
/// only, so a sweep can run beside another test and its own assertion
/// messages still print.
fn quietly(f: impl FnOnce()) -> bool {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !QUIET.with(Cell::get) {
                default(info);
            }
        }));
    });
    QUIET.with(|q| q.set(true));
    let panicked = catch_unwind(AssertUnwindSafe(f)).is_err();
    QUIET.with(|q| q.set(false));
    panicked
}

fn full_sweep() -> bool {
    std::env::var_os("BELLWETHER_FULL_SWEEP").is_some_and(|v| v == "1")
}

fn exercise(b: Vec<u8>, clean: &[u8]) {
    if let Ok(r) = SegmentReader::open(b) {
        let ks: Vec<Vec<u8>> = (0..10)
            .map(|i| key(i, "a"))
            .chain((0..10).map(id_key))
            .collect();
        let _ = r.lookup(&ks);
        let _ = r.lookup_hashes(&[parquet_plain::hash_i64(1)]);
        let _ = r.lookup_range(&id_key(0), &id_key(20));
        let _ = r.lookup_prefix(&id_key(3));
        let _ = r.lookup_prefix(&[]);
        let _ = r.lookup_prefix_range(&id_key(1), &id_key(4));
        let _ = r.latest(&ks);
        let _ = r.asof(&ks, 1);
        let _ = r.get(&ks);
        let _ = merge(std::slice::from_ref(&r), &WriterOptions::default());
        let pair = [r, SegmentReader::open(clean.to_vec()).unwrap()];
        let _ = temporal::latest(&pair, &ks);
        let _ = temporal::asof(&pair, &ks, 1);
        let _ = merge(&pair, &WriterOptions::default());
    }
}

/// A read-path exercise over corrupted bytes, given the clean segment.
type Exercise = fn(Vec<u8>, &[u8]);

/// The read paths that decode stored values (lookups, `get`, a prefix scan,
/// the temporal queries) and merge, which re-encodes every value. Lighter
/// than [`exercise`], for the default every-byte-value sweep.
fn exercise_values(b: Vec<u8>, _clean: &[u8]) {
    if let Ok(r) = SegmentReader::open(b) {
        let ks = [key(1, "a"), key(2, "\0")];
        let _ = r.lookup(&ks);
        let _ = r.get(&ks);
        let _ = r.lookup_prefix(&id_key(1));
        let _ = r.latest(&ks);
        let _ = r.asof(&ks, 3);
        let _ = merge(std::slice::from_ref(&r), &WriterOptions::default());
    }
}

#[test]
fn truncation_and_byte_flip_sweep_never_panics() {
    let mut panics = Vec::new();
    for (name, bytes) in segments() {
        // the uncorrupted segment works
        exercise(bytes.clone(), &bytes);
        for cut in 0..bytes.len() {
            let b = bytes[..cut].to_vec();
            if quietly(|| exercise(b, &bytes)) {
                panics.push(format!("{name} cut {cut}"));
            }
        }
        for pos in 0..bytes.len() {
            let orig = bytes[pos];
            for v in [
                0x00u8,
                0xFF,
                0x7F,
                0x80,
                0x01,
                orig ^ 0x01,
                orig ^ 0x10,
                orig.wrapping_add(1),
            ] {
                if v == orig {
                    continue;
                }
                let mut b = bytes.clone();
                b[pos] = v;
                if quietly(|| exercise(b, &bytes)) {
                    panics.push(format!("{name} pos {pos} = {v:#x}"));
                }
            }
        }
    }
    assert!(
        panics.is_empty(),
        "{} panics, first: {:?}",
        panics.len(),
        &panics[..panics.len().min(20)]
    );
}

/// Every byte value at every position. By default this covers the bodies of
/// two small segments through the value-decoding read paths (the byte-flip
/// sweep covers every header byte); with `BELLWETHER_FULL_SWEEP=1` it covers
/// every byte of every segment through every read path.
#[test]
fn every_byte_value_sweep_never_panics() {
    let full = full_sweep();
    let (segments, run): (_, Exercise) = if full {
        (segments(), exercise)
    } else {
        (small_segments(), exercise_values)
    };
    let mut panics = Vec::new();
    for (name, bytes) in segments {
        run(bytes.clone(), &bytes);
        let first = if full { 0 } else { body_start(&bytes) };
        for pos in first..bytes.len() {
            for v in 0..=u8::MAX {
                if v == bytes[pos] {
                    continue;
                }
                let mut b = bytes.clone();
                b[pos] = v;
                if quietly(|| run(b, &bytes)) {
                    panics.push(format!("{name} pos {pos} = {v:#x}"));
                }
            }
        }
    }
    assert!(
        panics.is_empty(),
        "{} panics, first: {:?}",
        panics.len(),
        &panics[..panics.len().min(20)]
    );
}
