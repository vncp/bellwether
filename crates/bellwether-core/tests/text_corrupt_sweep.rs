//! Corrupt text segments (B5) never panic: truncations and a byte-flip
//! sweep of a words and a trigram segment, in both the dictionary and the
//! bloom layout, each opened and exercised through every text read path
//! (`text::select` for each probe kind, `token_stats`, merge alone and
//! beside a clean segment). The gate flips every header byte and a
//! deterministic sample of the rest; the ignored `full_sweep_never_panics`
//! tries every truncation, every position, and every byte value at every
//! position of a small words segment. Each must return a value or a
//! typed error. A flip inside the header never opens as the same header: it
//! is refused, or it names another spec, types, or Unicode tables, which a
//! planner refuses (`SegmentReader::key_spec` against the declared spec;
//! committed segments are also read whole against their BLAKE3 id).
//!
//! Kept in its own test binary because it silences the panic hook, on the
//! sweeping thread only, while corrupted input is exercised.
#![allow(clippy::unwrap_used, missing_docs, clippy::cast_possible_truncation)]

use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Once};

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::segment::PREAMBLE_LEN;
use bellwether_core::text::{self, Probe};
use bellwether_core::{merge, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{KeySpec, RowGroupId, Tokenizer};

fn schema() -> Schema {
    Schema::new(vec![Field::new("body", DataType::Utf8, true)])
}

fn segment(tokenizer: Tokenizer, bloom: Option<f64>, rows: &[&[Option<&str>]]) -> Vec<u8> {
    let spec = KeySpec::Text {
        column: "body".into(),
        tokenizer,
        bloom,
    };
    let opts = WriterOptions {
        block_keys: 2,
        ..WriterOptions::default()
    };
    let mut w = SegmentWriter::for_dataset(spec, &schema(), opts).unwrap();
    for (rg, values) in rows.iter().enumerate() {
        let a: ArrayRef = Arc::new(values.iter().copied().collect::<StringArray>());
        let b = RecordBatch::try_new(Arc::new(schema()), vec![a]).unwrap();
        w.add_batch(RowGroupId(rg as u32), &b).unwrap();
    }
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(rows.len() as u32))
        .unwrap();
    w.finish().unwrap().bytes
}

const ROWS: [&[Option<&str>]; 3] = [
    &[Some("Hello, wörld"), None],
    &[Some("Straße\0x"), Some("ab")],
    &[None],
];

fn segments() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("words", segment(Tokenizer::Words, None, &ROWS)),
        ("trigram", segment(Tokenizer::Trigram, None, &ROWS)),
        ("words_bloom", segment(Tokenizer::Words, Some(0.1), &ROWS)),
        (
            "trigram_bloom",
            segment(Tokenizer::Trigram, Some(0.1), &ROWS),
        ),
    ]
}

fn probes() -> Vec<Probe> {
    vec![
        Probe::Key(text::word_key("hello")),
        Probe::Key(text::word_key("zzz")),
        Probe::Prefix(text::word_prefix_probe("st")),
        Probe::Prefix(Vec::new()),
        Probe::All(vec![text::trigram_key(b"str"), text::trigram_key(b"llo")]),
        Probe::Unpruned,
    ]
}

thread_local! {
    static QUIET: Cell<bool> = const { Cell::new(false) };
}

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

fn header_end(bytes: &[u8]) -> usize {
    let header_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    PREAMBLE_LEN + usize::try_from(header_len).unwrap()
}

fn exercise(b: Vec<u8>, clean: &[u8]) {
    if let Ok(r) = SegmentReader::open_for_dataset(b, &schema()) {
        for p in probes() {
            let _ = text::select_one(&r, &p);
            let _ = text::select(std::slice::from_ref(&r), &p);
        }
        let _ = text::token_stats(std::slice::from_ref(&r));
        let _ = merge(std::slice::from_ref(&r), &WriterOptions::default());
        let pair = [r, SegmentReader::open(clean.to_vec()).unwrap()];
        let _ = text::token_stats(&pair);
        let _ = text::select(&pair, &Probe::Key(text::word_key("hello")));
        let _ = merge(&pair, &WriterOptions::default());
    }
}

/// The flips tried at one position.
fn flips(orig: u8) -> [u8; 8] {
    [
        0x00u8,
        0xFF,
        0x7F,
        0x80,
        0x01,
        orig ^ 0x01,
        orig ^ 0x10,
        orig.wrapping_add(1),
    ]
}

/// Truncations at every `stride`-th length, and the flips at every header
/// byte (each must be refused or show as another header) and at every
/// `stride`-th body byte, of each segment: no panic anywhere.
fn sweep(stride: usize) {
    let mut panics = Vec::new();
    let mut silent = Vec::new();
    for (name, bytes) in segments() {
        exercise(bytes.clone(), &bytes);
        let clean = SegmentReader::open(bytes.clone()).unwrap();
        let end = header_end(&bytes);
        for cut in (0..bytes.len()).step_by(stride) {
            let b = bytes[..cut].to_vec();
            if quietly(|| exercise(b, &bytes)) {
                panics.push(format!("{name} cut {cut}"));
            }
        }
        let positions = (0..end).chain((end..bytes.len()).step_by(stride));
        for pos in positions {
            let orig = bytes[pos];
            for v in flips(orig) {
                if v == orig {
                    continue;
                }
                let mut b = bytes.clone();
                b[pos] = v;
                if (PREAMBLE_LEN..end).contains(&pos) {
                    if let Ok(r) = SegmentReader::open(b.clone()) {
                        if r.header() == clean.header() {
                            silent.push(format!("{name} pos {pos} = {v:#x}"));
                        }
                    }
                }
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
    assert!(
        silent.is_empty(),
        "header flips that open as the same header: {silent:?}"
    );
}

/// The gate's sweep: every header byte, and a deterministic sample of the
/// truncations and body bytes (every 3rd), so it stays well under
/// ten seconds; `full_sweep_never_panics` (ignored) covers every one.
#[test]
fn sampled_sweep_never_panics_and_header_flips_show() {
    sweep(3);
}

/// Every truncation and every position's flips, then every byte value at
/// every position of a small words segment. Run with `cargo test -p
/// bellwether-core --test text_corrupt_sweep -- --ignored` (seconds, not
/// minutes, in a debug build; too long for every gate run).
#[test]
#[ignore = "full every-byte sweep; run with --ignored"]
fn full_sweep_never_panics() {
    sweep(1);
    let bytes = segment(Tokenizer::Words, None, &[&[Some("a b")], &[Some("b")]]);
    exercise(bytes.clone(), &bytes);
    let mut panics = Vec::new();
    for pos in 0..bytes.len() {
        for v in 0..=u8::MAX {
            if v == bytes[pos] {
                continue;
            }
            let mut b = bytes.clone();
            b[pos] = v;
            if quietly(|| exercise(b, &bytes)) {
                panics.push(format!("pos {pos} = {v:#x}"));
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
