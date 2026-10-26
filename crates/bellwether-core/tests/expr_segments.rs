//! `expr` key segments: an ordinary exact or bloom segment over the virtual
//! column, whose header carries the expression and its conformance vectors.
//! Round trip, nulls (ADR 014), a property test against a brute-force oracle
//! over decoded rows, and corruption sweeps (every truncation, every byte
//! value at every position) that must never panic and never accept a
//! changed expression or vector.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::unnecessary_wraps,
    clippy::cast_possible_wrap,
    clippy::redundant_closure
)]

use std::cell::Cell;
use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Once};

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::encoding::{encode_value_as, Value};
use bellwether_core::expr::{self, json_get, parse_path, Over};
use bellwether_core::segment::{ColumnType, PREAMBLE_LEN};
use bellwether_core::{merge, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{Error, KeySpec, RowGroupId};
use proptest::prelude::*;
use roaring::RoaringBitmap;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("payload", DataType::Utf8, true),
    ])
}

fn sym_spec(over: Over) -> KeySpec {
    expr::spec("sym", "json_get(payload,'$.symbol')", over).unwrap()
}

fn batch(payloads: &[Option<String>]) -> RecordBatch {
    // Only the input column, as a build reads it: the virtual column is
    // derived inside the writer.
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(payloads.to_vec()))],
    )
    .unwrap()
}

fn key(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_as(&Value::Utf8(s), &DataType::Utf8, &mut out).unwrap();
    out
}

fn doc(sym: &str) -> Option<String> {
    Some(format!(
        r#"{{"symbol":{},"px":1}}"#,
        serde_json::to_string(sym).unwrap()
    ))
}

/// Writes one segment of `spec` over row groups `0..groups.len()`.
fn build(spec: &KeySpec, groups: &[Vec<Option<String>>], opts: WriterOptions) -> Vec<u8> {
    let mut w = SegmentWriter::for_dataset(spec.clone(), &schema(), opts).unwrap();
    for (rg, rows) in groups.iter().enumerate() {
        w.add_batch(RowGroupId(u32::try_from(rg).unwrap()), &batch(rows))
            .unwrap();
    }
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(u32::try_from(groups.len()).unwrap()))
        .unwrap();
    w.finish().unwrap().bytes
}

fn small() -> Vec<Vec<Option<String>>> {
    vec![
        vec![doc("SYM1"), doc("SYM2"), None],
        vec![
            doc("SYM1"),
            Some("not json".into()),
            Some(r#"{"symbol":null}"#.into()),
        ],
        vec![Some(r#"{"px":1}"#.into())],
        vec![doc("a\0b")],
    ]
}

#[test]
fn round_trip_exact_and_bloom() {
    for over in [Over::Exact, Over::Bloom(0.01)] {
        let spec = sym_spec(over);
        let bytes = build(&spec, &small(), WriterOptions::default());
        let r = SegmentReader::open_for_dataset(bytes.clone(), &schema()).unwrap();
        assert_eq!(r.key_spec(), &spec);
        assert_eq!(r.header().key_types, Some(vec![ColumnType::Utf8]));
        assert_eq!(r.rowgroup_range(), Some((RowGroupId(0), RowGroupId(3))));
        let sym1 = r.lookup(&[key("SYM1")]).unwrap();
        let sym2 = r.lookup(&[key("SYM2")]).unwrap();
        let nul = r.lookup(&[key("a\0b")]).unwrap();
        match over {
            Over::Exact => {
                assert_eq!(sym1, RoaringBitmap::from_iter([0, 1]));
                assert_eq!(sym2, RoaringBitmap::from_iter([0]));
                assert_eq!(nul, RoaringBitmap::from_iter([3]));
                // two distinct values plus "a\0b"; no null, no invalid JSON
                assert_eq!(r.entries(), 3);
                assert!(r.lookup(&[key("nope")]).unwrap().is_empty());
            }
            Over::Bloom(_) => {
                assert!(sym1.contains(0) && sym1.contains(1));
                assert!(sym2.contains(0));
                assert!(nul.contains(3));
            }
        }
        // merging two segments of the key keeps the spec
        let other = build(&spec, &small()[..1], WriterOptions::default());
        let m = merge(
            &[
                SegmentReader::open(bytes).unwrap(),
                SegmentReader::open(other).unwrap(),
            ],
            &WriterOptions::default(),
        )
        .unwrap();
        assert_eq!(m.key_spec, spec);
    }
}

#[test]
fn nulls_are_not_indexed_and_an_all_null_row_group_is_still_covered() {
    let spec = sym_spec(Over::Exact);
    // Every row's value is null: a null payload, a JSON null, a missing
    // path, an object, invalid JSON.
    let groups = vec![vec![
        None,
        Some(r#"{"symbol":null}"#.into()),
        Some("{}".into()),
        Some(r#"{"symbol":{"a":1}}"#.into()),
        Some("{".into()),
    ]];
    let bytes = build(&spec, &groups, WriterOptions::default());
    let r = SegmentReader::open_for_dataset(bytes, &schema()).unwrap();
    assert_eq!(r.entries(), 0);
    assert_eq!(r.rowgroup_range(), Some((RowGroupId(0), RowGroupId(0))));
    // a probe of the null marker matches nothing
    assert!(r.lookup(&[vec![0u8]]).unwrap().is_empty());
}

#[test]
fn open_for_dataset_refuses_a_schema_whose_input_type_changed_or_holds_the_name() {
    let spec = sym_spec(Over::Exact);
    let bytes = build(&spec, &small(), WriterOptions::default());
    let ints = Schema::new(vec![Field::new("payload", DataType::Int64, true)]);
    let e = SegmentReader::open_for_dataset(bytes.clone(), &ints)
        .unwrap_err()
        .to_string();
    assert!(e.contains("json_get takes a string"), "{e}");
    let named = Schema::new(vec![
        Field::new("payload", DataType::Utf8, true),
        Field::new("sym", DataType::Utf8, true),
    ]);
    let e = SegmentReader::open_for_dataset(bytes, &named)
        .unwrap_err()
        .to_string();
    assert!(e.contains("already a dataset column"), "{e}");
}

fn header_patch(bytes: &[u8], from: &str, to: &str) -> Vec<u8> {
    let len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let header = std::str::from_utf8(&bytes[PREAMBLE_LEN..PREAMBLE_LEN + len]).unwrap();
    assert!(header.contains(from), "{header}");
    let patched = header.replacen(from, to, 1);
    let mut out = bytes[..16].to_vec();
    out.extend_from_slice(&u32::try_from(patched.len()).unwrap().to_le_bytes());
    out.extend_from_slice(patched.as_bytes());
    let delta = patched.len() as i64 - len as i64;
    let body = &bytes[PREAMBLE_LEN + len..];
    // shift the footer's index offset by the header's growth
    let mut body = body.to_vec();
    let n = body.len();
    let off = u64::from_le_bytes(body[n - 24..n - 16].try_into().unwrap());
    let off = u64::try_from(off as i64 + delta).unwrap();
    body[n - 24..n - 16].copy_from_slice(&off.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

#[test]
fn a_header_with_a_changed_vector_or_expression_is_refused_typed() {
    let spec = sym_spec(Over::Exact);
    let bytes = build(&spec, &small(), WriterOptions::default());
    // the untouched patch round-trips
    let same = header_patch(&bytes, "\"sym\"", "\"sym\"");
    SegmentReader::open(same).unwrap();
    // a vector whose recorded value this binary does not reproduce
    let v = header_patch(&bytes, "0653594d310000", "0653594d320000");
    match SegmentReader::open(v) {
        Err(Error::KeySpec(m)) => assert!(m.contains("this binary computes"), "{m}"),
        other => panic!("{other:?}"),
    }
    // an expression stored in another spelling
    let v = header_patch(&bytes, "json_get(payload,", "JSON_GET(payload,");
    match SegmentReader::open(v) {
        Err(Error::KeySpec(m)) => assert!(m.contains("normalized"), "{m}"),
        other => panic!("{other:?}"),
    }
    // an expression that does not parse
    let v = header_patch(&bytes, "json_get(payload,", "json_got(payload,");
    match SegmentReader::open(v) {
        Err(Error::KeySpec(m)) => assert!(m.contains("unknown function"), "{m}"),
        other => panic!("{other:?}"),
    }
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

/// Opens corrupted bytes and exercises every read path; returns whether the
/// bytes opened.
fn exercise(b: Vec<u8>) -> bool {
    let probes = [key("SYM1"), key("SYM2"), key("a\0b")];
    match SegmentReader::open(b.clone()) {
        Ok(r) => {
            let _ = r.lookup(&probes);
            let _ = r.get(&probes);
            let _ = r.lookup_prefix(&key("A")[..2]);
            let _ = merge(std::slice::from_ref(&r), &WriterOptions::default());
            let _ = SegmentReader::open_for_dataset(b, &schema());
            true
        }
        Err(_) => false,
    }
}

/// Byte positions of every conformance vector's recorded value in the
/// header (the hex between `"value":"` and `"`).
fn vector_value_positions(bytes: &[u8]) -> BTreeSet<usize> {
    let len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let header = &bytes[PREAMBLE_LEN..PREAMBLE_LEN + len];
    let pat = b"\"value\":\"";
    let mut out = BTreeSet::new();
    let mut i = 0;
    while i + pat.len() <= header.len() {
        if &header[i..i + pat.len()] == pat {
            let mut j = i + pat.len();
            while header[j] != b'"' {
                out.insert(PREAMBLE_LEN + j);
                j += 1;
            }
            i = j;
        }
        i += 1;
    }
    out
}

fn full_sweep() -> bool {
    std::env::var_os("BELLWETHER_FULL_SWEEP").is_some_and(|v| v == "1")
}

/// Every truncation; every byte value at every body position of the exact
/// segment (every position of both with `BELLWETHER_FULL_SWEEP=1`); eight
/// values at every other position: never a panic. A change to any byte of a
/// conformance vector's recorded value never opens.
#[test]
fn every_truncation_and_every_byte_value_never_panics() {
    let opts = WriterOptions {
        block_keys: 2,
        ..WriterOptions::default()
    };
    let mut panics = Vec::new();
    let mut opened_with_bad_vector = Vec::new();
    for over in [Over::Exact, Over::Bloom(0.1)] {
        let spec = sym_spec(over);
        let bytes = build(&spec, &small(), opts.clone());
        let vectors = vector_value_positions(&bytes);
        assert!(!vectors.is_empty());
        assert!(exercise(bytes.clone()));
        let len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let body = PREAMBLE_LEN + len;
        for cut in 0..bytes.len() {
            if quietly(|| {
                exercise(bytes[..cut].to_vec());
            }) {
                panics.push(format!("{over:?} cut {cut}"));
            }
        }
        for pos in 0..bytes.len() {
            let orig = bytes[pos];
            let values: Vec<u8> = if (pos >= body && over == Over::Exact) || full_sweep() {
                (0..=u8::MAX).collect()
            } else {
                vec![
                    0x00,
                    0xFF,
                    0x7F,
                    0x80,
                    b'0',
                    orig ^ 0x01,
                    orig ^ 0x20,
                    orig.wrapping_add(1),
                ]
            };
            for v in values {
                if v == orig {
                    continue;
                }
                let mut b = bytes.clone();
                b[pos] = v;
                let mut opened = false;
                if quietly(|| opened = exercise(b)) {
                    panics.push(format!("{over:?} pos {pos} = {v:#x}"));
                }
                if opened && vectors.contains(&pos) {
                    opened_with_bad_vector.push(format!("{over:?} pos {pos} = {v:#x}"));
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
        opened_with_bad_vector.is_empty(),
        "{:?}",
        &opened_with_bad_vector[..opened_with_bad_vector.len().min(20)]
    );
}

// --- oracle ------------------------------------------------------------------

fn sym_value() -> impl Strategy<Value = Option<String>> {
    prop_oneof![
        3 => prop::sample::select(vec!["SYM1", "SYM2", "a\u{0}b", "", "é", "SYM3"])
            .prop_map(|s| doc(s)),
        1 => Just(None),
        1 => Just(Some(r#"{"symbol":7}"#.to_string())),
        1 => Just(Some(r#"{"symbol":null}"#.to_string())),
        1 => Just(Some(r#"{"symbol":"SYM1""#.to_string())),
        1 => Just(Some(r#"{"x":{"symbol":"SYM1"}}"#.to_string())),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Every probe's row groups equal (exact) or contain (bloom) the row
    /// groups of the rows whose decoded payload yields it, computed here
    /// row by row with json_get over the raw text.
    #[test]
    fn lookups_equal_the_brute_force_oracle(
        groups in prop::collection::vec(prop::collection::vec(sym_value(), 0..6), 1..7),
        bloom in any::<bool>(),
    ) {
        let over = if bloom { Over::Bloom(0.05) } else { Over::Exact };
        let spec = sym_spec(over);
        let bytes = build(&spec, &groups, WriterOptions { block_keys: 2, ..WriterOptions::default() });
        let r = SegmentReader::open_for_dataset(bytes, &schema()).unwrap();
        let path = parse_path("$.symbol").unwrap();
        for probe in ["SYM1", "SYM2", "a\u{0}b", "", "é", "SYM3", "7", "nope"] {
            let truth: RoaringBitmap = groups
                .iter()
                .enumerate()
                .filter(|(_, rows)| rows.iter().flatten().any(|p| json_get(p, &path).as_deref() == Some(probe)))
                .map(|(i, _)| u32::try_from(i).unwrap())
                .collect();
            let got = r.lookup(&[key(probe)]).unwrap();
            if bloom {
                prop_assert!(truth.is_subset(&got), "{probe}: {truth:?} ⊄ {got:?}");
            } else {
                prop_assert_eq!(got, truth, "{}", probe);
            }
        }
    }
}
