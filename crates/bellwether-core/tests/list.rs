//! List keys (B4): round trip, pinned conformance vectors, nulls and empty
//! lists (ADR 014), dataset-type conversion, refusals, an every-byte
//! corruption sweep, and a property test of `contains_any` and
//! `contains_all` against a brute-force oracle over decoded rows.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::too_many_lines,
    clippy::ref_option
)]

use std::cell::Cell;
use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Once};

use arrow::array::{
    ArrayRef, Float64Builder, Int32Builder, Int64Builder, LargeListBuilder, ListBuilder,
    MapBuilder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::encoding::{encode_tuple, encode_tuple_as, Value};
use bellwether_core::list::{
    check_list_column, contains_all, contains_any, distinct_elements, element_type, ListElements,
};
use bellwether_core::segment::{spec_types, Layout};
use bellwether_core::{merge, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{Error, Hash, KeySpec, RowGroupId};
use proptest::prelude::*;
use roaring::RoaringBitmap;

fn spec() -> KeySpec {
    KeySpec::List {
        column: "tags".into(),
    }
}

fn utf8(s: &str) -> Vec<u8> {
    encode_tuple(&[Value::Utf8(s)]).unwrap()
}

fn int(i: i64) -> Vec<u8> {
    encode_tuple_as(&[Value::Int(i)], &[DataType::Int64]).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A `List<Utf8>` column named `tags`; `None` is a null list.
fn utf8_lists(rows: &[Option<Vec<Option<&str>>>]) -> RecordBatch {
    let mut b = ListBuilder::new(StringBuilder::new());
    for r in rows {
        match r {
            None => b.append(false),
            Some(items) => {
                for i in items {
                    b.values().append_option(*i);
                }
                b.append(true);
            }
        }
    }
    let arr: ArrayRef = Arc::new(b.finish());
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "tags",
            arr.data_type().clone(),
            true,
        )])),
        vec![arr],
    )
    .unwrap()
}

fn utf8_schema() -> Schema {
    Schema::new(vec![Field::new(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    )])
}

fn opts(block_keys: usize) -> WriterOptions {
    WriterOptions {
        block_keys,
        ..WriterOptions::default()
    }
}

fn open(bytes: &[u8]) -> SegmentReader<Vec<u8>> {
    SegmentReader::open(bytes.to_vec()).unwrap()
}

/// Rows per row group, in the order used by the round trip and the pinned
/// vectors: rg 0 `[AAPL, MSFT, AAPL]`, `[]`; rg 1 null, `[MSFT, null]`;
/// rg 2 `[IBM]`; rg 3 `[]` only (covered, no key).
fn pinned_segment() -> Vec<u8> {
    let rgs = [
        utf8_lists(&[
            Some(vec![Some("AAPL"), Some("MSFT"), Some("AAPL")]),
            Some(vec![]),
        ]),
        utf8_lists(&[None, Some(vec![Some("MSFT"), None])]),
        utf8_lists(&[Some(vec![Some("IBM")])]),
        utf8_lists(&[Some(vec![])]),
    ];
    let mut w =
        SegmentWriter::for_dataset(spec(), &utf8_schema(), WriterOptions::default()).unwrap();
    for (i, b) in rgs.iter().enumerate() {
        w.add_batch(RowGroupId(i as u32), b).unwrap();
    }
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(4)).unwrap();
    w.finish().unwrap().bytes
}

fn rgs(ids: &[u32]) -> RoaringBitmap {
    ids.iter().copied().collect()
}

#[test]
fn list_segments_round_trip_with_the_exact_layout_keyed_by_element() {
    let bytes = pinned_segment();
    let r = SegmentReader::open_for_dataset(bytes.clone(), &utf8_schema()).unwrap();
    assert_eq!(r.key_spec(), &spec());
    assert_eq!(r.header().layout, Layout::Keyed);
    // The one recorded key type is the element type.
    assert_eq!(
        serde_json::to_value(r.header().key_types.as_ref().unwrap()).unwrap(),
        serde_json::json!([{"type": "utf8"}])
    );
    assert_eq!(
        r.entries(),
        3,
        "AAPL, IBM, MSFT; duplicates and nulls add none"
    );
    assert_eq!(r.rowgroup_range(), Some((RowGroupId(0), RowGroupId(3))));
    let readers = [r];
    assert_eq!(contains_any(&readers, &[utf8("AAPL")]).unwrap(), rgs(&[0]));
    assert_eq!(
        contains_any(&readers, &[utf8("MSFT"), utf8("IBM")]).unwrap(),
        rgs(&[0, 1, 2])
    );
    assert_eq!(
        contains_any(&readers, &[utf8("GOOG")]).unwrap(),
        RoaringBitmap::new()
    );
    assert_eq!(
        contains_any::<_, Vec<u8>>(&readers, &[]).unwrap(),
        RoaringBitmap::new()
    );
    assert_eq!(
        contains_all(&readers, &[vec![utf8("AAPL")], vec![utf8("MSFT")]]).unwrap(),
        rgs(&[0])
    );
    assert_eq!(
        contains_all(&readers, &[vec![utf8("MSFT")], vec![utf8("IBM")]]).unwrap(),
        RoaringBitmap::new(),
        "each in some row group, never together"
    );
    // Deterministic: the same rows give the same bytes.
    assert_eq!(pinned_segment(), bytes);
    // Merge keeps every answer.
    let a = {
        let mut w =
            SegmentWriter::for_dataset(spec(), &utf8_schema(), WriterOptions::default()).unwrap();
        w.add_batch(
            RowGroupId(0),
            &utf8_lists(&[Some(vec![Some("x"), Some("y")])]),
        )
        .unwrap();
        w.set_rowgroup_range(RowGroupId(0)..RowGroupId(1)).unwrap();
        w.finish().unwrap().bytes
    };
    let b = {
        let mut w =
            SegmentWriter::for_dataset(spec(), &utf8_schema(), WriterOptions::default()).unwrap();
        w.add_batch(RowGroupId(1), &utf8_lists(&[Some(vec![Some("y")])]))
            .unwrap();
        w.set_rowgroup_range(RowGroupId(1)..RowGroupId(2)).unwrap();
        w.finish().unwrap().bytes
    };
    let m = merge(&[open(&a), open(&b)], &WriterOptions::default()).unwrap();
    let merged = [open(&m.bytes)];
    assert_eq!(contains_any(&merged, &[utf8("y")]).unwrap(), rgs(&[0, 1]));
    assert_eq!(merged[0].key_spec(), &spec());
}

/// The vectors `docs/format.md` ("List keys") pins: the element keys are
/// the canonical encodings of the elements, each value a no-run roaring
/// bitmap, and the whole segment's blake3.
#[test]
fn list_conformance_vectors() {
    let bytes = pinned_segment();
    let r = open(&bytes);
    let got = r.get(&[utf8("AAPL"), utf8("MSFT"), utf8("IBM")]).unwrap();
    assert_eq!(hex(&utf8("AAPL")), "06 41 41 50 4c 00 00");
    let mut aapl = Vec::new();
    got[&utf8("AAPL")].bitmap.serialize_into(&mut aapl).unwrap();
    assert_eq!(
        hex(&aapl),
        "3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 00 00"
    );
    let mut msft = Vec::new();
    got[&utf8("MSFT")].bitmap.serialize_into(&mut msft).unwrap();
    assert_eq!(
        hex(&msft),
        "3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00"
    );
    assert_eq!(
        Hash::of(&bytes).to_hex(),
        PINNED_LIST_BLAKE3,
        "list:tags segment bytes changed"
    );
    let doc = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/format.md"))
        .unwrap();
    assert!(
        doc.contains(PINNED_LIST_BLAKE3),
        "docs/format.md must pin {PINNED_LIST_BLAKE3}"
    );
}

const PINNED_LIST_BLAKE3: &str = "972f0deeeb42ea46c6402160dae27f6ac7dfd87102ad9480e83798db78ad232d";

#[test]
fn null_lists_empty_lists_and_null_elements_are_not_indexed() {
    let mut w =
        SegmentWriter::for_dataset(spec(), &utf8_schema(), WriterOptions::default()).unwrap();
    w.add_batch(
        RowGroupId(0),
        &utf8_lists(&[None, Some(vec![]), Some(vec![None, None])]),
    )
    .unwrap();
    w.add_batch(RowGroupId(1), &utf8_lists(&[Some(vec![Some("")])]))
        .unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(2)).unwrap();
    let seg = w.finish().unwrap();
    let r = open(&seg.bytes);
    // Row group 0 is covered (its range is recorded) and holds no key.
    assert_eq!(r.rowgroup_range(), Some((RowGroupId(0), RowGroupId(1))));
    assert_eq!(r.entries(), 1, "only the empty string, a real value");
    let readers = [r];
    assert_eq!(contains_any(&readers, &[utf8("")]).unwrap(), rgs(&[1]));
    // A null probe never matches: its encoding is the null marker, which no
    // element key holds.
    assert_eq!(
        contains_any(&readers, &[vec![0u8]]).unwrap(),
        RoaringBitmap::new()
    );
    // "Contains all of nothing" holds for null and empty lists too, which
    // the index does not record: refused, never a wrong empty answer.
    match contains_all::<_, Vec<u8>>(&readers, &[]) {
        Err(Error::KeySpec(m)) => assert!(m.contains("at least one value"), "{m}"),
        other => panic!("{other:?}"),
    }
    // Two writers over different key-less row groups never share bytes.
    let keyless = |rg: u32| {
        let mut w =
            SegmentWriter::for_dataset(spec(), &utf8_schema(), WriterOptions::default()).unwrap();
        w.add_batch(RowGroupId(rg), &utf8_lists(&[None, Some(vec![])]))
            .unwrap();
        w.set_rowgroup_range(RowGroupId(rg)..RowGroupId(rg + 1))
            .unwrap();
        w.finish().unwrap()
    };
    assert_ne!(keyless(0).id, keyless(1).id);
}

#[test]
fn float_zero_elements_keep_both_encodings_and_nan_is_one_key() {
    let schema = Schema::new(vec![Field::new(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        true,
    )]);
    let mut b = ListBuilder::new(Float64Builder::new());
    b.values().append_value(-0.0);
    b.append(true);
    b.values().append_value(0.0);
    b.values().append_value(f64::NAN);
    b.append(true);
    let arr: ArrayRef = Arc::new(b.finish());
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![arr.clone()]).unwrap();
    let mut w = SegmentWriter::for_dataset(spec(), &schema, WriterOptions::default()).unwrap();
    w.add_batch(RowGroupId(0), &batch).unwrap();
    let readers = [open(&w.finish().unwrap().bytes)];
    let f = |x: f64| encode_tuple_as(&[Value::Float(x)], &[DataType::Float64]).unwrap();
    assert_eq!(readers[0].entries(), 3);
    assert_eq!(contains_any(&readers, &[f(-0.0)]).unwrap(), rgs(&[0]));
    assert_eq!(contains_any(&readers, &[f(0.0)]).unwrap(), rgs(&[0]));
    assert_eq!(contains_any(&readers, &[f(f64::NAN)]).unwrap(), rgs(&[0]));
    // Distinct elements by SQL equality: +0 and -0 are one value, NaN one,
    // whether the zeros sit in one segment or in two.
    assert_eq!(distinct_elements(&readers).unwrap(), 2);
    let one = |rg: u32, v: &[f64]| {
        let mut b = ListBuilder::new(Float64Builder::new());
        for x in v {
            b.values().append_value(*x);
        }
        b.append(true);
        let arr: ArrayRef = Arc::new(b.finish());
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![arr]).unwrap();
        let mut w = SegmentWriter::for_dataset(spec(), &schema, WriterOptions::default()).unwrap();
        w.add_batch(RowGroupId(rg), &batch).unwrap();
        open(&w.finish().unwrap().bytes)
    };
    let split = [one(0, &[0.0, 2.5]), one(1, &[-0.0, 2.5, 7.0])];
    assert_eq!(distinct_elements(&split).unwrap(), 3);
}

#[test]
fn file_types_convert_to_the_dataset_element_type_or_are_refused() {
    // A file List<Int32> and a file LargeList<Int64> under a dataset
    // List<Int64>: both encode at Int64.
    let dataset = Schema::new(vec![Field::new(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )]);
    let mut narrow = ListBuilder::new(Int32Builder::new());
    narrow.values().append_value(7);
    narrow.append(true);
    let narrow: ArrayRef = Arc::new(narrow.finish());
    let mut large = LargeListBuilder::new(Int64Builder::new());
    large.values().append_value(7);
    large.values().append_value(i64::MAX);
    large.append(true);
    let large: ArrayRef = Arc::new(large.finish());
    let mut w = SegmentWriter::for_dataset(spec(), &dataset, WriterOptions::default()).unwrap();
    for (rg, arr) in [narrow, large].into_iter().enumerate() {
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "tags",
                arr.data_type().clone(),
                true,
            )])),
            vec![arr],
        )
        .unwrap();
        w.add_batch(RowGroupId(rg as u32), &b).unwrap();
    }
    let readers = [open(&w.finish().unwrap().bytes)];
    assert_eq!(contains_any(&readers, &[int(7)]).unwrap(), rgs(&[0, 1]));
    assert_eq!(contains_any(&readers, &[int(i64::MAX)]).unwrap(), rgs(&[1]));

    // A value that does not fit the dataset element type fails the whole
    // batch, and the writer keeps nothing from it.
    let narrow_ds = Schema::new(vec![Field::new(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]);
    let mut w = SegmentWriter::for_dataset(spec(), &narrow_ds, WriterOptions::default()).unwrap();
    let mut big = ListBuilder::new(Int64Builder::new());
    big.values().append_value(1);
    big.values().append_value(i64::MAX);
    big.append(true);
    let big: ArrayRef = Arc::new(big.finish());
    let b = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "tags",
            big.data_type().clone(),
            true,
        )])),
        vec![big],
    )
    .unwrap();
    assert!(w.add_batch(RowGroupId(0), &b).is_err());
    let seg = w.finish().unwrap();
    assert_eq!(open(&seg.bytes).entries(), 0, "all or nothing");

    // A map is indexed by its keys.
    let mut m = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    m.keys().append_value("k1");
    m.values().append_value(1);
    m.keys().append_value("k2");
    m.values().append_null();
    m.append(true).unwrap();
    m.append(false).unwrap();
    let marr: ArrayRef = Arc::new(m.finish());
    let mschema = Schema::new(vec![Field::new("tags", marr.data_type().clone(), true)]);
    assert_eq!(element_type(marr.data_type()), Some(&DataType::Utf8));
    let mut w = SegmentWriter::for_dataset(spec(), &mschema, WriterOptions::default()).unwrap();
    w.add_batch(
        RowGroupId(0),
        &RecordBatch::try_new(Arc::new(mschema.clone()), vec![marr.clone()]).unwrap(),
    )
    .unwrap();
    let readers = [open(&w.finish().unwrap().bytes)];
    assert_eq!(contains_any(&readers, &[utf8("k2")]).unwrap(), rgs(&[0]));
    assert_eq!(ListElements::new(&marr).unwrap().row(1), None);
}

#[test]
fn columns_that_are_not_lists_of_flat_values_are_refused_with_the_reason() {
    let flat = Schema::new(vec![Field::new("tags", DataType::Utf8, true)]);
    let e = SegmentWriter::for_dataset(spec(), &flat, WriterOptions::default())
        .err()
        .unwrap();
    assert!(e.to_string().contains("exact:tags instead"), "{e}");
    let nested = DataType::List(Arc::new(Field::new(
        "item",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )));
    let e = check_list_column("tags", &nested).unwrap_err();
    assert!(e.to_string().contains("nested list"), "{e}");
    let fixed = DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), 2);
    assert!(check_list_column("tags", &fixed).is_err());
    let missing = Schema::new(vec![Field::new("other", DataType::Utf8, true)]);
    assert!(spec_types(&spec(), &missing).is_err());
    // A segment of another kind is refused by the membership lookups.
    let mut w = SegmentWriter::new(
        KeySpec::Exact {
            columns: vec!["tags".into()],
            counts: false,
            bounds: None,
        },
        WriterOptions::default(),
    )
    .unwrap();
    w.insert(&utf8("a"), RowGroupId(0)).unwrap();
    let exact = [open(&w.finish().unwrap().bytes)];
    assert!(contains_any(&exact, &[utf8("a")]).is_err());
    // A list segment opened against a dataset whose element type changed is
    // refused, never read under the wrong encoding.
    let other = Schema::new(vec![Field::new(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )]);
    assert!(SegmentReader::open_for_dataset(pinned_segment(), &other).is_err());
}

// -- corruption sweep -------------------------------------------------------

thread_local! {
    static QUIET: Cell<bool> = const { Cell::new(false) };
}
static HOOK: Once = Once::new();

fn install_hook() {
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !QUIET.with(Cell::get) {
                previous(info);
            }
        }));
    });
}

/// Opens `bytes` and runs every list read path: both lookups, `get`, and
/// merge alone and with a clean segment. Returns whether it panicked.
fn exercise(bytes: &[u8], clean: &[u8]) -> bool {
    QUIET.with(|q| q.set(true));
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        if let Ok(r) = SegmentReader::open(bytes.to_vec()) {
            let probes = [utf8("AAPL"), utf8("MSFT"), utf8("IBM"), utf8("ZZZ")];
            let _ = r.get(&probes);
            let readers = [r];
            let _ = contains_any(&readers, &probes);
            let _ = contains_all(&readers, &[vec![utf8("AAPL")], vec![utf8("MSFT")]]);
            let _ = merge(&readers, &WriterOptions::default());
            let [r] = readers;
            let _ = merge(&[r, open(clean)], &WriterOptions::default());
        }
    }))
    .is_err();
    QUIET.with(|q| q.set(false));
    panicked
}

/// Every truncation, and every byte value at every position, of a list
/// segment with several blocks: opening and every read path return a value
/// or an error, never a panic.
#[test]
fn every_byte_of_a_list_segment_corrupted_never_panics() {
    install_hook();
    let mut w = SegmentWriter::for_dataset(spec(), &utf8_schema(), opts(2)).unwrap();
    for rg in 0..4u32 {
        let items: Vec<Option<&str>> = ["AAPL", "MSFT", "IBM", "a\0b", ""]
            .iter()
            .skip(rg as usize)
            .map(|s| Some(*s))
            .collect();
        w.add_batch(RowGroupId(rg), &utf8_lists(&[Some(items), None]))
            .unwrap();
    }
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(4)).unwrap();
    let clean = w.finish().unwrap().bytes;
    for n in 0..clean.len() {
        assert!(!exercise(&clean[..n], &clean), "truncated to {n}");
    }
    let full = std::env::var("BELLWETHER_FULL_SWEEP").is_ok();
    let values: Vec<u8> = if full {
        (0..=255).collect()
    } else {
        vec![0x00, 0xff, 0x01, 0x80, 0x7f]
    };
    for i in 0..clean.len() {
        let mut variants: BTreeSet<u8> = values.iter().copied().collect();
        variants.extend([clean[i] ^ 0x01, clean[i] ^ 0x10, clean[i].wrapping_add(1)]);
        for v in variants {
            if v == clean[i] {
                continue;
            }
            let mut bad = clean.clone();
            bad[i] = v;
            assert!(!exercise(&bad, &clean), "byte {i} set to {v:#04x}");
        }
    }
}

// -- property test against a brute-force oracle ----------------------------

type Rows = Vec<Option<Vec<Option<u8>>>>;

/// Small element alphabet so lists repeat elements and row groups share
/// them; `None` is a null element, an outer `None` a null list.
fn rows() -> impl Strategy<Value = Rows> {
    prop::collection::vec(
        prop::option::weighted(
            0.85,
            prop::collection::vec(prop::option::weighted(0.9, 0u8..12), 0..6),
        ),
        0..5,
    )
}

fn name(e: u8) -> String {
    format!("e{e}")
}

fn batch_of(rows: &Rows) -> RecordBatch {
    let owned: Vec<Option<Vec<Option<String>>>> = rows
        .iter()
        .map(|r| r.as_ref().map(|v| v.iter().map(|e| e.map(name)).collect()))
        .collect();
    let borrowed: Vec<Option<Vec<Option<&str>>>> = owned
        .iter()
        .map(|r| r.as_ref().map(|v| v.iter().map(|e| e.as_deref()).collect()))
        .collect();
    utf8_lists(&borrowed)
}

fn holds(row: &Option<Vec<Option<u8>>>, e: u8) -> bool {
    row.as_ref().is_some_and(|v| v.contains(&Some(e)))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// `contains_any` equals the row groups holding a row whose list holds
    /// a queried element; `contains_all` equals the row groups holding every
    /// queried element (each in some row), which contains every row group
    /// with a row holding all of them. Over several segments with random
    /// splits and block sizes, with duplicates, nulls, and empty lists.
    #[test]
    fn lookups_equal_the_oracle(
        layout in prop::collection::vec(rows(), 1..8),
        split in 1usize..4,
        block_keys in 1usize..5,
        any in prop::collection::vec(0u8..14, 0..5),
        all in prop::collection::vec(0u8..14, 1..4),
    ) {
        let mut segs = Vec::new();
        for (chunk_no, chunk) in layout.chunks(split).enumerate() {
            let mut w = SegmentWriter::for_dataset(spec(), &utf8_schema(), opts(block_keys)).unwrap();
            let start = (chunk_no * split) as u32;
            for (i, rows) in chunk.iter().enumerate() {
                w.add_batch(RowGroupId(start + i as u32), &batch_of(rows)).unwrap();
            }
            w.set_rowgroup_range(RowGroupId(start)..RowGroupId(start + chunk.len() as u32)).unwrap();
            segs.push(w.finish().unwrap().bytes);
        }
        let readers: Vec<_> = segs.iter().map(|b| open(b)).collect();
        let any_probes: Vec<Vec<u8>> = any.iter().map(|e| utf8(&name(*e))).collect();
        let want_any: RoaringBitmap = layout.iter().enumerate()
            .filter(|(_, rows)| rows.iter().any(|r| any.iter().any(|e| holds(r, *e))))
            .map(|(rg, _)| rg as u32).collect();
        prop_assert_eq!(contains_any(&readers, &any_probes).unwrap(), want_any);
        let groups: Vec<Vec<Vec<u8>>> = all.iter().map(|e| vec![utf8(&name(*e))]).collect();
        let got_all = contains_all(&readers, &groups).unwrap();
        let want_all: RoaringBitmap = layout.iter().enumerate()
            .filter(|(_, rows)| all.iter().all(|e| rows.iter().any(|r| holds(r, *e))))
            .map(|(rg, _)| rg as u32).collect();
        prop_assert_eq!(&got_all, &want_all);
        // Invariant 1 per row: every row group with a row holding all.
        for (rg, rows) in layout.iter().enumerate() {
            if rows.iter().any(|r| all.iter().all(|e| holds(r, *e))) {
                prop_assert!(got_all.contains(rg as u32));
            }
        }
        // Distinct elements over every segment, an element in several
        // segments counted once.
        let distinct: BTreeSet<u8> = layout.iter().flatten().flatten().flatten().flatten().copied().collect();
        prop_assert_eq!(distinct_elements(&readers).unwrap(), distinct.len() as u64);
    }
}
