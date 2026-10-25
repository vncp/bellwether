//! Pinned conformance tests for `bounds` and `temporal2` sidecar segments,
//! format rules, `measure`, `add_batch` atomicity, and corruption sweeps.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::case_sensitive_file_extension_comparisons,
    clippy::too_many_lines
)]

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether_core::encoding::{encode_tuple, encode_tuple_as, Value};
use bellwether_core::segment::{FileRange, ReadAt, PREAMBLE_LEN};
use bellwether_core::sidecar::{
    measure, RgBounds, SidecarDef, SidecarHeader, SidecarKind, SidecarReader, SidecarWriter,
    Triple, BLOCK_KEYS, BLOCK_KEY_BYTES,
};
use bellwether_types::format::{ENCODING_VERSION, STORAGE_VERSION};
use bellwether_types::{Error, Hash, Result, RowGroupId};

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn from_hex(s: &str) -> Vec<u8> {
    s.split_whitespace()
        .map(|b| u8::from_str_radix(b, 16).unwrap())
        .collect()
}

fn utf8_key(s: &str) -> Vec<u8> {
    encode_tuple(&[Value::Utf8(s)]).unwrap()
}

fn bounds_def() -> SidecarDef {
    SidecarDef {
        kind: SidecarKind::Bounds,
        columns: vec!["symbol".into()],
        time_columns: vec!["ts".into()],
    }
}

fn temporal2_def() -> SidecarDef {
    SidecarDef {
        kind: SidecarKind::Bitemporal,
        columns: vec!["symbol".into()],
        time_columns: vec!["valid".into(), "known".into()],
    }
}

/// Pinned tiny bounds sidecar:
/// - def: `bounds:symbol@ts`
/// - writer `SidecarWriter::new` (no recorded types)
/// - `set_rowgroup_range(RowGroupId(0)..RowGroupId(4))`
/// - keys: canonical Utf8 `"AAPL"` (`06 41 41 50 4c 00 00`) and `"MSFT"` (`06 4d 53 46 54 00 00`)
/// - `"AAPL"`: rg 0 `[1, 86_400_000_000]`, rg 2 `[-1, 1]`
/// - `"MSFT"`: rg 1 `[86_400_000_000, 86_400_000_000]`
const PINNED_BOUNDS_HEX: &str = "\
42 57 53 49 44 45 43 52 01 00 00 00 01 00 00 00 74 00 00 00 \
7b 22 73 74 6f 72 61 67 65 5f 76 65 72 73 69 6f 6e 22 3a 31 2c \
22 65 6e 63 6f 64 69 6e 67 5f 76 65 72 73 69 6f 6e 22 3a 31 2c \
22 6b 69 6e 64 22 3a 22 62 6f 75 6e 64 73 22 2c \
22 63 6f 6c 75 6d 6e 73 22 3a 5b 22 73 79 6d 62 6f 6c 22 5d 2c \
22 74 69 6d 65 5f 63 6f 6c 75 6d 6e 73 22 3a 5b 22 74 73 22 5d 2c \
22 63 6f 64 65 63 22 3a 22 6e 6f 6e 65 22 7d \
02 00 00 00 \
07 00 00 00 06 41 41 50 4c 00 00 \
02 00 00 00 \
00 00 00 00 01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00 \
02 00 00 00 ff ff ff ff ff ff ff ff 01 00 00 00 00 00 00 00 \
07 00 00 00 06 4d 53 46 54 00 00 \
01 00 00 00 \
01 00 00 00 00 60 d7 1d 14 00 00 00 00 60 d7 1d 14 00 00 00 \
02 00 00 00 00 00 00 00 01 00 00 00 00 03 00 00 00 01 00 00 00 \
07 00 00 00 06 41 41 50 4c 00 00 \
07 00 00 00 06 4d 53 46 54 00 00 \
88 00 00 00 00 00 00 00 5e 00 00 00 02 00 00 00 \
e6 00 00 00 00 00 00 00 3b 00 00 00 00 00 00 00 \
42 57 53 49 44 45 43 52";

const PINNED_BOUNDS_RECORD_HEX: &str =
    "00 00 00 00 01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00";

/// Pinned tiny temporal2 sidecar:
/// - def: `temporal2:symbol@valid,known`
/// - writer `SidecarWriter::new` (no recorded types)
/// - `set_rowgroup_range(RowGroupId(0)..RowGroupId(4))`
/// - keys: canonical Utf8 `"AAPL"` and `"MSFT"`
/// - `"AAPL"`: `(valid = 1, known = 1, rg = 0)` and `(valid = 1, known = 86_400_000_000, rg = 2)`
/// - `"MSFT"`: `(valid = 86_400_000_000, known = 86_400_000_000, rg = 1)`
const PINNED_TEMPORAL2_HEX: &str = "\
42 57 53 49 44 45 43 52 01 00 00 00 01 00 00 00 82 00 00 00 \
7b 22 73 74 6f 72 61 67 65 5f 76 65 72 73 69 6f 6e 22 3a 31 2c \
22 65 6e 63 6f 64 69 6e 67 5f 76 65 72 73 69 6f 6e 22 3a 31 2c \
22 6b 69 6e 64 22 3a 22 74 65 6d 70 6f 72 61 6c 32 22 2c \
22 63 6f 6c 75 6d 6e 73 22 3a 5b 22 73 79 6d 62 6f 6c 22 5d 2c \
22 74 69 6d 65 5f 63 6f 6c 75 6d 6e 73 22 3a 5b 22 76 61 6c 69 64 22 2c 22 6b 6e 6f 77 6e 22 5d 2c \
22 63 6f 64 65 63 22 3a 22 6e 6f 6e 65 22 7d \
02 00 00 00 \
07 00 00 00 06 41 41 50 4c 00 00 \
02 00 00 00 \
01 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 \
01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00 02 00 00 00 \
07 00 00 00 06 4d 53 46 54 00 00 \
01 00 00 00 \
00 60 d7 1d 14 00 00 00 00 60 d7 1d 14 00 00 00 01 00 00 00 \
02 00 00 00 00 00 00 00 01 00 00 00 00 03 00 00 00 01 00 00 00 \
07 00 00 00 06 41 41 50 4c 00 00 \
07 00 00 00 06 4d 53 46 54 00 00 \
96 00 00 00 00 00 00 00 5e 00 00 00 02 00 00 00 \
f4 00 00 00 00 00 00 00 3b 00 00 00 00 00 00 00 \
42 57 53 49 44 45 43 52";

const PINNED_TEMPORAL2_RECORD_HEX: &str =
    "01 00 00 00 00 00 00 00 00 60 d7 1d 14 00 00 00 02 00 00 00";

fn build_pinned_bounds() -> bellwether_core::EncodedSidecar {
    let aapl = utf8_key("AAPL");
    let msft = utf8_key("MSFT");
    let mut w = SidecarWriter::new(bounds_def()).unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(4)).unwrap();
    w.insert_bounds(&msft, RowGroupId(1), 86_400_000_000, 86_400_000_000)
        .unwrap();
    w.insert_bounds(&aapl, RowGroupId(2), -1, 1).unwrap();
    w.insert_bounds(&aapl, RowGroupId(0), 10, 100).unwrap();
    // Widens rg 0 for AAPL to [1, 86_400_000_000].
    w.insert_bounds(&aapl, RowGroupId(0), 1, 86_400_000_000)
        .unwrap();
    w.finish().unwrap()
}

fn build_pinned_temporal2() -> bellwether_core::EncodedSidecar {
    let aapl = utf8_key("AAPL");
    let msft = utf8_key("MSFT");
    let mut w = SidecarWriter::new(temporal2_def()).unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(4)).unwrap();
    w.insert_triple(&msft, 86_400_000_000, 86_400_000_000, RowGroupId(1))
        .unwrap();
    w.insert_triple(&aapl, 1, 86_400_000_000, RowGroupId(2))
        .unwrap();
    w.insert_triple(&aapl, 1, 1, RowGroupId(0)).unwrap();
    // Duplicate triple is deduplicated.
    w.insert_triple(&aapl, 1, 1, RowGroupId(0)).unwrap();
    w.finish().unwrap()
}

#[test]
fn pinned_bounds_and_temporal2_sidecars_match_byte_for_byte() {
    let b = build_pinned_bounds();
    assert_eq!(hex(&b.bytes), hex(&from_hex(PINNED_BOUNDS_HEX)));
    assert_eq!(b.id, Hash::of(&b.bytes));
    assert_eq!(
        b.id.to_hex(),
        "56b28b58922841137a0d3b51f47293790dd8099934f56292360b683405863b95"
    );
    assert!(b.path().starts_with("segments/") && b.path().ends_with(".seg"));
    // Pin one bounds record (rg=0, min=1, max=86_400_000_000) at offset 0x88 + 4 + 4 + 7 + 4 = 155.
    assert_eq!(
        hex(&b.bytes[155..175]),
        hex(&from_hex(PINNED_BOUNDS_RECORD_HEX))
    );

    let t = build_pinned_temporal2();
    assert_eq!(hex(&t.bytes), hex(&from_hex(PINNED_TEMPORAL2_HEX)));
    assert_eq!(t.id, Hash::of(&t.bytes));
    assert_eq!(
        t.id.to_hex(),
        "63dc33afbfe97a78dcd5003a3390061bca9c20fdf19e811a24c839170548d31c"
    );
    // Pin one temporal2 record (valid=1, known=86_400_000_000, rg=2) at offset 0x96 + 4 + 4 + 7 + 4 + 20 = 189.
    assert_eq!(
        hex(&t.bytes[189..209]),
        hex(&from_hex(PINNED_TEMPORAL2_RECORD_HEX))
    );

    let aapl = utf8_key("AAPL");
    let msft = utf8_key("MSFT");
    let br = SidecarReader::open(&b.bytes).unwrap();
    assert_eq!(br.entries(), 2);
    assert_eq!(br.rowgroup_range(), Some((RowGroupId(0), RowGroupId(3))));
    let got_b = br.bounds(&[&aapl, &msft]).unwrap();
    assert_eq!(
        got_b[&aapl],
        vec![
            RgBounds {
                rg: RowGroupId(0),
                min: 1,
                max: 86_400_000_000,
            },
            RgBounds {
                rg: RowGroupId(2),
                min: -1,
                max: 1,
            },
        ]
    );
    assert_eq!(
        got_b[&msft],
        vec![RgBounds {
            rg: RowGroupId(1),
            min: 86_400_000_000,
            max: 86_400_000_000,
        }]
    );

    let tr = SidecarReader::open(&t.bytes).unwrap();
    assert_eq!(tr.entries(), 2);
    assert_eq!(tr.rowgroup_range(), Some((RowGroupId(0), RowGroupId(3))));
    let got_t = tr.triples(&[&aapl, &msft]).unwrap();
    assert_eq!(
        got_t[&aapl],
        vec![
            Triple {
                valid: 1,
                known: 1,
                rg: RowGroupId(0),
            },
            Triple {
                valid: 1,
                known: 86_400_000_000,
                rg: RowGroupId(2),
            },
        ]
    );
    assert_eq!(
        got_t[&msft],
        vec![Triple {
            valid: 86_400_000_000,
            known: 86_400_000_000,
            rg: RowGroupId(1),
        }]
    );
}

#[test]
fn sidecar_def_and_kind_validation() {
    assert_eq!(SidecarKind::Bounds.as_str(), "bounds");
    assert_eq!(SidecarKind::Bitemporal.as_str(), "temporal2");
    assert_eq!(SidecarKind::parse("bounds"), Some(SidecarKind::Bounds));
    assert_eq!(
        SidecarKind::parse("temporal2"),
        Some(SidecarKind::Bitemporal)
    );
    assert_eq!(SidecarKind::parse("bitemporal"), None);

    assert_eq!(bounds_def().name(), "bounds:symbol@ts");
    assert_eq!(
        SidecarDef {
            kind: SidecarKind::Bitemporal,
            columns: vec!["a".into(), "b".into()],
            time_columns: vec!["valid".into(), "known".into()],
        }
        .name(),
        "temporal2:a,b@valid,known"
    );

    // >= 1 key column
    assert!(SidecarDef {
        kind: SidecarKind::Bounds,
        columns: vec![],
        time_columns: vec!["ts".into()],
    }
    .validate()
    .is_err());

    // bounds requires 1 time column, temporal2 requires 2
    assert!(SidecarDef {
        kind: SidecarKind::Bounds,
        columns: vec!["symbol".into()],
        time_columns: vec!["a".into(), "b".into()],
    }
    .validate()
    .is_err());
    assert!(SidecarDef {
        kind: SidecarKind::Bitemporal,
        columns: vec!["symbol".into()],
        time_columns: vec!["valid".into()],
    }
    .validate()
    .is_err());

    // distinct key columns, distinct time columns, and time columns not in key columns
    assert!(SidecarDef {
        kind: SidecarKind::Bounds,
        columns: vec!["a".into(), "a".into()],
        time_columns: vec!["ts".into()],
    }
    .validate()
    .is_err());
    assert!(SidecarDef {
        kind: SidecarKind::Bounds,
        columns: vec!["ts".into()],
        time_columns: vec!["ts".into()],
    }
    .validate()
    .is_err());
    assert!(SidecarDef {
        kind: SidecarKind::Bitemporal,
        columns: vec!["symbol".into()],
        time_columns: vec!["valid".into(), "valid".into()],
    }
    .validate()
    .is_err());
}

#[test]
fn rg_bounds_overlaps_is_inclusive_on_both_ends() {
    let b = RgBounds {
        rg: RowGroupId(0),
        min: 10,
        max: 20,
    };
    assert!(b.overlaps(10, 20));
    assert!(b.overlaps(0, 10));
    assert!(b.overlaps(20, 30));
    assert!(b.overlaps(15, 15));
    assert!(!b.overlaps(0, 9));
    assert!(!b.overlaps(21, 30));
    assert!(!b.overlaps(20, 10));
}

#[test]
fn multi_block_lookups_and_memory_bound_read_count() {
    struct Counting {
        bytes: Vec<u8>,
        reads: RefCell<Vec<usize>>,
    }
    impl ReadAt for Counting {
        fn size(&self) -> Result<u64> {
            Ok(self.bytes.len() as u64)
        }
        fn read_at(&self, offset: u64, len: usize) -> Result<Cow<'_, [u8]>> {
            self.reads.borrow_mut().push(len);
            let o = usize::try_from(offset).unwrap();
            Ok(Cow::Borrowed(&self.bytes[o..o + len]))
        }
    }

    let n = (BLOCK_KEYS as u32) * 3 + 17;
    let mut w = SidecarWriter::new(bounds_def()).unwrap();
    for i in 0..n {
        let k = encode_tuple(&[Value::Int(i64::from(i) * 2)]).unwrap();
        w.insert_bounds(&k, RowGroupId(i % 7), i64::from(i), i64::from(i) + 10)
            .unwrap();
    }
    let seg = w.finish().unwrap();

    // Test FileRange reader as well
    let dir = std::env::temp_dir().join(format!("bw-sidecar-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{}.seg", seg.id.to_hex()));
    std::fs::write(&path, &seg.bytes).unwrap();
    let file_reader = SidecarReader::open(FileRange::open(&path).unwrap()).unwrap();
    assert_eq!(file_reader.entries(), u64::from(n));

    let r = SidecarReader::open(Counting {
        bytes: seg.bytes,
        reads: RefCell::new(Vec::new()),
    })
    .unwrap();
    // Open reads preamble, header, footer, index = 4 reads.
    assert_eq!(r.def(), &bounds_def());

    // Also test block splitting by BLOCK_KEY_BYTES (64 KiB)
    let mut w_big_keys = SidecarWriter::new(bounds_def()).unwrap();
    let big_str = "x".repeat(4000);
    for i in 0..25u32 {
        let k = encode_tuple(&[Value::Utf8(&format!("{big_str}-{i:04}"))]).unwrap();
        assert!(k.len() > 4000);
        w_big_keys
            .insert_bounds(&k, RowGroupId(0), i64::from(i), i64::from(i))
            .unwrap();
    }
    const { assert!(25 * 4000 > BLOCK_KEY_BYTES) };
    let big_seg = w_big_keys.finish().unwrap();
    let big_r = SidecarReader::open(&big_seg.bytes).unwrap();
    let probe_big = encode_tuple(&[Value::Utf8(&format!("{big_str}-0020"))]).unwrap();
    assert_eq!(big_r.bounds(&[&probe_big]).unwrap()[&probe_big][0].min, 20);

    // Probe: before first block (-2), inside block 0 (4), between block 0 and block 1
    // (odd key at boundary: (BLOCK_KEYS*2 - 1)), inside block 2 (BLOCK_KEYS*4 + 6), after last block.
    let before = encode_tuple(&[Value::Int(-2)]).unwrap();
    let in_b0_a = encode_tuple(&[Value::Int(4)]).unwrap();
    let in_b0_b = encode_tuple(&[Value::Int(8)]).unwrap();
    let between_b0_b1 = encode_tuple(&[Value::Int((BLOCK_KEYS as i64) * 2 - 1)]).unwrap();
    let in_b2 = encode_tuple(&[Value::Int((BLOCK_KEYS as i64) * 4 + 6)]).unwrap();
    let after = encode_tuple(&[Value::Int(i64::from(n) * 2 + 10)]).unwrap();

    // Two keys in the same block plus absent probes outside blocks -> only 1 block read!
    let got_file = file_reader
        .bounds(&[&before, &in_b0_b, &in_b0_a, &between_b0_b1, &after])
        .unwrap();
    let got_mem = r
        .bounds(&[&before, &in_b0_b, &in_b0_a, &between_b0_b1, &after])
        .unwrap();
    assert_eq!(got_file, got_mem);
    assert_eq!(got_mem.len(), 2);
    assert_eq!(got_mem[&in_b0_a][0].min, 2);
    assert_eq!(got_mem[&in_b0_b][0].min, 4);

    let got_b2 = r.bounds(&[&in_b2]).unwrap();
    assert_eq!(got_b2.len(), 1);
    assert_eq!(got_b2[&in_b2][0].min, (BLOCK_KEYS as i64) * 2 + 3);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn add_batch_is_all_or_nothing_and_leaves_writer_usable() {
    let dt_key = DataType::Decimal128(10, 2);
    let dt_ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Schema::new(vec![
        Field::new("amt", dt_key.clone(), true),
        Field::new("ts", dt_ts.clone(), true),
    ]);
    let def = SidecarDef {
        kind: SidecarKind::Bounds,
        columns: vec!["amt".into()],
        time_columns: vec!["ts".into()],
    };
    let mut w = SidecarWriter::for_dataset(def.clone(), &schema).unwrap();

    let batch_with_scale = |scale: i8, amts: &[i128], ts: &[i64]| -> RecordBatch {
        let a: ArrayRef = Arc::new(
            Decimal128Array::from(amts.to_vec())
                .with_precision_and_scale(18, scale)
                .unwrap(),
        );
        let t: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(ts.to_vec()).with_timezone("UTC"));
        RecordBatch::try_from_iter(vec![("amt", a), ("ts", t)]).unwrap()
    };

    // Row 0 converts (1.500 -> 1.50), Row 1 fails (1.234 not exact at scale 2), Row 2 converts (2.000 -> 2.00)
    assert!(w
        .add_batch(
            RowGroupId(0),
            &batch_with_scale(3, &[1500, 1234, 2000], &[10, 20, 30])
        )
        .is_err());

    // Writer remains usable: add a valid batch at rg 1
    w.add_batch(RowGroupId(1), &batch_with_scale(3, &[1500], &[99]))
        .unwrap();
    let seg = w.finish().unwrap();
    let r = SidecarReader::open_for_dataset(&seg.bytes, &schema).unwrap();
    let k150 = encode_tuple_as(
        &[Value::Decimal128 {
            value: 150,
            scale: 2,
        }],
        std::slice::from_ref(&dt_key),
    )
    .unwrap();
    let k200 = encode_tuple_as(
        &[Value::Decimal128 {
            value: 200,
            scale: 2,
        }],
        std::slice::from_ref(&dt_key),
    )
    .unwrap();
    let res = r.bounds(&[&k150, &k200]).unwrap();
    assert_eq!(
        res.get(&k150),
        Some(&vec![RgBounds {
            rg: RowGroupId(1),
            min: 99,
            max: 99,
        }]),
        "rg 0 must not have been recorded"
    );
    assert!(!res.contains_key(&k200));

    // Also verify temporal2 add_batch atomicity when a timestamp fails to convert (e.g. ms not divisible by 1_000 to s)
    let schema_t2 = Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("valid", DataType::Timestamp(TimeUnit::Second, None), true),
        Field::new("known", DataType::Timestamp(TimeUnit::Second, None), true),
    ]);
    let def_t2 = SidecarDef {
        kind: SidecarKind::Bitemporal,
        columns: vec!["sym".into()],
        time_columns: vec!["valid".into(), "known".into()],
    };
    let mut w2 = SidecarWriter::for_dataset(def_t2, &schema_t2).unwrap();
    let bad_t2 = RecordBatch::try_from_iter(vec![
        (
            "sym",
            Arc::new(StringArray::from(vec!["A", "B"])) as ArrayRef,
        ),
        (
            "valid",
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_000, 1_000_000])) as ArrayRef,
        ),
        (
            "known",
            Arc::new(TimestampMicrosecondArray::from(vec![2_000_000, 2_000_001])) as ArrayRef,
        ),
    ])
    .unwrap();
    assert!(w2.add_batch(RowGroupId(0), &bad_t2).is_err());
    let good_t2 = RecordBatch::try_from_iter(vec![
        ("sym", Arc::new(StringArray::from(vec!["A"])) as ArrayRef),
        (
            "valid",
            Arc::new(TimestampMicrosecondArray::from(vec![3_000_000])) as ArrayRef,
        ),
        (
            "known",
            Arc::new(TimestampMicrosecondArray::from(vec![4_000_000])) as ArrayRef,
        ),
    ])
    .unwrap();
    w2.add_batch(RowGroupId(1), &good_t2).unwrap();
    let seg2 = w2.finish().unwrap();
    let r2 = SidecarReader::open_for_dataset(&seg2.bytes, &schema_t2).unwrap();
    let ka = utf8_key("A");
    let kb = utf8_key("B");
    let got2 = r2.triples(&[&ka, &kb]).unwrap();
    assert_eq!(
        got2.get(&ka),
        Some(&vec![Triple {
            valid: 3,
            known: 4,
            rg: RowGroupId(1),
        }])
    );
    assert!(!got2.contains_key(&kb));
}

#[test]
fn open_for_dataset_checks_recorded_types_and_names_column_and_both_types() {
    let schema1 = Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]);
    let mut w = SidecarWriter::for_dataset(bounds_def(), &schema1).unwrap();
    let b = RecordBatch::try_from_iter(vec![
        (
            "symbol",
            Arc::new(StringArray::from(vec!["AAPL"])) as ArrayRef,
        ),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(vec![Some(100)]).with_timezone("UTC"))
                as ArrayRef,
        ),
    ])
    .unwrap();
    w.add_batch(RowGroupId(0), &b).unwrap();
    let seg = w.finish().unwrap();

    assert!(SidecarReader::open_for_dataset(&seg.bytes, &schema1).is_ok());

    // Wrong key type: Int64 instead of Utf8
    let bad_key_schema = Schema::new(vec![
        Field::new("symbol", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]);
    let err = SidecarReader::open_for_dataset(&seg.bytes, &bad_key_schema)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("symbol") && err.contains("Utf8") && err.contains("Int64"),
        "expected error naming column and both types, got: {err}"
    );

    // Wrong time type: Date32 instead of Timestamp(us, UTC)
    let bad_time_schema = Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts", DataType::Date32, true),
    ]);
    let err = SidecarReader::open_for_dataset(&seg.bytes, &bad_time_schema)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("ts") && err.contains("Timestamp") && err.contains("Date32"),
        "expected error naming time column and both types, got: {err}"
    );

    // Untyped segment refused by open_for_dataset
    let untyped = build_pinned_bounds();
    assert!(SidecarReader::open_for_dataset(&untyped.bytes, &schema1).is_err());
}

#[test]
fn measure_matches_specified_upper_bound() {
    let schema = Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("tag", DataType::Int64, true),
        Field::new("valid", DataType::Int64, true),
        Field::new("known", DataType::Int64, true),
    ]);
    let b_def = SidecarDef {
        kind: SidecarKind::Bounds,
        columns: vec!["symbol".into(), "tag".into()],
        time_columns: vec!["valid".into()],
    };
    let t_def = SidecarDef {
        kind: SidecarKind::Bitemporal,
        columns: vec!["symbol".into(), "tag".into()],
        time_columns: vec!["valid".into(), "known".into()],
    };
    let batch = RecordBatch::try_from_iter(vec![
        (
            "symbol",
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("A"),
                Some("A"),
                None, // leading key null -> skipped
                Some("B"),
                Some("C"),
            ])) as ArrayRef,
        ),
        (
            "tag",
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                None, // trailing key null -> encoded as 0x00 marker
                Some(2),
            ])) as ArrayRef,
        ),
        (
            "valid",
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(10),
                Some(20),
                Some(10),
                Some(30),
                None, // null ts -> skipped
            ])) as ArrayRef,
        ),
        (
            "known",
            Arc::new(Int64Array::from(vec![
                Some(100),
                Some(100), // duplicate (A, 1, 10, 100)
                Some(200),
                Some(100),
                None, // null known -> skipped in temporal2, kept in bounds (which only uses valid)
                Some(300),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();

    let ka = encode_tuple(&[Value::Utf8("A"), Value::Int(1)]).unwrap();
    let mut kb = encode_tuple(&[Value::Utf8("B")]).unwrap();
    kb.push(0x00);

    // For bounds: ("A", 1) appears twice with non-null valid -> 1 distinct (key, rg); ("B", null) has valid=30 -> 1 distinct (key, rg).
    let (b_keys, b_bytes) = measure(
        &b_def,
        &b_def.key_types(&schema).unwrap(),
        &b_def.time_types(&schema).unwrap(),
        &batch,
    )
    .unwrap();
    assert_eq!(b_keys, 2);
    assert_eq!(b_bytes, (ka.len() + 20 + kb.len() + 20) as u64);

    // For temporal2: ("A", 1) has two distinct (valid, known) pairs: (10, 100) and (20, 200).
    // ("B", null) has null known -> skipped! ("C", 2) has null valid -> skipped!
    let (t_keys, t_bytes) = measure(
        &t_def,
        &t_def.key_types(&schema).unwrap(),
        &t_def.time_types(&schema).unwrap(),
        &batch,
    )
    .unwrap();
    assert_eq!(t_keys, 1);
    assert_eq!(t_bytes, (ka.len() + 2 * 20) as u64);
}

// ---------------------------------------------------------------------------
// Corruption sweep
// ---------------------------------------------------------------------------

thread_local! {
    static QUIET: Cell<bool> = const { Cell::new(false) };
}

fn quietly(f: impl FnOnce()) -> bool {
    static HOOK: std::sync::Once = std::sync::Once::new();
    HOOK.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !QUIET.with(Cell::get) {
                prev(info);
            }
        }));
    });
    QUIET.with(|q| q.set(true));
    let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_ok();
    QUIET.with(|q| q.set(false));
    ok
}

fn exercise_sidecar(bytes: Vec<u8>) {
    let probes = [utf8_key("AAPL"), utf8_key("MSFT"), utf8_key("GOOG")];
    if let Ok(r) = SidecarReader::open(bytes) {
        let _ = r.bounds(&probes);
        let _ = r.triples(&probes);
    }
}

#[test]
fn newer_version_in_preamble_returns_unsupported_version_before_header_parse() {
    for seg in [build_pinned_bounds(), build_pinned_temporal2()] {
        for (offset, what, ver) in [
            (8usize, "storage", STORAGE_VERSION + 1),
            (12usize, "encoding", ENCODING_VERSION + 1),
        ] {
            let mut b = seg.bytes.clone();
            b[offset..offset + 4].copy_from_slice(&ver.to_le_bytes());
            // Even if the JSON header is garbage, preamble check runs first!
            b[PREAMBLE_LEN] = b'!';
            match SidecarReader::open(b) {
                Err(Error::UnsupportedVersion {
                    what: w,
                    found,
                    supported: _,
                }) => {
                    assert_eq!(w, what);
                    assert_eq!(found, ver);
                }
                other => panic!("expected UnsupportedVersion({what}), got {other:?}"),
            }
        }
    }
}

#[test]
fn header_validation_refuses_disagreements_unknown_kind_codec_and_preserves_extra() {
    let seg = build_pinned_bounds();
    let header_len = u32::from_le_bytes(seg.bytes[16..20].try_into().unwrap()) as usize;
    let json = std::str::from_utf8(&seg.bytes[PREAMBLE_LEN..PREAMBLE_LEN + header_len]).unwrap();

    let patch = |from: &str, to: &str| -> Vec<u8> {
        assert_eq!(from.len(), to.len());
        let swapped = json.replace(from, to);
        let mut b = seg.bytes.clone();
        b[PREAMBLE_LEN..PREAMBLE_LEN + header_len].copy_from_slice(swapped.as_bytes());
        b
    };

    assert!(SidecarReader::open(patch("\"storage_version\":1", "\"storage_version\":0")).is_err());
    assert!(
        SidecarReader::open(patch("\"encoding_version\":1", "\"encoding_version\":0")).is_err()
    );
    assert!(SidecarReader::open(patch("\"codec\":\"none\"", "\"codec\":\"zstd\"")).is_err());
    assert!(SidecarReader::open(patch("\"kind\":\"bounds\"", "\"kind\":\"bogus!\"")).is_err());

    // Unknown header fields are preserved in `extra`
    let mut h: SidecarHeader = serde_json::from_str(json).unwrap();
    assert_eq!(h.extra.len(), 0);
    h.extra
        .insert("future_note".into(), serde_json::json!("preserved"));
    let s = serde_json::to_string(&h).unwrap();
    let round: SidecarHeader = serde_json::from_str(&s).unwrap();
    assert_eq!(
        round.extra.get("future_note"),
        Some(&serde_json::json!("preserved"))
    );
}

#[test]
fn truncation_and_byte_flip_sweep_errors_or_never_panics() {
    let mut typed_w = SidecarWriter::for_dataset(
        bounds_def(),
        &Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]),
    )
    .unwrap();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "symbol",
            Arc::new(StringArray::from(vec!["AAPL", "MSFT"])) as ArrayRef,
        ),
        ("ts", Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef),
    ])
    .unwrap();
    typed_w.add_batch(RowGroupId(0), &batch).unwrap();
    let typed_seg = typed_w.finish().unwrap();

    let fixtures = [
        ("bounds", build_pinned_bounds().bytes),
        ("temporal2", build_pinned_temporal2().bytes),
        ("typed_bounds", typed_seg.bytes),
    ];

    for (name, bytes) in &fixtures {
        let header_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let body_start = PREAMBLE_LEN + header_len;
        let footer_off = bytes.len() - 24;
        let index_off = usize::try_from(u64::from_le_bytes(
            bytes[footer_off..footer_off + 8].try_into().unwrap(),
        ))
        .unwrap();

        // 1. Every truncation of a valid file MUST error and never panic.
        for len in 0..bytes.len() {
            let truncated = bytes[..len].to_vec();
            let mut opened_and_queried_ok = false;
            assert!(
                quietly(|| {
                    if let Ok(r) = SidecarReader::open(truncated) {
                        let probes = [utf8_key("AAPL"), utf8_key("MSFT")];
                        let res_b = r.bounds(&probes);
                        let res_t = r.triples(&probes);
                        if res_b.is_ok() || res_t.is_ok() {
                            opened_and_queried_ok = true;
                        }
                    }
                }),
                "{name} panicked when truncated to {len} bytes"
            );
            assert!(
                !opened_and_queried_ok,
                "{name} truncated to {len} bytes did not error"
            );
        }

        // 2. Every single-byte flip (and fixed values) across the entire file:
        //    - in preamble (0..PREAMBLE_LEN) and index/footer (index_off..bytes.len()):
        //      every single-bit flip in preamble/index/footer that changes the structural
        //      metadata (or any flip anywhere) must never panic; moreover, flips in magic,
        //      versions, header_len, index_offset, index_len, block_count, off, len, n,
        //      has_rg, first_key_len, last_key_len, entries MUST error.
        for pos in 0..bytes.len() {
            let orig = bytes[pos];
            for bit in 0..8u8 {
                let b = orig ^ (1u8 << bit);
                let mut v = bytes.clone();
                v[pos] = b;
                let mut query_ok = false;
                assert!(
                    quietly(|| {
                        let probes = [utf8_key("AAPL"), utf8_key("MSFT")];
                        if let Ok(r) = SidecarReader::open(v) {
                            match r.def().kind {
                                SidecarKind::Bounds => {
                                    if r.bounds(&probes).is_ok() {
                                        query_ok = true;
                                    }
                                }
                                SidecarKind::Bitemporal => {
                                    if r.triples(&probes).is_ok() {
                                        query_ok = true;
                                    }
                                }
                            }
                        }
                    }),
                    "{name}: single-bit flip at byte {pos} (bit {bit}) panicked"
                );
                // Every single-bit flip in preamble (0..20) or footer (footer_off..end)
                // must be rejected on open!
                if pos < PREAMBLE_LEN || pos >= footer_off {
                    assert!(
                        !query_ok,
                        "{name}: single-bit flip in preamble/footer at pos {pos} (bit {bit}) was not rejected"
                    );
                }
            }
            for b in [0x00u8, 0x01, 0x7F, 0x80, 0xFF, orig.wrapping_add(1)] {
                if b == orig {
                    continue;
                }
                let mut v = bytes.clone();
                v[pos] = b;
                assert!(
                    quietly(|| exercise_sidecar(v)),
                    "{name}: byte {b:#04x} at {pos} (body_start={body_start}, index_off={index_off}) panicked"
                );
            }
        }
    }
}

/// The `| `label` | `value` |` rows of the sidecar vector table in
/// `docs/format.md`, which must list exactly the vectors pinned here.
#[test]
fn sidecar_table_in_format_md_matches_the_pinned_vectors() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/format.md");
    let doc = std::fs::read_to_string(path).unwrap();
    let section = doc
        .split("## Sidecar conformance vectors")
        .nth(1)
        .expect("docs/format.md has a sidecar conformance vectors section");
    let section = section.split("\n## ").next().unwrap_or(section);
    let mut rows = std::collections::BTreeMap::new();
    for line in section.lines() {
        let cells: Vec<&str> = line
            .split('|')
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .collect();
        if let [label, value] = cells[..] {
            if let (Some(l), Some(v)) = (
                label.strip_prefix('`').and_then(|l| l.strip_suffix('`')),
                value.strip_prefix('`').and_then(|v| v.strip_suffix('`')),
            ) {
                rows.insert(l.to_string(), v.to_string());
            }
        }
    }
    let b = build_pinned_bounds();
    let t = build_pinned_temporal2();
    let want: std::collections::BTreeMap<String, String> = [
        (
            "bounds record rg=0 min=1 max=86400000000",
            hex(&b.bytes[155..175]),
        ),
        (
            "temporal2 record valid=1 known=86400000000 rg=2",
            hex(&t.bytes[189..209]),
        ),
        ("bounds:symbol@ts blake3", b.id.to_hex()),
        ("temporal2:symbol@valid,known blake3", t.id.to_hex()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    let rows: std::collections::BTreeMap<String, String> = rows
        .into_iter()
        .map(|(k, v)| {
            let v = if v.contains(' ') {
                hex(&from_hex(&v))
            } else {
                v
            };
            (k, v)
        })
        .collect();
    assert_eq!(rows, want);
}
