//! Segment format: range reads from disk, version, codec, and `bloom_hash`
//! refusal, forward-compatible headers, read accounting for the ADR 018
//! memory bound, merge, NaN range probes, and writer rules. The corrupt-byte
//! sweep lives in `corrupt_sweep.rs`.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::needless_for_each,
    clippy::case_sensitive_file_extension_comparisons
)]

mod common;

use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::bloom::{parquet_plain, BloomHash, CanonicalHash, PlainHash, Sbbf};
use bellwether_core::encoding::{encode_tuple, encode_tuple_as, Value, NAN_KEY};
use bellwether_core::segment::{ColumnType, FileRange, ReadAt, SegmentHeader, PREAMBLE_LEN};
use bellwether_core::{merge, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::format::STORAGE_VERSION;
use bellwether_types::{Error, KeySpec, Result, RowGroupId};
use common::*;

fn big_exact(n: u32) -> bellwether_core::EncodedSegment {
    let mut w = SegmentWriter::new(exact(), WriterOptions::default()).unwrap();
    for i in 0..n {
        w.insert(&key(i64::from(i), "x"), RowGroupId(i % 97))
            .unwrap();
        w.insert(&key(i64::from(i), "x"), RowGroupId(i % 13 + 200))
            .unwrap();
    }
    w.finish().unwrap()
}

#[test]
fn file_range_reader_matches_in_memory_reader() {
    let seg = big_exact(5000);
    assert!(seg.path().starts_with("segments/") && seg.path().ends_with(".seg"));
    assert_eq!(seg.id, bellwether_types::Hash::of(&seg.bytes));
    let dir = std::env::temp_dir().join(format!("bw-core-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{}.seg", seg.id.to_hex()));
    std::fs::write(&path, &seg.bytes).unwrap();
    let file = SegmentReader::open(FileRange::open(&path).unwrap()).unwrap();
    let mem = SegmentReader::open(seg.bytes.clone()).unwrap();
    assert_eq!(file.entries(), 5000);
    assert_eq!(
        file.rowgroup_range(),
        Some((RowGroupId(0), RowGroupId(212)))
    );
    let keys: Vec<Vec<u8>> = (0..50)
        .map(|i| key(i * 97, "x"))
        .chain([key(-1, "x")])
        .collect();
    let got = file.lookup(&keys).unwrap();
    assert_eq!(got, mem.lookup(&keys).unwrap());
    let one = file.lookup(&[key(98, "x")]).unwrap();
    assert_eq!(one.iter().collect::<Vec<_>>(), vec![1, 207]);
    assert!(file.lookup(&[key(5000, "x")]).unwrap().is_empty());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn newer_storage_or_encoding_version_is_refused() {
    let seg = big_exact(10);
    for (offset, what) in [(8usize, "storage"), (12, "encoding")] {
        let mut b = seg.bytes.clone();
        b[offset..offset + 4].copy_from_slice(&(STORAGE_VERSION + 1).to_le_bytes());
        match SegmentReader::open(b) {
            Err(Error::UnsupportedVersion { what: w, found, .. }) => {
                assert_eq!(w, what);
                assert_eq!(found, STORAGE_VERSION + 1);
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }
}

#[test]
fn unknown_codec_is_refused_and_unknown_header_fields_are_preserved() {
    let seg = big_exact(10);
    let header_len = u32::from_le_bytes(seg.bytes[16..20].try_into().unwrap()) as usize;
    let json = std::str::from_utf8(&seg.bytes[PREAMBLE_LEN..PREAMBLE_LEN + header_len]).unwrap();
    // Same-length substitution keeps every absolute offset valid.
    let swapped = json.replace("\"codec\":\"none\"", "\"codec\":\"zstd\"");
    assert_eq!(swapped.len(), json.len());
    let mut b = seg.bytes.clone();
    b[PREAMBLE_LEN..PREAMBLE_LEN + header_len].copy_from_slice(swapped.as_bytes());
    assert!(SegmentReader::open(b).is_err());

    let mut h: SegmentHeader = serde_json::from_str(json).unwrap();
    h.extra
        .insert("future_field".into(), serde_json::json!({"x": 1}));
    let round: SegmentHeader = serde_json::from_str(&serde_json::to_string(&h).unwrap()).unwrap();
    assert_eq!(
        round.extra.get("future_field"),
        Some(&serde_json::json!({"x": 1}))
    );
}

/// Counts reads, to show what one lookup touches.
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

#[test]
fn single_key_lookup_reads_one_block_and_one_value() {
    let seg = big_exact(200_000);
    let total = seg.bytes.len();
    let r = SegmentReader::open(Counting {
        bytes: seg.bytes,
        reads: RefCell::new(Vec::new()),
    })
    .unwrap();
    let opened: usize = r_reads(&r).iter().sum();
    assert!(
        opened < total / 20,
        "open reads only header and index: {opened} of {total}"
    );
    clear(&r);
    let got = r.lookup(&[key(123_456, "x")]).unwrap();
    assert_eq!(got.len(), 2);
    let reads = r_reads(&r);
    assert_eq!(reads.len(), 3, "bloom, key directory, value: {reads:?}");
    assert!(reads.iter().sum::<usize>() < 128 * 1024);
    clear(&r);
    // An absent key inside a block's range is usually stopped by the bloom.
    let _ = r.lookup(&[key(123_456, "y")]).unwrap();
    assert!(r_reads(&r).len() <= 2);
}

fn r_reads(r: &SegmentReader<Counting>) -> Vec<usize> {
    reader_src(r).reads.borrow().clone()
}
fn clear(r: &SegmentReader<Counting>) {
    reader_src(r).reads.borrow_mut().clear();
}
fn reader_src(r: &SegmentReader<Counting>) -> &Counting {
    r.source()
}

#[test]
fn merge_equals_union_and_rejects_mixed_specs() {
    let mut a = SegmentWriter::new(temporal(), WriterOptions::default()).unwrap();
    a.insert_version(&key(1, "a"), Some(5), RowGroupId(1))
        .unwrap();
    a.insert_version(&key(2, "a"), None, RowGroupId(1)).unwrap();
    let mut b = SegmentWriter::new(temporal(), WriterOptions::default()).unwrap();
    b.insert_version(&key(1, "a"), Some(9), RowGroupId(7))
        .unwrap();
    b.insert_version(&key(1, "a"), Some(9), RowGroupId(8))
        .unwrap();
    let readers = open(&[a.finish().unwrap(), b.finish().unwrap()]);
    let m = SegmentReader::open(merge(&readers, &WriterOptions::default()).unwrap().bytes).unwrap();
    assert_eq!(m.entries(), 2);
    let l = m.latest(&[key(1, "a"), key(2, "a")]).unwrap();
    assert_eq!(l.len(), 1, "a key with only null timestamps has no latest");
    let h = &l[&key(1, "a")];
    assert_eq!(
        (h.ts, h.row_groups.iter().collect::<Vec<_>>()),
        (9, vec![7, 8])
    );
    assert_eq!(m.asof(&[key(1, "a")], 6).unwrap()[&key(1, "a")].ts, 5);
    assert_eq!(
        m.lookup(&[key(2, "a")]).unwrap().iter().collect::<Vec<_>>(),
        vec![1]
    );

    let other = open(&[big_exact(3)]);
    let mixed: Vec<_> = readers.into_iter().chain(other).collect();
    assert!(matches!(
        merge(&mixed, &WriterOptions::default()),
        Err(Error::KeySpec(_))
    ));
}

#[test]
fn writer_rules() {
    let mut t = SegmentWriter::new(temporal(), WriterOptions::default()).unwrap();
    assert!(t.insert(&key(1, "a"), RowGroupId(0)).is_err());
    let mut e = SegmentWriter::new(exact(), WriterOptions::default()).unwrap();
    assert!(e
        .insert_version(&key(1, "a"), Some(1), RowGroupId(0))
        .is_err());
    assert!(SegmentWriter::new(bloom(1.0), WriterOptions::default()).is_err());
    assert!(SegmentWriter::new(bloom(0.0), WriterOptions::default()).is_err());
    assert!(
        SegmentWriter::new(KeySpec::Exact { columns: vec![] }, WriterOptions::default()).is_err()
    );
    // Range lookups are refused on bloom segments; latest on non-temporal.
    let mut b = SegmentWriter::new(bloom(0.01), WriterOptions::default()).unwrap();
    b.insert(&key(1, "a"), RowGroupId(3)).unwrap();
    let r = SegmentReader::open(b.finish().unwrap().bytes).unwrap();
    assert!(r.lookup_range(&key(0, "a"), &key(2, "a")).is_err());
    assert!(r.latest(&[key(1, "a")]).is_err());
    assert!(r.lookup(&[key(1, "a")]).unwrap().contains(3));
}

#[test]
fn empty_segments_round_trip() {
    for spec in [exact(), bloom(0.01), range(), temporal()] {
        let s = SegmentWriter::new(spec.clone(), WriterOptions::default())
            .unwrap()
            .finish()
            .unwrap();
        let r = SegmentReader::open(s.bytes).unwrap();
        assert_eq!(r.key_spec(), &spec);
        assert_eq!(r.entries(), 0);
        assert_eq!(r.rowgroup_range(), None);
        assert!(r.lookup(&[key(1, "a")]).unwrap().is_empty());
    }
}

/// Rewrites the JSON header in place with a same-length substitution, which
/// keeps every absolute offset valid.
fn patch_header(bytes: &[u8], from: &str, to: &str) -> Vec<u8> {
    assert_eq!(from.len(), to.len());
    let header_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let json = std::str::from_utf8(&bytes[PREAMBLE_LEN..PREAMBLE_LEN + header_len]).unwrap();
    assert!(json.contains(from), "{json}");
    let swapped = json.replace(from, to);
    let mut b = bytes.to_vec();
    b[PREAMBLE_LEN..PREAMBLE_LEN + header_len].copy_from_slice(swapped.as_bytes());
    b
}

#[test]
fn header_records_bloom_hash_per_layout_and_refuses_unknown_values() {
    // every header carries bloom_hash, range segments included
    for spec in [exact(), temporal(), bloom(0.01), range()] {
        let s = SegmentWriter::new(spec, WriterOptions::default())
            .unwrap()
            .finish()
            .unwrap();
        let header_len = u32::from_le_bytes(s.bytes[16..20].try_into().unwrap()) as usize;
        let json = std::str::from_utf8(&s.bytes[PREAMBLE_LEN..PREAMBLE_LEN + header_len]).unwrap();
        assert!(json.contains("\"bloom_hash\":\"canonical\""), "{json}");
        assert_eq!(
            SegmentReader::open(s.bytes).unwrap().bloom_hash(),
            BloomHash::Canonical
        );
    }
    let seg = big_exact(10);
    let b = patch_header(
        &seg.bytes,
        "\"bloom_hash\":\"canonical\"",
        "\"bloom_hash\":\"canonicak\"",
    );
    assert!(SegmentReader::open(b).is_err(), "unknown bloom_hash");
    // a header without bloom_hash is refused
    let mut v = serde_json::to_value(SegmentHeader::new(exact())).unwrap();
    v.as_object_mut().unwrap().remove("bloom_hash");
    assert!(serde_json::from_value::<SegmentHeader>(v).is_err());
    // only bloom segments may hold parquet_plain filters
    for spec in [exact(), temporal(), range()] {
        let mut h = SegmentHeader::new(spec);
        h.bloom_hash = BloomHash::ParquetPlain;
        assert!(h.validate(h.storage_version, h.encoding_version).is_err());
    }
}

#[test]
fn header_versions_must_agree_with_the_preamble() {
    let seg = big_exact(10);
    let b = patch_header(&seg.bytes, "\"storage_version\":1", "\"storage_version\":0");
    assert!(SegmentReader::open(b).is_err());
    let b = patch_header(
        &seg.bytes,
        "\"encoding_version\":1",
        "\"encoding_version\":0",
    );
    assert!(SegmentReader::open(b).is_err());
}

fn plain_filter(values: &[i64]) -> Sbbf<PlainHash> {
    let mut f = Sbbf::<PlainHash>::with_ndv_fpp(values.len() as u64, 0.01).unwrap();
    for v in values {
        f.insert_hash(parquet_plain::hash_i64(*v));
    }
    f
}

#[test]
fn reused_parquet_filters_are_probed_only_with_plain_hashes() {
    let single = KeySpec::Bloom {
        columns: vec!["id".into()],
        fpr: 0.01,
    };
    let mut w = SegmentWriter::new(single.clone(), WriterOptions::default()).unwrap();
    w.insert_filter(RowGroupId(2), 3, &plain_filter(&[10, 20, 30]))
        .unwrap();
    w.insert_filter(RowGroupId(5), 1, &plain_filter(&[40]))
        .unwrap();
    // one hash kind per segment
    assert!(w.insert(&id_key(1), RowGroupId(9)).is_err());
    let r = SegmentReader::open(w.finish().unwrap().bytes).unwrap();
    assert_eq!(r.bloom_hash(), BloomHash::ParquetPlain);
    let got = r
        .lookup_hashes(&[parquet_plain::hash_i64(20), parquet_plain::hash_i64(40)])
        .unwrap();
    assert_eq!(got.iter().collect::<Vec<_>>(), vec![2, 5]);
    // canonical probes are refused, never silently missed
    assert!(r.lookup(&[id_key(20)]).is_err());
    assert!(r.lookup_hashes(&[CanonicalHash::of(&id_key(20))]).is_err());

    // parquet_plain filters cover one column only
    let mut w = SegmentWriter::new(bloom(0.01), WriterOptions::default()).unwrap();
    assert!(w
        .insert_filter(RowGroupId(0), 1, &plain_filter(&[1]))
        .is_err());
    // a canonical segment refuses plain filters
    let mut w = SegmentWriter::new(single.clone(), WriterOptions::default()).unwrap();
    w.insert(&id_key(1), RowGroupId(0)).unwrap();
    assert!(w
        .insert_filter(RowGroupId(1), 1, &plain_filter(&[1]))
        .is_err());
    let canonical = SegmentReader::open(w.finish().unwrap().bytes).unwrap();

    // merge refuses to mix hash kinds
    let mut w = SegmentWriter::new(single, WriterOptions::default()).unwrap();
    w.insert_filter(RowGroupId(7), 1, &plain_filter(&[1]))
        .unwrap();
    let plain = SegmentReader::open(w.finish().unwrap().bytes).unwrap();
    assert!(merge(&[canonical, plain], &WriterOptions::default()).is_err());
}

#[test]
fn bloom_merge_keeps_filters_of_different_sizes_for_one_row_group() {
    let mut a = SegmentWriter::new(bloom(0.01), WriterOptions::default()).unwrap();
    a.insert(&key(1, "a"), RowGroupId(3)).unwrap();
    let mut b = SegmentWriter::new(bloom(0.01), WriterOptions::default()).unwrap();
    for i in 0..5000 {
        b.insert(&key(i, "b"), RowGroupId(3)).unwrap();
    }
    b.insert(&key(1, "c"), RowGroupId(4)).unwrap();
    let rs = open(&[a.finish().unwrap(), b.finish().unwrap()]);
    let m = SegmentReader::open(merge(&rs, &WriterOptions::default()).unwrap().bytes).unwrap();
    assert_eq!(m.block_count(), 3, "two filters for row group 3, one for 4");
    assert!(m.lookup(&[key(1, "a")]).unwrap().contains(3));
    assert!(m.lookup(&[key(4999, "b")]).unwrap().contains(3));
    assert!(m.lookup(&[key(1, "c")]).unwrap().contains(4));
}

#[test]
fn nan_probes_are_never_pruned_by_range_bounds() {
    let f = |x: f64| encode_tuple(&[Value::Float(x)]).unwrap();
    let spec = KeySpec::Range { column: "x".into() };
    let mut w = SegmentWriter::new(
        spec,
        WriterOptions {
            range_block_entries: 1,
            ..WriterOptions::default()
        },
    )
    .unwrap();
    // bounds from statistics exclude NaN, though rg 1 may hold NaN
    w.insert_min_max(RowGroupId(0), &f(0.0), &f(1.0)).unwrap();
    w.insert_min_max(RowGroupId(1), &f(5.0), &f(6.0)).unwrap();
    let r = SegmentReader::open(w.finish().unwrap().bytes).unwrap();
    assert_eq!(f(f64::NAN), NAN_KEY.to_vec());
    assert_eq!(r.lookup(&[f(f64::NAN)]).unwrap().len(), 2);
    assert_eq!(r.lookup_range(&f(5.5), &f(f64::NAN)).unwrap().len(), 2);
    // ordinary probes still prune
    assert_eq!(
        r.lookup(&[f(0.5)]).unwrap().iter().collect::<Vec<_>>(),
        vec![0]
    );
    assert!(r.lookup(&[f(3.0)]).unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Second review: bitmaps, bloom merge, add_batch, recorded types, signed zero
// ---------------------------------------------------------------------------

fn tagged_exact() -> (Vec<u8>, Vec<Vec<u8>>) {
    let o = WriterOptions {
        block_keys: 2,
        ..WriterOptions::default()
    };
    let mut w = SegmentWriter::new(exact(), o).unwrap();
    let keys: Vec<Vec<u8>> = (0..10i64)
        .map(|i| key(i, ["a", "\0", "", "a\0"][usize::try_from(i % 4).unwrap()]))
        .collect();
    for (i, k) in keys.iter().enumerate() {
        w.insert(k, RowGroupId(u32::try_from(i % 4).unwrap()))
            .unwrap();
    }
    (w.finish().unwrap().bytes, keys)
}

#[test]
fn a_bitmap_not_in_the_no_run_form_is_refused_not_misread() {
    let (bytes, keys) = tagged_exact();
    let header_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let body = PREAMBLE_LEN + header_len;
    let cookie = [0x3a, 0x30, 0x00, 0x00];
    let at = body
        + bytes[body..]
            .windows(4)
            .position(|w| w == cookie)
            .expect("a stored bitmap");
    // 0x3a -> 0x3b turns the no-run cookie into roaring's run cookie
    let mut b = bytes.clone();
    b[at] = 0x3b;
    let r = SegmentReader::open(b).unwrap();
    assert!(
        r.lookup(&keys).is_err(),
        "a lookup must not silently answer []"
    );
    assert!(r.get(&keys).is_err());
    let merged = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        merge(std::slice::from_ref(&r), &WriterOptions::default()).is_err()
    }));
    assert_eq!(
        merged.ok(),
        Some(true),
        "merge must refuse, never panic or launder"
    );
}

fn bloom_seg(tag: &str, n: i64, rg: u32) -> SegmentReader<Vec<u8>> {
    let mut w = SegmentWriter::new(bloom(0.01), WriterOptions::default()).unwrap();
    for i in 0..n {
        w.insert(&key(i, tag), RowGroupId(rg)).unwrap();
    }
    SegmentReader::open(w.finish().unwrap().bytes).unwrap()
}

#[test]
fn same_size_bloom_filters_of_one_row_group_are_or_ed() {
    let m = SegmentReader::open(
        merge(
            &[bloom_seg("x", 10, 3), bloom_seg("y", 10, 3)],
            &WriterOptions::default(),
        )
        .unwrap()
        .bytes,
    )
    .unwrap();
    assert_eq!(m.block_count(), 1, "same-size filters OR-ed into one entry");
    for i in 0..10 {
        assert!(m.lookup(&[key(i, "x")]).unwrap().contains(3), "x{i}");
        assert!(m.lookup(&[key(i, "y")]).unwrap().contains(3), "y{i}");
    }
}

fn amount_schema(dt: DataType) -> Schema {
    Schema::new(vec![Field::new("amt", dt, true)])
}

fn amounts(scale: i8, values: &[i128]) -> RecordBatch {
    let col: ArrayRef = Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(18, scale)
            .unwrap(),
    );
    RecordBatch::try_from_iter(vec![("amt", col)]).unwrap()
}

fn amount_key(dt: &DataType, cents: i128) -> Vec<u8> {
    encode_tuple_as(
        &[Value::Decimal128 {
            value: cents,
            scale: 2,
        }],
        std::slice::from_ref(dt),
    )
    .unwrap()
}

#[test]
fn add_batch_is_all_or_nothing_and_leaves_the_writer_usable() {
    let dataset = DataType::Decimal128(10, 2);
    let spec = KeySpec::Exact {
        columns: vec!["amt".into()],
    };
    let mut w = SegmentWriter::for_dataset(
        spec,
        &amount_schema(dataset.clone()),
        WriterOptions::default(),
    )
    .unwrap();
    // 1.500, then 1.234 (not exact at scale 2), then 2.000
    assert!(w
        .add_batch(RowGroupId(0), &amounts(3, &[1500, 1234, 2000]))
        .is_err());
    // the writer is not poisoned: a good batch still lands
    w.add_batch(RowGroupId(1), &amounts(3, &[1500])).unwrap();
    let r = SegmentReader::open(w.finish().unwrap().bytes).unwrap();
    let rgs = |cents| r.lookup(&[amount_key(&dataset, cents)]).unwrap();
    assert_eq!(
        rgs(150).iter().collect::<Vec<_>>(),
        vec![1],
        "nothing from rg 0"
    );
    assert!(
        rgs(200).is_empty(),
        "rows after the failing row are not recorded either"
    );
}

#[test]
fn headers_record_key_types_and_differing_types_never_merge() {
    let spec = KeySpec::Exact {
        columns: vec!["amt".into()],
    };
    let build = |dt: DataType, rg: u32| {
        let mut w =
            SegmentWriter::for_dataset(spec.clone(), &amount_schema(dt), WriterOptions::default())
                .unwrap();
        w.add_batch(RowGroupId(rg), &amounts(2, &[150])).unwrap();
        SegmentReader::open(w.finish().unwrap().bytes).unwrap()
    };
    let d102 = ColumnType::Decimal128 {
        precision: 10,
        scale: 2,
    };
    let a = build(DataType::Decimal128(10, 2), 0);
    let b = build(DataType::Decimal128(12, 3), 1);
    assert_eq!(a.header().key_types, Some(vec![d102.clone()]));
    assert_eq!(a.header().ts_type, None);
    // the reviewer's split: 1.50 is a different key under each type
    assert!(matches!(
        merge(&[a, b], &WriterOptions::default()),
        Err(Error::KeySpec(_))
    ));
    // typed and untyped segments do not merge either
    let typed = build(DataType::Decimal128(10, 2), 0);
    let mut raw = SegmentWriter::new(spec.clone(), WriterOptions::default()).unwrap();
    raw.insert(
        &amount_key(&DataType::Decimal128(10, 2), 150),
        RowGroupId(2),
    )
    .unwrap();
    let raw = SegmentReader::open(raw.finish().unwrap().bytes).unwrap();
    assert!(merge(&[typed, raw], &WriterOptions::default()).is_err());
    // equal types merge, and the merged header keeps them
    let m = merge(
        &[
            build(DataType::Decimal128(10, 2), 0),
            build(DataType::Decimal128(10, 2), 1),
        ],
        &WriterOptions::default(),
    )
    .unwrap();
    let m = SegmentReader::open(m.bytes).unwrap();
    assert_eq!(m.header().key_types, Some(vec![d102]));
    let q = amount_key(&DataType::Decimal128(10, 2), 150);
    assert_eq!(m.lookup(&[q]).unwrap().len(), 2);
}

#[test]
fn temporal_headers_record_the_ts_type_and_unit() {
    let s = build(&temporal(), &vec![vec![]], &[], &WriterOptions::default());
    let r = SegmentReader::open(s[0].bytes.clone()).unwrap();
    assert_eq!(
        r.header().key_types,
        Some(vec![ColumnType::Int64, ColumnType::Utf8])
    );
    assert_eq!(
        r.header().ts_type,
        Some(ColumnType::Timestamp {
            unit: bellwether_core::segment::RecordedUnit::Millisecond,
            tz: Some("UTC".into()),
        })
    );
}

#[test]
fn open_for_dataset_refuses_segments_encoded_under_other_types() {
    let spec = KeySpec::Exact {
        columns: vec!["amt".into()],
    };
    let (d102, d123) = (DataType::Decimal128(10, 2), DataType::Decimal128(12, 3));
    let mut w = SegmentWriter::for_dataset(
        spec.clone(),
        &amount_schema(d102.clone()),
        WriterOptions::default(),
    )
    .unwrap();
    w.add_batch(RowGroupId(0), &amounts(2, &[150])).unwrap();
    let bytes = w.finish().unwrap().bytes;
    assert!(SegmentReader::open_for_dataset(bytes.clone(), &amount_schema(d102)).is_ok());
    assert!(matches!(
        SegmentReader::open_for_dataset(bytes, &amount_schema(d123.clone())),
        Err(Error::KeySpec(_))
    ));
    // a segment that records no types cannot be checked, so it is refused
    let mut raw = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
    raw.insert(&amount_key(&d123, 150), RowGroupId(0)).unwrap();
    assert!(
        SegmentReader::open_for_dataset(raw.finish().unwrap().bytes, &amount_schema(d123)).is_err()
    );
}

#[test]
fn signed_zero_range_bounds_are_widened() {
    let f = |x: f64| encode_tuple(&[Value::Float(x)]).unwrap();
    let spec = KeySpec::Range { column: "x".into() };
    let mut w = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
    // footer statistics that did not apply the Parquet zero rule
    w.insert_min_max(RowGroupId(0), &f(0.0), &f(0.0)).unwrap();
    w.insert_min_max(RowGroupId(1), &f(-0.0), &f(-0.0)).unwrap();
    // and bounds the writer computes from values
    w.insert(&f(0.0), RowGroupId(2)).unwrap();
    w.insert_min_max(RowGroupId(3), &f(1.0), &f(2.0)).unwrap();
    let r = SegmentReader::open(w.finish().unwrap().bytes).unwrap();
    let rgs = |b: roaring::RoaringBitmap| b.iter().collect::<Vec<_>>();
    assert_eq!(rgs(r.lookup(&[f(-0.0)]).unwrap()), vec![0, 1, 2]);
    assert_eq!(rgs(r.lookup(&[f(0.0)]).unwrap()), vec![0, 1, 2]);
    assert_eq!(
        rgs(r.lookup_range(&f(-1.0), &f(-0.0)).unwrap()),
        vec![0, 1, 2]
    );
    assert_eq!(rgs(r.lookup(&[f(1.5)]).unwrap()), vec![3]);
}
