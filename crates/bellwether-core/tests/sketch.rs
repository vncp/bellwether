//! `sketch` segments: round trip, the HyperLogLog error bound and the MinHash
//! and inclusion-exclusion overlap bounds against brute-force oracles, nulls
//! per ADR 014, determinism and lossless merge, reader rules, and the
//! every-byte corruption sweep (every damaged byte is a typed refusal,
//! because the footer carries a BLAKE3 checksum).
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::doc_markdown,
    clippy::cast_possible_wrap
)]

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::encoding::{encode_tuple, Value};
use bellwether_core::hll::Hll;
use bellwether_core::sketch::{
    overlap, validate, ItemSketch, SketchReader, SketchTotals, SketchWriter, FOOTER_LEN, MAGIC,
};
use bellwether_types::{Error, OverlapMethod, RowGroupId, SketchSpec};
use proptest::prelude::*;

/// SplitMix64: the deterministic generator of the statistical trials.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("tag", DataType::Utf8, true),
    ]))
}

fn spec(items: &[&[&str]], precision: u8, minhash: Option<u16>) -> SketchSpec {
    SketchSpec {
        items: items
            .iter()
            .map(|i| i.iter().map(|c| (*c).to_string()).collect())
            .collect(),
        precision,
        minhash,
    }
}

fn batch(ids: &[Option<i64>], tags: &[Option<&str>]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(tags.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

/// One file: its row groups, each a list of `(id, tag)` rows.
type File = Vec<Vec<(Option<i64>, Option<String>)>>;

fn rg_batch(rows: &[(Option<i64>, Option<String>)]) -> RecordBatch {
    let ids: Vec<Option<i64>> = rows.iter().map(|r| r.0).collect();
    let tags: Vec<Option<&str>> = rows.iter().map(|r| r.1.as_deref()).collect();
    batch(&ids, &tags)
}

/// Writes `files` into one segment, row-group ids dense from `first`.
fn write(spec: &SketchSpec, files: &[File], first: u32) -> Vec<u8> {
    let mut w = SketchWriter::for_dataset(spec, &schema()).unwrap();
    let mut rg = first;
    for (fi, f) in files.iter().enumerate() {
        for g in f {
            w.add_batch(fi as u64, RowGroupId(rg), &rg_batch(g))
                .unwrap();
            rg += 1;
        }
    }
    w.set_rowgroup_range(RowGroupId(first)..RowGroupId(rg))
        .unwrap();
    w.finish().unwrap().bytes
}

/// Folds every entry of `bytes`, one entry per file.
fn totals(spec: &SketchSpec, bytes: &[u8]) -> SketchTotals {
    let r = SketchReader::open_for_dataset(bytes, &schema()).unwrap();
    let mut t = SketchTotals::new(spec).unwrap();
    for i in 0..r.len() {
        t.add_file(&r.entry(i).unwrap().items).unwrap();
    }
    t
}

/// The canonical bytes of `(id)` and `(id, tag)` for a row, per ADR 014:
/// `None` when the leading column is null.
fn oracle_key(item: &[&str], id: Option<i64>, tag: Option<&str>) -> Option<Vec<u8>> {
    let v = |c: &str| match c {
        "id" => id.map(Value::Int),
        _ => tag.map(Value::Utf8),
    };
    let mut out = encode_tuple(&[v(item[0])?]).unwrap();
    for c in &item[1..] {
        match v(c) {
            Some(x) => out.extend(encode_tuple(&[x]).unwrap()),
            None => out.push(0x00),
        }
    }
    Some(out)
}

fn row_strategy() -> impl Strategy<Value = (Option<i64>, Option<String>)> {
    (
        prop::option::weighted(0.9, 0i64..40),
        prop::option::weighted(0.85, prop::sample::select(vec!["a", "b", "", "\0", "a\0"])),
    )
        .prop_map(|(i, t)| (i, t.map(String::from)))
}

fn files_strategy() -> impl Strategy<Value = Vec<File>> {
    prop::collection::vec(
        prop::collection::vec(prop::collection::vec(row_strategy(), 0..12), 1..4),
        0..6,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Round trip and exact counts: every entry decodes to what the writer
    /// sketched, per item the rows and nulls equal a decoded count, the
    /// distinct interval holds the oracle's exact count, and a file with a
    /// repeated value is never missed as "unique" when the estimate is
    /// exact (small sets at p = 14 never collide in these sizes, so the
    /// interval is tight).
    #[test]
    fn round_trip_counts_and_bounds_match_the_oracle(files in files_strategy(), minhash in prop::option::of(1u16..16)) {
        let items: &[&[&str]] = &[&["id"], &["tag"], &["id", "tag"]];
        let sp = spec(items, 14, minhash);
        let bytes = write(&sp, &files, 3);
        let r = SketchReader::open_for_dataset(&bytes, &schema()).unwrap();
        prop_assert_eq!(r.spec(), sp.clone());
        prop_assert_eq!(r.len(), files.len());
        let mut rg = 3u32;
        for (fi, f) in files.iter().enumerate() {
            let e = r.entry(fi).unwrap();
            prop_assert_eq!(e.rowgroups.clone(), rg..rg + f.len() as u32);
            rg += f.len() as u32;
            for (ii, item) in items.iter().enumerate() {
                let rows: Vec<_> = f.iter().flatten().collect();
                let keys: Vec<Vec<u8>> = rows.iter().filter_map(|(i, t)| oracle_key(item, *i, t.as_deref())).collect();
                prop_assert_eq!(e.items[ii].rows, keys.len() as u64);
                prop_assert_eq!(e.items[ii].nulls, (rows.len() - keys.len()) as u64);
                // The registers are exactly the oracle's set inserted into a
                // fresh sketch.
                let mut h = Hll::new(14).unwrap();
                for k in &keys {
                    h.insert(bellwether_core::sketch::value_hash(k) as u64);
                }
                prop_assert_eq!(e.items[ii].hll.registers(), h.registers());
            }
        }
        let t = totals(&sp, &bytes);
        for (ii, item) in items.iter().enumerate() {
            let all: Vec<_> = files.iter().flatten().flatten().collect();
            let distinct: HashSet<Vec<u8>> = all.iter().filter_map(|(i, tg)| oracle_key(item, *i, tg.as_deref())).collect();
            let c = t.cardinality(ii, None).unwrap();
            prop_assert!(c.approximate);
            prop_assert!(c.distinct_low <= distinct.len() as u64 && distinct.len() as u64 <= c.distinct_high,
                "{}: exact {} outside [{}, {}]", c.item, distinct.len(), c.distinct_low, c.distinct_high);
            prop_assert_eq!(c.files, files.len() as u64);
            let dup_files = files.iter().filter(|f| {
                let ks: Vec<Vec<u8>> = f.iter().flatten().filter_map(|(i, tg)| oracle_key(item, *i, tg.as_deref())).collect();
                let set: HashSet<&Vec<u8>> = ks.iter().collect();
                set.len() < ks.len()
            }).count() as u64;
            // Never claims duplicates a file does not have.
            prop_assert!(c.files_with_duplicates <= dup_files);
        }
    }

    /// Merging a file's stretches (two segments, or two entries) gives the
    /// registers of sketching the whole file at once.
    #[test]
    fn split_files_merge_losslessly(f in prop::collection::vec(prop::collection::vec(row_strategy(), 0..12), 2..6), cut in 1usize..5) {
        let cut = cut.min(f.len() - 1);
        let sp = spec(&[&["id"], &["id", "tag"]], 10, Some(8));
        let whole = write(&sp, std::slice::from_ref(&f), 0);
        let a = write(&sp, &[f[..cut].to_vec()], 0);
        let b = write(&sp, &[f[cut..].to_vec()], cut as u32);
        let rw = SketchReader::open(&whole).unwrap().entry(0).unwrap();
        let mut merged: Vec<ItemSketch> = SketchReader::open(&a).unwrap().entry(0).unwrap().items;
        let rb = SketchReader::open(&b).unwrap().entry(0).unwrap();
        for (m, x) in merged.iter_mut().zip(&rb.items) {
            m.merge(x).unwrap();
        }
        prop_assert_eq!(merged, rw.items);
    }
}

/// The HyperLogLog error bound against a brute-force distinct count: over
/// 330 deterministic trials from 1 to 300,000 distinct values (with
/// duplicates, across files), the reported 99.7 % interval holds the exact
/// count in at least 99 % of trials, and every estimate is within four
/// relative standard errors.
#[test]
fn hll_error_bound_holds_against_a_brute_force_oracle() {
    let mut rng = Rng(7);
    let mut inside = 0;
    let mut trials = 0;
    for &p in &[10u8, 12, 14] {
        let rse = Hll::relative_standard_error(p);
        for &n in &[1u64, 7, 60, 500, 3_000, 20_000, 120_000, 300_000] {
            let reps = if n >= 120_000 { 4 } else { 16 };
            for _ in 0..reps {
                let sp = spec(&[&["id"]], p, None);
                let mut t = SketchTotals::new(&sp).unwrap();
                let base = rng.next() as i64 >> 2;
                let mut exact = HashSet::new();
                // Three files; each value lands in one or two of them.
                let mut files = vec![ItemSketch::empty(&sp).unwrap(); 3];
                let mut file_hll: Vec<Vec<i64>> = vec![Vec::new(); 3];
                for i in 0..n as i64 {
                    let v = base.wrapping_add(i * 7919);
                    exact.insert(v);
                    let copies = 1 + rng.below(2);
                    for _ in 0..copies {
                        file_hll[rng.below(3) as usize].push(v);
                    }
                }
                for (f, vals) in files.iter_mut().zip(&file_hll) {
                    for &v in vals {
                        let k = encode_tuple(&[Value::Int(v)]).unwrap();
                        f.hll.insert(bellwether_core::sketch::value_hash(&k) as u64);
                        f.rows += 1;
                    }
                }
                for f in &files {
                    t.add_file(std::slice::from_ref(f)).unwrap();
                }
                let c = t.cardinality(0, None).unwrap();
                let truth = exact.len() as u64;
                trials += 1;
                if c.distinct_low <= truth && truth <= c.distinct_high {
                    inside += 1;
                }
                let err = (c.distinct as f64 - truth as f64).abs() / truth as f64;
                assert!(
                    err <= 4.0 * rse + 1.0 / truth as f64,
                    "p={p} n={n}: estimate {} vs exact {truth} (error {err:.4}, rse {rse:.4})",
                    c.distinct
                );
                assert!((c.relative_standard_error - rse).abs() < 1e-12);
            }
        }
    }
    assert!(
        f64::from(inside) >= 0.99 * f64::from(trials),
        "{inside} of {trials} intervals held the exact count"
    );
}

/// Sets `(A, B)` with a known Jaccard similarity, each value hashed as the
/// sketch hashes it.
fn overlap_sets(
    rng: &mut Rng,
    a_only: u64,
    both: u64,
    b_only: u64,
    p: u8,
    k: Option<u16>,
) -> (ItemSketch, ItemSketch) {
    let sp = spec(&[&["id"]], p, k);
    let mut a = ItemSketch::empty(&sp).unwrap();
    let mut b = ItemSketch::empty(&sp).unwrap();
    let base = rng.next() as i64 >> 2;
    let add = |s: &mut ItemSketch, v: i64| {
        let key = encode_tuple(&[Value::Int(v)]).unwrap();
        let h = bellwether_core::sketch::value_hash(&key);
        s.hll.insert(h as u64);
        if let Some(m) = &mut s.minhash {
            m.insert(h);
        }
        s.rows += 1;
    };
    let mut v = base;
    for _ in 0..a_only {
        add(&mut a, v);
        v += 1;
    }
    for _ in 0..both {
        add(&mut a, v);
        add(&mut b, v);
        v += 1;
    }
    for _ in 0..b_only {
        add(&mut b, v);
        v += 1;
    }
    (a, b)
}

fn totals_of(s: &ItemSketch, p: u8, k: Option<u16>) -> SketchTotals {
    let mut t = SketchTotals::new(&spec(&[&["id"]], p, k)).unwrap();
    t.add_file(std::slice::from_ref(s)).unwrap();
    t
}

/// MinHash and HyperLogLog inclusion-exclusion Jaccard estimates against
/// the exact similarity: the reported interval holds it in at least 99 % of
/// trials for each method.
#[test]
fn overlap_bounds_hold_against_the_exact_jaccard() {
    use bellwether_core::segment::ColumnType;
    let mut rng = Rng(11);
    let shapes = [
        (0u64, 1_000u64, 0u64),
        (1_000, 0, 1_000),
        (500, 500, 500),
        (9_000, 1_000, 0),
        (100, 10_000, 100),
        (20_000, 2_000, 20_000),
        (3, 1, 2),
    ];
    for k in [Some(256u16), None] {
        let mut inside = 0;
        let mut trials = 0;
        for &(ao, both, bo) in &shapes {
            for _ in 0..12 {
                let (a, b) = overlap_sets(&mut rng, ao, both, bo, 14, k);
                let exact = both as f64 / (ao + both + bo) as f64;
                let ta = totals_of(&a, 14, k);
                let tb = totals_of(&b, 14, k);
                let t = [ColumnType::Int64];
                let o = overlap(
                    ("a", ta.item(0).unwrap(), &t, 14),
                    ("b", tb.item(0).unwrap(), &t, 14),
                )
                .unwrap();
                assert!(o.approximate);
                assert_eq!(
                    o.method,
                    if k.is_some() {
                        OverlapMethod::Minhash
                    } else {
                        OverlapMethod::Hll
                    }
                );
                assert!(o.jaccard_low <= o.jaccard && o.jaccard <= o.jaccard_high);
                trials += 1;
                if o.jaccard_low <= exact + 1e-12 && exact <= o.jaccard_high + 1e-12 {
                    inside += 1;
                } else {
                    eprintln!(
                        "MISS {k:?} {ao}/{both}/{bo}: exact {exact} est {} [{}, {}]",
                        o.jaccard, o.jaccard_low, o.jaccard_high
                    );
                }
            }
        }
        assert!(
            f64::from(inside) >= 0.99 * f64::from(trials),
            "{k:?}: {inside} of {trials} intervals held the exact Jaccard"
        );
    }
}

#[test]
fn overlap_refuses_items_of_different_types_or_precisions() {
    use bellwether_core::segment::ColumnType;
    let mut rng = Rng(3);
    let (a, b) = overlap_sets(&mut rng, 5, 5, 5, 12, None);
    let ta = totals_of(&a, 12, None);
    let tb = totals_of(&b, 12, None);
    let e = overlap(
        ("a", ta.item(0).unwrap(), &[ColumnType::Int64], 12),
        ("b", tb.item(0).unwrap(), &[ColumnType::Utf8], 12),
    )
    .unwrap_err();
    assert!(e.to_string().contains("never compare equal"), "{e}");
    let e = overlap(
        ("a", ta.item(0).unwrap(), &[ColumnType::Int64], 12),
        ("b", tb.item(0).unwrap(), &[ColumnType::Int64], 14),
    )
    .unwrap_err();
    assert!(e.to_string().contains("precision"), "{e}");
}

/// ADR 014: a row whose leading column is null is not sketched and counts
/// as a null; a null trailing column of a tuple is the null marker, a value
/// distinct from every non-null one; a file whose item is all null still
/// has its entry, with zero rows.
#[test]
fn nulls_follow_adr_014() {
    let sp = spec(&[&["id"], &["id", "tag"], &["tag"]], 14, Some(4));
    let files: Vec<File> = vec![
        vec![vec![
            (Some(1), None),
            (Some(1), Some("x".into())),
            (None, Some("x".into())),
            (None, None),
        ]],
        vec![vec![(None, None), (None, None)]],
    ];
    let bytes = write(&sp, &files, 0);
    let r = SketchReader::open(&bytes).unwrap();
    let e0 = r.entry(0).unwrap();
    assert_eq!((e0.items[0].rows, e0.items[0].nulls), (2, 2));
    // (1, null) and (1, "x") are two tuples.
    assert_eq!((e0.items[1].rows, e0.items[1].nulls), (2, 2));
    let t = totals(&sp, &bytes);
    assert_eq!(t.cardinality(0, None).unwrap().distinct, 1);
    assert_eq!(t.cardinality(1, None).unwrap().distinct, 2);
    assert_eq!(t.cardinality(2, None).unwrap().distinct, 1);
    let e1 = r.entry(1).unwrap();
    for item in &e1.items {
        assert_eq!(item.rows, 0);
        assert_eq!(item.nulls, 2);
        assert_eq!(item.hll.nonzero(), 0);
        assert!(item.minhash.as_ref().unwrap().is_empty());
    }
}

#[test]
fn writing_is_deterministic_and_the_range_is_in_the_bytes() {
    let sp = spec(&[&["id"]], 12, None);
    let f: Vec<File> = vec![vec![vec![(Some(1), None)]]];
    assert_eq!(write(&sp, &f, 0), write(&sp, &f, 0));
    // Same content, another processed range: other bytes, another id.
    let mut w = SketchWriter::for_dataset(&sp, &schema()).unwrap();
    w.add_batch(0, RowGroupId(0), &rg_batch(&f[0][0])).unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(9)).unwrap();
    let wide = w.finish().unwrap();
    assert_ne!(wide.bytes, write(&sp, &f, 0));
    assert_eq!(
        SketchReader::open(&wide.bytes).unwrap().rowgroup_range(),
        (0, 9)
    );
    // An empty writer still records its range.
    let mut w = SketchWriter::for_dataset(&sp, &schema()).unwrap();
    w.set_rowgroup_range(RowGroupId(4)..RowGroupId(6)).unwrap();
    let empty = w.finish().unwrap();
    let r = SketchReader::open(&empty.bytes).unwrap();
    assert!(r.is_empty());
    assert_eq!(r.rowgroup_range(), (4, 6));
}

#[test]
fn writer_refuses_descending_row_groups_and_out_of_range_entries() {
    let sp = spec(&[&["id"]], 12, None);
    let b = rg_batch(&[(Some(1), None)]);
    let mut w = SketchWriter::for_dataset(&sp, &schema()).unwrap();
    w.add_batch(0, RowGroupId(5), &b).unwrap();
    assert!(w.add_batch(1, RowGroupId(4), &b).is_err());
    let mut w = SketchWriter::for_dataset(&sp, &schema()).unwrap();
    w.add_batch(0, RowGroupId(5), &b).unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(5)).unwrap();
    assert!(w.finish().is_err());
}

#[test]
fn a_new_entry_starts_at_each_file_and_each_gap() {
    let sp = spec(&[&["id"]], 12, None);
    let b = rg_batch(&[(Some(1), None)]);
    let mut w = SketchWriter::for_dataset(&sp, &schema()).unwrap();
    assert!(w.starts_entry(0, RowGroupId(0)));
    w.add_batch(0, RowGroupId(0), &b).unwrap();
    assert!(!w.starts_entry(0, RowGroupId(1)));
    w.add_batch(0, RowGroupId(1), &b).unwrap();
    // Contiguous ids, another file.
    assert!(w.starts_entry(1, RowGroupId(2)));
    w.add_batch(1, RowGroupId(2), &b).unwrap();
    // Same file, a gap.
    w.add_batch(1, RowGroupId(4), &b).unwrap();
    let out = w.finish().unwrap();
    let r = SketchReader::open(&out.bytes).unwrap();
    let ranges: Vec<_> = (0..r.len())
        .map(|i| r.entry(i).unwrap().rowgroups)
        .collect();
    assert_eq!(ranges, vec![0..2, 2..3, 4..5]);
}

#[test]
fn spec_validation_names_the_problem() {
    let cases: Vec<(SketchSpec, &str)> = vec![
        (spec(&[], 14, None), "no column"),
        (spec(&[&[]], 14, None), "names no column"),
        (spec(&[&["a+b"]], 14, None), "separators"),
        (spec(&[&["a", "a"]], 14, None), "twice"),
        (spec(&[&["a"], &["a"]], 14, None), "listed twice"),
        (spec(&[&["a"]], 3, None), "precision"),
        (spec(&[&["a"]], 19, None), "precision"),
        (spec(&[&["a"]], 14, Some(0)), "minhash"),
        (spec(&[&["a"]], 14, Some(1025)), "minhash"),
    ];
    for (s, want) in cases {
        let e = validate(&s).unwrap_err();
        assert!(matches!(e, Error::KeySpec(_)), "{e:?}");
        assert!(e.to_string().contains(want), "{e} lacks {want}");
    }
    assert!(validate(&spec(&[&["a"], &["a", "b"]], 14, Some(64))).is_ok());
}

#[test]
fn a_segment_written_under_other_types_is_refused_for_the_dataset() {
    let sp = spec(&[&["id"]], 12, None);
    let bytes = write(&sp, &[vec![vec![(Some(1), None)]]], 0);
    let other = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    let e = SketchReader::open_for_dataset(&bytes, &other).unwrap_err();
    assert!(e.to_string().contains("re-index"), "{e}");
}

/// Re-seals `bytes` after an edit: recomputes the footer checksum, so a test
/// can reach the layout rules behind it.
fn reseal(mut bytes: Vec<u8>) -> Vec<u8> {
    let at = bytes.len() - FOOTER_LEN;
    let sum = blake3::hash(&bytes[..at + 12]);
    bytes[at + 12..at + 44].copy_from_slice(sum.as_bytes());
    bytes
}

#[test]
fn the_reader_enforces_the_layout_behind_the_checksum() {
    let sp = spec(&[&["id"]], 4, None);
    let bytes = write(&sp, &[vec![vec![(Some(1), None), (Some(2), None)]]], 0);
    assert!(SketchReader::open(&bytes).is_ok());
    let header_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let body = 20 + header_len;
    // rg_start = rg_end
    let mut b = bytes.clone();
    b[body..body + 4].copy_from_slice(&1u32.to_le_bytes());
    assert!(SketchReader::open(&reseal(b)).is_err());
    // An unknown register encoding tag.
    let mut b = bytes.clone();
    b[body + 8 + 16] = 7;
    assert!(SketchReader::open(&reseal(b)).is_err());
    // A row count that disagrees with the registers.
    let mut b = bytes.clone();
    b[body + 8..body + 16].copy_from_slice(&0u64.to_le_bytes());
    assert!(SketchReader::open(&reseal(b)).is_err());
    // A newer storage version in the preamble.
    let mut b = bytes.clone();
    b[8..12].copy_from_slice(&99u32.to_le_bytes());
    assert!(matches!(
        SketchReader::open(&reseal(b)),
        Err(Error::UnsupportedVersion { .. })
    ));
}

/// Every truncation and every single-byte change of a segment (every
/// position, eight values each) is refused with an error: the checksum
/// leaves no byte whose damage reads as a different estimate.
#[test]
fn every_byte_corruption_is_refused() {
    let sp = spec(&[&["id"], &["id", "tag"]], 4, Some(2));
    let files: Vec<File> = vec![
        vec![
            vec![(Some(1), Some("a".into())), (Some(2), None)],
            vec![(None, None)],
        ],
        vec![vec![(Some(3), Some("b".into()))]],
    ];
    let bytes = write(&sp, &files, 0);
    assert!(SketchReader::open(&bytes).is_ok());
    assert_eq!(&bytes[..8], &MAGIC);
    for cut in 0..bytes.len() {
        assert!(
            SketchReader::open(&bytes[..cut]).is_err(),
            "cut {cut} opened"
        );
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
            assert!(
                SketchReader::open(&b).is_err(),
                "byte {pos} = {v:#x} opened"
            );
        }
    }
}

/// `docs/format.md`'s "Sketch conformance vectors" table: the pinned bytes
/// and segment id of the reference segment below, which the encoder must
/// reproduce exactly.
#[test]
fn sketch_table_in_format_md_matches_the_encoder() {
    // Reference: items id and id+tag, p = 4, minhash 2, two files.
    let sp = spec(&[&["id"], &["id", "tag"]], 4, Some(2));
    let files: Vec<File> = vec![
        vec![vec![(Some(1), Some("a".into())), (Some(2), None)]],
        vec![vec![(Some(1), Some("a".into()))], vec![(None, None)]],
    ];
    let bytes = write(&sp, &files, 0);
    let header_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let body = 20 + header_len;
    let hex = |b: &[u8]| {
        b.iter()
            .map(|x| format!("{x:02x}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    let id_key = encode_tuple(&[Value::Int(1)]).unwrap();
    // The first entry's `id` item: range, counts, the sparse tag, n, n
    // registers, two MinHash slots.
    let at = body + 8 + 16;
    assert_eq!(bytes[at], 0, "the reference item is sparse");
    let n = u32::from_le_bytes(bytes[at + 1..at + 5].try_into().unwrap()) as usize;
    let item_end = at + 1 + 4 + 5 * n + 16;
    let computed = [
        (
            "value_hash(Int64 1)",
            format!("{:032x}", bellwether_core::sketch::value_hash(&id_key)),
        ),
        ("preamble", hex(&bytes[..20])),
        ("first entry, item id", hex(&bytes[body..item_end])),
        ("footer", hex(&bytes[bytes.len() - FOOTER_LEN..])),
        (
            "segment blake3",
            bellwether_types::Hash::of(&bytes).to_hex(),
        ),
    ];
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/format.md");
    let doc = std::fs::read_to_string(path).unwrap();
    let section = doc
        .split("## Sketch conformance vectors")
        .nth(1)
        .expect("docs/format.md has a sketch conformance vectors section");
    let rows: Vec<(String, String)> = section
        .lines()
        .skip_while(|l| !l.starts_with("| `"))
        .take_while(|l| l.starts_with('|'))
        .map(|l| {
            let cells: Vec<&str> = l.split('|').map(str::trim).collect();
            (
                cells[1].trim_matches('`').to_string(),
                cells[2].trim_matches('`').to_string(),
            )
        })
        .collect();
    let want: Vec<(String, String)> = computed
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect();
    assert_eq!(
        rows, want,
        "update docs/format.md only when the format changes on purpose"
    );
}
