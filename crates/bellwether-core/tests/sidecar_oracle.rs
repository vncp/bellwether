//! Oracle tests for `bounds` and `temporal2` sidecar segments and
//! `bitemporal_winner` against independently computed ground truth.
//!
//! Covers:
//! - Random batches over several row groups (`proptest`), including keys whose
//!   rows straddle row groups, null leading keys (skipped), null trailing keys
//!   (encoded with the `0x00` marker; probe tuples containing a null match
//!   nothing), and null timestamps (excluded from `bounds` and `temporal2`
//!   records).
//! - Utf8, Int64, and composite `(Utf8, Int64)` keys.
//! - `Timestamp(µs, UTC)` and `Date32` time columns.
//! - Sentinel timestamps: `i64::MIN + 1`, `i64::MAX`, year `9999-12-31` in µs
//!   (`253_402_300_799_999_999`), and negative pre-1970 values.
//! - `bitemporal_winner` against a brute-force oracle over restatements (same
//!   `valid` with several `known` values), late-arriving rows (small `known`
//!   with large `valid` and vice versa), ties across row groups, and split
//!   segments.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::too_many_lines
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether_core::encoding::{encode_tuple, tag, Value};
use bellwether_core::sidecar::{
    bitemporal_winner, BitemporalHit, RgBounds, SidecarDef, SidecarKind, SidecarReader,
    SidecarWriter, Triple,
};
use bellwether_types::RowGroupId;
use proptest::prelude::*;
use roaring::RoaringBitmap;

const YEAR_9999_END_US: i64 = 253_402_300_799_999_999;

const SENTINELS: [i64; 8] = [
    i64::MIN + 1,
    -86_400_000_000, // 1969-12-31T00:00:00Z in µs
    -1,
    0,
    1,
    86_400_000_000, // 1970-01-02T00:00:00Z in µs
    YEAR_9999_END_US,
    i64::MAX,
];

const SYMBOLS: [&str; 6] = ["AAPL", "MSFT", "GOOG", "NVDA", "", "A\0B"];

#[derive(Clone, Debug)]
struct OracleRow {
    sym: Option<&'static str>,
    sub: Option<i64>,
    valid_us: Option<i64>,
    known_us: Option<i64>,
    valid_d32: Option<i32>,
    known_d32: Option<i32>,
}

fn ts_choice() -> impl Strategy<Value = Option<i64>> {
    prop_oneof![
        2 => Just(None),
        5 => prop::sample::select(&SENTINELS[..]).prop_map(Some),
        5 => (-1_000i64..1_000).prop_map(Some),
    ]
}

fn d32_choice() -> impl Strategy<Value = Option<i32>> {
    prop_oneof![
        2 => Just(None),
        5 => prop::sample::select(&[i32::MIN + 1, -365, -1, 0, 1, 365, 2_932_896, i32::MAX][..]).prop_map(Some),
        5 => (-50i32..50).prop_map(Some),
    ]
}

fn row_strategy() -> impl Strategy<Value = OracleRow> {
    (
        prop_oneof![
            2 => Just(None),
            8 => prop::sample::select(&SYMBOLS[..]).prop_map(Some),
        ],
        prop_oneof![
            2 => Just(None),
            8 => (-3i64..4).prop_map(Some),
        ],
        ts_choice(),
        ts_choice(),
        d32_choice(),
        d32_choice(),
    )
        .prop_map(
            |(sym, sub, valid_us, known_us, valid_d32, known_d32)| OracleRow {
                sym,
                sub,
                valid_us,
                known_us,
                valid_d32,
                known_d32,
            },
        )
}

fn layout_strategy() -> impl Strategy<Value = Vec<Vec<OracleRow>>> {
    prop::collection::vec(prop::collection::vec(row_strategy(), 0..20), 1..10)
}

fn dataset_schema() -> Schema {
    Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("sub", DataType::Int64, true),
        Field::new(
            "valid_us",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "known_us",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("valid_d32", DataType::Date32, true),
        Field::new("known_d32", DataType::Date32, true),
    ])
}

fn to_batch(rows: &[OracleRow]) -> RecordBatch {
    let sym: ArrayRef = Arc::new(rows.iter().map(|r| r.sym).collect::<StringArray>());
    let sub: ArrayRef = Arc::new(rows.iter().map(|r| r.sub).collect::<Int64Array>());
    let valid_us: ArrayRef = Arc::new(
        rows.iter()
            .map(|r| r.valid_us)
            .collect::<TimestampMicrosecondArray>()
            .with_timezone("UTC"),
    );
    let known_us: ArrayRef = Arc::new(
        rows.iter()
            .map(|r| r.known_us)
            .collect::<TimestampMicrosecondArray>()
            .with_timezone("UTC"),
    );
    let valid_d32: ArrayRef = Arc::new(rows.iter().map(|r| r.valid_d32).collect::<Date32Array>());
    let known_d32: ArrayRef = Arc::new(rows.iter().map(|r| r.known_d32).collect::<Date32Array>());
    RecordBatch::try_from_iter(vec![
        ("sym", sym),
        ("sub", sub),
        ("valid_us", valid_us),
        ("known_us", known_us),
        ("valid_d32", valid_d32),
        ("known_d32", known_d32),
    ])
    .unwrap()
}

fn encode_sym_key(sym: &str) -> Vec<u8> {
    encode_tuple(&[Value::Utf8(sym)]).unwrap()
}

fn encode_sub_key(sub: i64) -> Vec<u8> {
    encode_tuple(&[Value::Int(sub)]).unwrap()
}

fn encode_comp_key(sym: &str, sub: Option<i64>) -> Vec<u8> {
    if let Some(i) = sub {
        encode_tuple(&[Value::Utf8(sym), Value::Int(i)]).unwrap()
    } else {
        let mut k = encode_tuple(&[Value::Utf8(sym)]).unwrap();
        k.push(tag::NULL);
        k
    }
}

/// Independent ground-truth calculation of per-key, per-row-group `[min, max]` bounds.
fn oracle_bounds(
    layout: &[Vec<OracleRow>],
    extract_key: impl Fn(&OracleRow) -> Option<Vec<u8>>,
    extract_ts: impl Fn(&OracleRow) -> Option<i64>,
) -> BTreeMap<Vec<u8>, Vec<RgBounds>> {
    let mut map: BTreeMap<Vec<u8>, BTreeMap<u32, (i64, i64)>> = BTreeMap::new();
    for (rg_idx, rows) in layout.iter().enumerate() {
        let rg = u32::try_from(rg_idx).unwrap();
        for r in rows {
            let (Some(k), Some(t)) = (extract_key(r), extract_ts(r)) else {
                continue;
            };
            map.entry(k)
                .or_default()
                .entry(rg)
                .and_modify(|b| {
                    b.0 = b.0.min(t);
                    b.1 = b.1.max(t);
                })
                .or_insert((t, t));
        }
    }
    map.into_iter()
        .map(|(k, rgs)| {
            let v = rgs
                .into_iter()
                .map(|(rg, (min, max))| RgBounds {
                    rg: RowGroupId(rg),
                    min,
                    max,
                })
                .collect();
            (k, v)
        })
        .collect()
}

/// Independent ground-truth calculation of per-key sorted `(valid, known, rg)` triples.
fn oracle_triples(
    layout: &[Vec<OracleRow>],
    extract_key: impl Fn(&OracleRow) -> Option<Vec<u8>>,
    extract_times: impl Fn(&OracleRow) -> Option<(i64, i64)>,
) -> BTreeMap<Vec<u8>, Vec<Triple>> {
    let mut map: BTreeMap<Vec<u8>, BTreeSet<Triple>> = BTreeMap::new();
    for (rg_idx, rows) in layout.iter().enumerate() {
        let rg = RowGroupId(u32::try_from(rg_idx).unwrap());
        for r in rows {
            let (Some(k), Some((valid, known))) = (extract_key(r), extract_times(r)) else {
                continue;
            };
            map.entry(k)
                .or_default()
                .insert(Triple { valid, known, rg });
        }
    }
    map.into_iter()
        .map(|(k, set)| (k, set.into_iter().collect()))
        .collect()
}

/// Independent brute-force bitemporal winner directly from raw rows.
fn oracle_winner_from_rows(
    layout: &[Vec<OracleRow>],
    target_key: &[u8],
    extract_key: impl Fn(&OracleRow) -> Option<Vec<u8>>,
    extract_times: impl Fn(&OracleRow) -> Option<(i64, i64)>,
    valid_at: Option<i64>,
    known_at: Option<i64>,
) -> Option<BitemporalHit> {
    let mut qualifying: Vec<(i64, i64, u32)> = Vec::new();
    for (rg_idx, rows) in layout.iter().enumerate() {
        let rg = u32::try_from(rg_idx).unwrap();
        for r in rows {
            if extract_key(r).as_deref() != Some(target_key) {
                continue;
            }
            let Some((valid, known)) = extract_times(r) else {
                continue;
            };
            if valid_at.is_some_and(|v| valid > v) || known_at.is_some_and(|k| known > k) {
                continue;
            }
            qualifying.push((valid, known, rg));
        }
    }
    let max_valid = qualifying.iter().map(|q| q.0).max()?;
    let max_known = qualifying
        .iter()
        .filter(|q| q.0 == max_valid)
        .map(|q| q.1)
        .max()?;
    let row_groups: RoaringBitmap = qualifying
        .into_iter()
        .filter(|q| q.0 == max_valid && q.1 == max_known)
        .map(|q| q.2)
        .collect();
    Some(BitemporalHit {
        valid: max_valid,
        known: max_known,
        row_groups,
    })
}

#[test]
fn sentinel_timestamps_and_bitemporal_restatements_late_rows_and_ties() {
    let schema = dataset_schema();
    // Scenario:
    // - AAPL has restatements at valid = 100 (known = 10 in rg 0, known = 20 in rg 1, known = 20 in rg 2 -> tie across rg 1 and rg 2!)
    // - AAPL also has a late-arriving row: valid = 50, known = 30 in rg 2 (should NOT beat valid = 100 when valid_at >= 100 and known_at >= 30, because valid is primary!)
    // - AAPL also has a future valid row: valid = YEAR_9999_END_US, known = 5 in rg 0, and sentinel extremes i64::MIN + 1, i64::MAX
    // - AAPL has rows with null valid or null known (must never win)
    let layout = [
        vec![
            OracleRow {
                sym: Some("AAPL"),
                sub: Some(1),
                valid_us: Some(100),
                known_us: Some(10),
                valid_d32: Some(-10),
                known_d32: Some(1),
            },
            OracleRow {
                sym: Some("AAPL"),
                sub: Some(1),
                valid_us: Some(YEAR_9999_END_US),
                known_us: Some(5),
                valid_d32: Some(i32::MAX),
                known_d32: Some(2),
            },
            OracleRow {
                sym: Some("AAPL"),
                sub: Some(1),
                valid_us: Some(i64::MIN + 1),
                known_us: Some(i64::MIN + 1),
                valid_d32: Some(i32::MIN + 1),
                known_d32: Some(i32::MIN + 1),
            },
            OracleRow {
                sym: Some("AAPL"),
                sub: Some(1),
                valid_us: Some(999_999),
                known_us: None,
                valid_d32: Some(999),
                known_d32: None,
            },
        ],
        vec![
            OracleRow {
                sym: Some("AAPL"),
                sub: Some(1),
                valid_us: Some(100),
                known_us: Some(20),
                valid_d32: Some(-10),
                known_d32: Some(5),
            },
            OracleRow {
                sym: Some("AAPL"),
                sub: Some(1),
                valid_us: None,
                known_us: Some(999_999),
                valid_d32: None,
                known_d32: Some(999),
            },
        ],
        vec![
            OracleRow {
                sym: Some("AAPL"),
                sub: Some(1),
                valid_us: Some(100),
                known_us: Some(20),
                valid_d32: Some(-10),
                known_d32: Some(5),
            },
            OracleRow {
                sym: Some("AAPL"),
                sub: Some(1),
                valid_us: Some(50),
                known_us: Some(30),
                valid_d32: Some(-20),
                known_d32: Some(10),
            },
            OracleRow {
                sym: Some("AAPL"),
                sub: Some(1),
                valid_us: Some(i64::MAX),
                known_us: Some(i64::MAX),
                valid_d32: Some(100),
                known_d32: Some(100),
            },
        ],
    ];

    // Split across two segments: rg 0..2 and rg 2..3
    let def_t2 = SidecarDef {
        kind: SidecarKind::Bitemporal,
        columns: vec!["sym".into()],
        time_columns: vec!["valid_us".into(), "known_us".into()],
    };
    let mut w_seg1 = SidecarWriter::for_dataset(def_t2.clone(), &schema).unwrap();
    w_seg1
        .set_rowgroup_range(RowGroupId(0)..RowGroupId(2))
        .unwrap();
    w_seg1
        .add_batch(RowGroupId(0), &to_batch(&layout[0]))
        .unwrap();
    w_seg1
        .add_batch(RowGroupId(1), &to_batch(&layout[1]))
        .unwrap();
    let r_seg1 = SidecarReader::open_for_dataset(w_seg1.finish().unwrap().bytes, &schema).unwrap();

    let mut w_seg2 = SidecarWriter::for_dataset(def_t2, &schema).unwrap();
    w_seg2
        .set_rowgroup_range(RowGroupId(2)..RowGroupId(3))
        .unwrap();
    w_seg2
        .add_batch(RowGroupId(2), &to_batch(&layout[2]))
        .unwrap();
    let r_seg2 = SidecarReader::open_for_dataset(w_seg2.finish().unwrap().bytes, &schema).unwrap();

    let aapl = encode_sym_key("AAPL");
    let t1 = r_seg1.triples(&[&aapl]).unwrap().remove(&aapl).unwrap();
    let t2 = r_seg2.triples(&[&aapl]).unwrap().remove(&aapl).unwrap();

    // Check restatement & tie at valid_at = Some(200), known_at = Some(30):
    // Qualifying triples:
    // - (i64::MIN+1, i64::MIN+1, rg 0)
    // - (50, 30, rg 2) [late row with known=30, but valid=50 < 100]
    // - (100, 10, rg 0)
    // - (100, 20, rg 1)
    // - (100, 20, rg 2)
    // Winner must be valid=100, known=20, row_groups={1, 2}!
    let hit = bitemporal_winner(t1.iter().chain(&t2), Some(200), Some(30)).unwrap();
    assert_eq!(hit.valid, 100);
    assert_eq!(hit.known, 20);
    assert_eq!(hit.row_groups.iter().collect::<Vec<_>>(), vec![1, 2]);

    // At known_at = Some(15), the restatement at known=20 was not yet known, so (100, 10, rg 0) wins!
    let hit_earlier_known = bitemporal_winner(t1.iter().chain(&t2), Some(200), Some(15)).unwrap();
    assert_eq!(hit_earlier_known.valid, 100);
    assert_eq!(hit_earlier_known.known, 10);
    assert_eq!(
        hit_earlier_known.row_groups.iter().collect::<Vec<_>>(),
        vec![0]
    );

    // Unbounded valid_at and known_at -> sentinel i64::MAX wins
    let hit_max = bitemporal_winner(t1.iter().chain(&t2), None, None).unwrap();
    assert_eq!(hit_max.valid, i64::MAX);
    assert_eq!(hit_max.known, i64::MAX);
    assert_eq!(hit_max.row_groups.iter().collect::<Vec<_>>(), vec![2]);

    // At valid_at = Some(YEAR_9999_END_US), known_at = Some(10) -> year 9999 sentinel wins
    let hit_9999 =
        bitemporal_winner(t1.iter().chain(&t2), Some(YEAR_9999_END_US), Some(10)).unwrap();
    assert_eq!(hit_9999.valid, YEAR_9999_END_US);
    assert_eq!(hit_9999.known, 5);
    assert_eq!(hit_9999.row_groups.iter().collect::<Vec<_>>(), vec![0]);

    // At valid_at = Some(i64::MIN + 1), known_at = Some(i64::MIN + 1) -> i64::MIN + 1 wins
    let hit_min =
        bitemporal_winner(t1.iter().chain(&t2), Some(i64::MIN + 1), Some(i64::MIN + 1)).unwrap();
    assert_eq!(hit_min.valid, i64::MIN + 1);
    assert_eq!(hit_min.known, i64::MIN + 1);

    // At valid_at = Some(i64::MIN), nothing qualifies -> None
    assert!(bitemporal_winner(t1.iter().chain(&t2), Some(i64::MIN), None).is_none());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn bounds_and_temporal2_equal_independent_oracle(layout in layout_strategy()) {
        let schema = dataset_schema();
        let batches: Vec<RecordBatch> = layout.iter().map(|r| to_batch(r)).collect();

        // 1. Bounds over Utf8 key + Timestamp(us, UTC)
        let def_b_utf8 = SidecarDef {
            kind: SidecarKind::Bounds,
            columns: vec!["sym".into()],
            time_columns: vec!["valid_us".into()],
        };
        let mut w_b_utf8 = SidecarWriter::for_dataset(def_b_utf8, &schema).unwrap();
        // 2. Bounds over Int64 key + Date32
        let def_b_i64 = SidecarDef {
            kind: SidecarKind::Bounds,
            columns: vec!["sub".into()],
            time_columns: vec!["valid_d32".into()],
        };
        let mut w_b_i64 = SidecarWriter::for_dataset(def_b_i64, &schema).unwrap();
        // 3. Bounds over Composite (Utf8, Int64) key + Timestamp(us, UTC)
        let def_b_comp = SidecarDef {
            kind: SidecarKind::Bounds,
            columns: vec!["sym".into(), "sub".into()],
            time_columns: vec!["valid_us".into()],
        };
        let mut w_b_comp = SidecarWriter::for_dataset(def_b_comp, &schema).unwrap();

        // 4. Bitemporal over Utf8 key + Timestamp(us, UTC)
        let def_t_utf8 = SidecarDef {
            kind: SidecarKind::Bitemporal,
            columns: vec!["sym".into()],
            time_columns: vec!["valid_us".into(), "known_us".into()],
        };
        let mut w_t_utf8 = SidecarWriter::for_dataset(def_t_utf8.clone(), &schema).unwrap();
        // 5. Bitemporal over Composite (Utf8, Int64) key + Date32
        let def_t_comp_d32 = SidecarDef {
            kind: SidecarKind::Bitemporal,
            columns: vec!["sym".into(), "sub".into()],
            time_columns: vec!["valid_d32".into(), "known_d32".into()],
        };
        let mut w_t_comp_d32 = SidecarWriter::for_dataset(def_t_comp_d32, &schema).unwrap();

        for (i, b) in batches.iter().enumerate() {
            let rg = RowGroupId(u32::try_from(i).unwrap());
            w_b_utf8.add_batch(rg, b).unwrap();
            w_b_i64.add_batch(rg, b).unwrap();
            w_b_comp.add_batch(rg, b).unwrap();
            w_t_utf8.add_batch(rg, b).unwrap();
            w_t_comp_d32.add_batch(rg, b).unwrap();
        }

        let r_b_utf8 = SidecarReader::open_for_dataset(w_b_utf8.finish().unwrap().bytes, &schema).unwrap();
        let r_b_i64 = SidecarReader::open_for_dataset(w_b_i64.finish().unwrap().bytes, &schema).unwrap();
        let r_b_comp = SidecarReader::open_for_dataset(w_b_comp.finish().unwrap().bytes, &schema).unwrap();
        let r_t_utf8 = SidecarReader::open_for_dataset(w_t_utf8.finish().unwrap().bytes, &schema).unwrap();
        let r_t_comp_d32 = SidecarReader::open_for_dataset(w_t_comp_d32.finish().unwrap().bytes, &schema).unwrap();

        // Cross-kind calls must error
        let dummy = [encode_sym_key("AAPL")];
        prop_assert!(r_b_utf8.triples(&dummy).is_err());
        prop_assert!(r_t_utf8.bounds(&dummy).is_err());

        // Check 1: Utf8 + Timestamp(us, UTC) bounds
        let all_utf8_probes: Vec<Vec<u8>> = SYMBOLS
            .iter()
            .chain(&["ABSENT_1", "ABSENT_2"])
            .map(|s| encode_sym_key(s))
            .collect();
        let want_b_utf8 = oracle_bounds(&layout, |r| r.sym.map(encode_sym_key), |r| r.valid_us);
        let got_b_utf8 = r_b_utf8.bounds(&all_utf8_probes).unwrap();
        prop_assert_eq!(&got_b_utf8, &want_b_utf8);

        // Also check RgBounds::overlaps against row-level truth for several query windows
        for &(lo, hi) in &[
            (i64::MIN + 1, i64::MAX),
            (-100, 100),
            (0, YEAR_9999_END_US),
            (-86_400_000_000, -1),
        ] {
            for sym in SYMBOLS {
                let k = encode_sym_key(sym);
                let rgs_from_bounds: BTreeSet<u32> = got_b_utf8
                    .get(&k)
                    .into_iter()
                    .flatten()
                    .filter(|b| b.overlaps(lo, hi))
                    .map(|b| b.rg.0)
                    .collect();
                // Any row group actually containing a row for `sym` with `lo <= valid_us <= hi` MUST be in `rgs_from_bounds`!
                for (rg_idx, rows) in layout.iter().enumerate() {
                    let has_in_window = rows.iter().any(|r| {
                        r.sym == Some(sym) && r.valid_us.is_some_and(|t| t >= lo && t <= hi)
                    });
                    if has_in_window {
                        prop_assert!(rgs_from_bounds.contains(&(rg_idx as u32)));
                    }
                }
            }
        }

        // Check 2: Int64 + Date32 bounds
        let all_i64_probes: Vec<Vec<u8>> = (-5i64..6).map(encode_sub_key).collect();
        let want_b_i64 = oracle_bounds(
            &layout,
            |r| r.sub.map(encode_sub_key),
            |r| r.valid_d32.map(i64::from),
        );
        let got_b_i64 = r_b_i64.bounds(&all_i64_probes).unwrap();
        prop_assert_eq!(&got_b_i64, &want_b_i64);

        // Check 3: Composite (Utf8, Int64) bounds (note: probes with null trailing column match nothing in lookup per ADR 014)
        let non_null_comp_probes: Vec<Vec<u8>> = SYMBOLS
            .iter()
            .flat_map(|s| (-4i64..5).map(move |i| encode_comp_key(s, Some(i))))
            .collect();
        let want_b_comp = oracle_bounds(
            &layout,
            |r| match (r.sym, r.sub) {
                (Some(s), Some(i)) => Some(encode_comp_key(s, Some(i))),
                _ => None,
            },
            |r| r.valid_us,
        );
        let got_b_comp = r_b_comp.bounds(&non_null_comp_probes).unwrap();
        prop_assert_eq!(&got_b_comp, &want_b_comp);

        // Probe with trailing null marker must match nothing
        let null_comp_probes: Vec<Vec<u8>> = SYMBOLS
            .iter()
            .map(|s| encode_comp_key(s, None))
            .collect();
        prop_assert!(r_b_comp.bounds(&null_comp_probes).unwrap().is_empty());

        // Check 4: Bitemporal Utf8 + Timestamp(us, UTC) triples and bitemporal_winner
        let want_t_utf8 = oracle_triples(
            &layout,
            |r| r.sym.map(encode_sym_key),
            |r| r.valid_us.zip(r.known_us),
        );
        let got_t_utf8 = r_t_utf8.triples(&all_utf8_probes).unwrap();
        prop_assert_eq!(&got_t_utf8, &want_t_utf8);

        // Split layout into two segments and verify index-wide bitemporal_winner across segments
        let mid = (layout.len() / 2).max(1);
        let mut w1 = SidecarWriter::for_dataset(def_t_utf8.clone(), &schema).unwrap();
        let mut w2 = SidecarWriter::for_dataset(def_t_utf8, &schema).unwrap();
        for (i, b) in batches.iter().enumerate() {
            if i < mid {
                w1.add_batch(RowGroupId(i as u32), b).unwrap();
            } else {
                w2.add_batch(RowGroupId(i as u32), b).unwrap();
            }
        }
        let r1 = SidecarReader::open(w1.finish().unwrap().bytes).unwrap();
        let r2 = SidecarReader::open(w2.finish().unwrap().bytes).unwrap();
        let m1 = r1.triples(&all_utf8_probes).unwrap();
        let m2 = r2.triples(&all_utf8_probes).unwrap();

        let cutoffs = [
            None,
            Some(i64::MIN + 1),
            Some(-1),
            Some(0),
            Some(500),
            Some(YEAR_9999_END_US),
            Some(i64::MAX),
        ];
        for sym in SYMBOLS {
            let k = encode_sym_key(sym);
            let s1 = m1.get(&k).map_or(&[][..], Vec::as_slice);
            let s2 = m2.get(&k).map_or(&[][..], Vec::as_slice);
            for &v_at in &cutoffs {
                for &k_at in &cutoffs {
                    let got_win = bitemporal_winner(s1.iter().chain(s2), v_at, k_at);
                    let want_win = oracle_winner_from_rows(
                        &layout,
                        &k,
                        |r| r.sym.map(encode_sym_key),
                        |r| r.valid_us.zip(r.known_us),
                        v_at,
                        k_at,
                    );
                    prop_assert_eq!(got_win, want_win);
                }
            }
        }

        // Check 5: Bitemporal Composite + Date32 triples
        let want_t_comp_d32 = oracle_triples(
            &layout,
            |r| match (r.sym, r.sub) {
                (Some(s), Some(i)) => Some(encode_comp_key(s, Some(i))),
                _ => None,
            },
            |r| match (r.valid_d32, r.known_d32) {
                (Some(v), Some(k)) => Some((i64::from(v), i64::from(k))),
                _ => None,
            },
        );
        let got_t_comp_d32 = r_t_comp_d32.triples(&non_null_comp_probes).unwrap();
        prop_assert_eq!(&got_t_comp_d32, &want_t_comp_d32);
    }
}
