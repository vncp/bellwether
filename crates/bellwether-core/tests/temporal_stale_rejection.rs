//! Invariant 5: stale-version rejection. A temporal lookup never returns a
//! superseded row, even when the newest version lives in a row group pruned
//! for another reason. `temporal::latest` reports, across every segment, the
//! newest version's row groups and the key's index-wide `max_ts` regardless
//! of other predicates, so a caller that intersects with another predicate's
//! bitmap can reject stale rows.
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

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether_core::encoding::{encode_tuple, Value};
use bellwether_core::temporal::{self, is_stale};
use bellwether_core::{SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{KeySpec, RowGroupId};
use common::*;
use proptest::prelude::*;
use roaring::RoaringBitmap;

fn region_schema() -> Schema {
    let mut fields: Vec<Field> = schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("region", DataType::Utf8, false));
    Schema::new(fields)
}

fn rg_batch(rows: &[(i64, &str, Option<i64>, &str)]) -> RecordBatch {
    let col = |v: ArrayRef| v;
    RecordBatch::try_from_iter(vec![
        (
            "id",
            col(Arc::new(rows.iter().map(|r| r.0).collect::<Int64Array>())),
        ),
        (
            "tag",
            col(Arc::new(
                rows.iter().map(|r| Some(r.1)).collect::<StringArray>(),
            )),
        ),
        (
            "ts",
            col(Arc::new(
                rows.iter()
                    .map(|r| r.2)
                    .collect::<TimestampMillisecondArray>()
                    .with_timezone("UTC"),
            )),
        ),
        (
            "region",
            col(Arc::new(
                rows.iter().map(|r| Some(r.3)).collect::<StringArray>(),
            )),
        ),
    ])
    .unwrap()
}

fn bitmap(v: &[u32]) -> RoaringBitmap {
    v.iter().copied().collect()
}

#[test]
fn newest_version_in_a_pruned_row_group_still_rejects_the_stale_row() {
    // rg0: version at ts=10 ms in region east
    // rg1: version at ts=20 ms in region west   <- the newest, pruned by region
    // rg2: version with a null ts in region east (never wins)
    let rgs = [
        rg_batch(&[(1, "a", Some(10), "east"), (2, "b", Some(5), "east")]),
        rg_batch(&[(1, "a", Some(20), "west")]),
        rg_batch(&[(1, "a", None, "east")]),
    ];
    let schema = region_schema();
    // Temporal segments split so the newest version lives in another segment.
    let mut temporal_segs = Vec::new();
    for chunk in [&[0usize, 2][..], &[1][..]] {
        let mut w =
            SegmentWriter::for_dataset(common::temporal(), &schema, WriterOptions::default())
                .unwrap();
        for &i in chunk {
            w.add_batch(RowGroupId(u32::try_from(i).unwrap()), &rgs[i])
                .unwrap();
        }
        temporal_segs.push(SegmentReader::open(w.finish().unwrap().bytes).unwrap());
    }
    let region = KeySpec::Exact {
        columns: vec!["region".into()],
        counts: false,
        bounds: None,
    };
    let mut w = SegmentWriter::for_dataset(region, &schema, WriterOptions::default()).unwrap();
    for (i, b) in rgs.iter().enumerate() {
        w.add_batch(RowGroupId(u32::try_from(i).unwrap()), b)
            .unwrap();
    }
    let region_seg = SegmentReader::open(w.finish().unwrap().bytes).unwrap();

    let k = key(1, "a");
    let east = region_seg
        .lookup(&[encode_tuple(&[Value::Utf8("east")]).unwrap()])
        .unwrap();
    assert_eq!(east, bitmap(&[0, 2]));

    // One segment alone does not know about the newest version...
    let partial = temporal_segs[0].latest(&[&k]).unwrap();
    assert_eq!(partial[&k].ts, 10);
    // ...the index-wide answer, over every segment, does.
    let latest = temporal::latest(&temporal_segs, &[&k]).unwrap();
    let hit = &latest[&k];
    assert_eq!(
        hit.ts, 20,
        "max_ts is index-wide, in the column's unit (ms)"
    );
    assert_eq!(
        hit.row_groups,
        bitmap(&[1]),
        "the newest version's row groups"
    );
    let members = temporal_segs
        .iter()
        .map(|s| s.lookup(&[&k]).unwrap())
        .fold(RoaringBitmap::new(), |a, b| a | b);
    assert_eq!(members, bitmap(&[0, 1, 2]));

    // The caller prunes to region = east and reads rg0 and rg2. Every
    // candidate row is older than max_ts (or null) and is rejected; the
    // superseded ts=10 row is never returned as "latest".
    let planned = &members & &east;
    assert!(!planned.contains(1), "the newest row group is pruned");
    for ts in [Some(10i64), None] {
        assert!(is_stale(hit, ts));
    }
    // And a plan that keeps rg1 accepts exactly the newest row.
    assert!(!is_stale(hit, Some(20)));

    // asof between the versions picks the older one, index-wide.
    let asof = temporal::asof(&temporal_segs, &[&k], 15).unwrap();
    assert_eq!(
        (asof[&k].ts, asof[&k].row_groups.clone()),
        (10, bitmap(&[0]))
    );
}

#[test]
fn the_fold_refuses_segments_of_other_specs_or_recorded_types() {
    let k = key(1, "a");
    let one = |spec: KeySpec| {
        let mut w = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
        w.insert_version(&k, Some(5), RowGroupId(0)).unwrap();
        SegmentReader::open(w.finish().unwrap().bytes).unwrap()
    };
    // two temporal specs over different columns
    let other = KeySpec::Temporal {
        columns: vec!["id".into(), "tag".into()],
        ts_column: "updated_at".into(),
        counts: false,
    };
    let mixed = [one(common::temporal()), one(other)];
    assert!(temporal::latest(&mixed, &[&k]).is_err());
    assert!(temporal::asof(&mixed, &[&k], 9).is_err());
    // the same spec recorded under different types (typed and untyped)
    let typed = SegmentReader::open(
        build(
            &common::temporal(),
            &vec![vec![]],
            &[],
            &WriterOptions::default(),
        )[0]
        .bytes
        .clone(),
    )
    .unwrap();
    let mixed = [typed, one(common::temporal())];
    assert!(temporal::latest(&mixed, &[&k]).is_err());
}

#[test]
fn asof_and_latest_use_the_column_unit_including_year_9999() {
    // A microsecond column with a year-9999 sentinel: no nanosecond overflow.
    let sentinel = 253_402_300_799_999_999i64;
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]);
    let b = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from(vec![1i64, 1, 1])) as ArrayRef,
        ),
        (
            "tag",
            Arc::new(StringArray::from(vec!["a", "a", "a"])) as ArrayRef,
        ),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_000),
                Some(sentinel),
                None,
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut w =
        SegmentWriter::for_dataset(common::temporal(), &schema, WriterOptions::default()).unwrap();
    w.add_batch(RowGroupId(4), &b).unwrap();
    let segs = vec![SegmentReader::open(w.finish().unwrap().bytes).unwrap()];
    let k = key(1, "a");
    assert_eq!(temporal::latest(&segs, &[&k]).unwrap()[&k].ts, sentinel);
    assert_eq!(
        temporal::asof(&segs, &[&k], sentinel - 1).unwrap()[&k].ts,
        1_000
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Over random layouts and a random "other predicate" that excludes some
    /// row groups, a caller that ranks candidate rows against
    /// `temporal::latest` never accepts a superseded row, and accepts the
    /// newest row whenever its row group survives the other predicate.
    #[test]
    fn caller_never_accepts_a_superseded_row(
        layout in layout(),
        cuts in prop::collection::vec(0usize..20, 0..3),
        allowed_mask in any::<u32>(),
        id in 0i64..16,
        tag in prop::sample::select(TAGS.to_vec()),
    ) {
        let readers = open(&build(&common::temporal(), &layout, &cuts, &WriterOptions { block_keys: 2, ..WriterOptions::default() }));
        let k = key(id, tag);
        let allowed: RoaringBitmap = (0..u32::try_from(layout.len()).unwrap()).filter(|i| allowed_mask & (1 << (i % 32)) != 0).collect();

        let hit = temporal::latest(&readers, &[&k]).unwrap().remove(&k);
        let members = readers.iter().map(|r| r.lookup(&[&k]).unwrap()).fold(RoaringBitmap::new(), |a, b| a | b);
        let truth_latest = true_version(&layout, id, tag, None);
        let planned = &members & &allowed;
        let mut accepted = Vec::new();
        for rg in &planned {
            for r in &layout[rg as usize] {
                if has_key(r, id, tag) && hit.as_ref().is_some_and(|h| !is_stale(h, r.ts)) {
                    accepted.push((rg, r.ts));
                }
            }
        }
        match truth_latest {
            None => prop_assert!(accepted.is_empty() && hit.is_none()),
            Some((max, rgs)) => {
                let h = hit.unwrap();
                prop_assert_eq!(h.ts, max);
                prop_assert_eq!(&h.row_groups, &rgs, "latest reports every row group at max_ts");
                for (_, ts) in &accepted {
                    prop_assert_eq!(*ts, Some(max), "accepted a superseded row");
                }
                let surviving = &rgs & &allowed;
                let accepted_rgs: RoaringBitmap = accepted.iter().map(|a| a.0).collect();
                prop_assert_eq!(accepted_rgs, surviving);
            }
        }
    }
}
