//! PR #30 review, item 4 and the ruling on two bitemporal sources: the
//! fold of an index written before M5 whose four sidecars cannot all fold.
//!
//! The index declares `exact:sym` beside the sidecars `bounds:sym@ts`,
//! `bounds:sym@valid`, `temporal2:sym@valid,known` and
//! `temporal2:sym@ts,known`, each built over every row group with the
//! storage version 1 sidecar writer, as a binary of the first storage
//! version 2 release built them. Its first build with work folds
//! `bounds:sym@ts` into `exact:sym@ts` and `temporal2:sym@valid,known`
//! into the bitemporal key of that identity, and keeps the other two as
//! sidecars (their key spec's slot holds one on another time column).
//! Every lookup plans the same row groups before and after: a window on
//! `valid` through `bounds:sym@valid`, a window on `ts`, and a bitemporal
//! as-of on each pair of clocks, each equal to an oracle over the rows.
//! A bitemporal as-of that names no clocks, with two bitemporal sources on
//! `sym`, is refused naming both and the clock columns that pick each.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::type_complexity
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether::types::{EvidenceBody, KeySpec, Record, RowGroupId};
use bellwether::{Engine, EngineOptions, KeyFile, PlanResult};
use bellwether_core::sidecar::{SidecarDef, SidecarKind, SidecarWriter};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Value};

const DAY: i64 = 86_400_000_000;
const HOUR: i64 = 3_600_000_000;
/// 2026-01-01T00:00:00Z in microseconds.
const BASE: i64 = 20_454 * DAY;
const FILES: i64 = 3;
const RGS: i64 = 4;
const ROWS: i64 = 3;

fn schema() -> SchemaRef {
    let ts = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("ts", ts(), true),
        Field::new("valid", ts(), true),
        Field::new("known", ts(), true),
    ]))
}

/// One row: (file, row group, sym, ts, valid, known).
type Row = (
    String,
    usize,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);

/// `FILES` files of `RGS` row groups of `ROWS` rows. `ts` rises with the
/// row group; `valid` is scrambled across row groups (so a window on it
/// prunes other row groups than one on `ts`), `known` trails `valid` by 0
/// to 2 days (restatements), and some rows have a null clock.
fn corpus(dir: &Path) -> Vec<Row> {
    std::fs::create_dir_all(dir).unwrap();
    let mut rows = Vec::new();
    for f in 0..FILES {
        let name = format!("part-{f}.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(usize::MAX))
            .build();
        let file = File::create(dir.join(&name)).unwrap();
        let mut w = ArrowWriter::try_new(file, schema(), Some(props)).unwrap();
        for r in 0..RGS {
            let n = f * RGS + r;
            let mut cols: (
                Vec<Option<String>>,
                Vec<Option<i64>>,
                Vec<Option<i64>>,
                Vec<Option<i64>>,
            ) = Default::default();
            for i in 0..ROWS {
                let sym = ["A", "B", "C"][usize::try_from((i + r) % 3).unwrap()];
                let ts = BASE + n * DAY + i * HOUR;
                let valid = BASE + ((n * 7 + i * 3) % 12) * DAY;
                let known = valid + ((n + i) % 3) * DAY;
                let (valid, known) = match (n + i) % 5 {
                    3 => (None, Some(known)),
                    4 => (Some(valid), None),
                    _ => (Some(valid), Some(known)),
                };
                cols.0.push(Some(sym.to_string()));
                cols.1.push(Some(ts));
                cols.2.push(valid);
                cols.3.push(known);
                rows.push((
                    name.clone(),
                    usize::try_from(r).unwrap(),
                    Some(sym.to_string()),
                    Some(ts),
                    valid,
                    known,
                ));
            }
            let t = |v: Vec<Option<i64>>| -> ArrayRef {
                Arc::new(TimestampMicrosecondArray::from(v).with_timezone("UTC"))
            };
            let batch = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(StringArray::from(cols.0)) as ArrayRef,
                    t(cols.1),
                    t(cols.2),
                    t(cols.3),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
    rows
}

fn keys(path: &Path, syms: &[&str]) -> KeyFile {
    let s = Arc::new(Schema::new(vec![Field::new("sym", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&s),
        vec![Arc::new(StringArray::from(syms.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), s, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    KeyFile::new(path)
}

fn def(kind: SidecarKind, time: &[&str]) -> SidecarDef {
    SidecarDef {
        kind,
        columns: vec!["sym".into()],
        time_columns: time.iter().map(|t| (*t).to_string()).collect(),
    }
}

/// The four sidecars, in the order the review's index declared them.
fn sidecars() -> [SidecarDef; 4] {
    [
        def(SidecarKind::Bounds, &["ts"]),
        def(SidecarKind::Bounds, &["valid"]),
        def(SidecarKind::Bitemporal, &["valid", "known"]),
        def(SidecarKind::Bitemporal, &["ts", "known"]),
    ]
}

/// Adds `sidecars()` to the index under `data`, as a binary of the first
/// storage version 2 release wrote them: declared in `schema.sidecars`,
/// each built over every live row group with the storage version 1 sidecar
/// layout, listed with its kind under its name, and complete.
fn add_legacy_sidecars(data: &Path) {
    let index = data.join(".bellwether");
    let mut m: Value =
        serde_json::from_slice(&std::fs::read(index.join("manifest.json")).unwrap()).unwrap();
    let sequence = m["sequence"].as_u64().unwrap();
    let next_id = u32::try_from(m["rowgroup_space"]["next_id"].as_u64().unwrap()).unwrap();
    let files: Vec<(String, Vec<u32>)> = m["rowgroup_space"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["fingerprint"]["path"].as_str().unwrap().to_string(),
                f["rowgroup_ids"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|i| u32::try_from(i.as_u64().unwrap()).unwrap())
                    .collect(),
            )
        })
        .collect();
    for d in sidecars() {
        let mut w = SidecarWriter::for_dataset(d.clone(), &schema()).unwrap();
        w.set_rowgroup_range(RowGroupId(0)..RowGroupId(next_id))
            .unwrap();
        for (path, ids) in &files {
            for (i, id) in ids.iter().enumerate() {
                let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
                    .unwrap()
                    .with_row_groups(vec![i])
                    .build()
                    .unwrap();
                for batch in reader {
                    w.add_batch(RowGroupId(*id), &batch.unwrap()).unwrap();
                }
            }
        }
        let seg = w.finish().unwrap();
        std::fs::write(index.join(seg.path()), &seg.bytes).unwrap();
        let name = d.name();
        if m["schema"].get("sidecars").is_none() {
            m["schema"]["sidecars"] = json!([]);
        }
        m["schema"]["sidecars"].as_array_mut().unwrap().push(json!({
            "kind": d.kind.as_str(),
            "columns": d.columns,
            "time_columns": d.time_columns,
        }));
        m["segments"].as_array_mut().unwrap().push(json!({
            "id": seg.id.to_hex(),
            "kind": d.kind.as_str(),
            "key_spec": name,
            "rowgroup_range": {"start": 0, "end": next_id},
            "bytes": seg.bytes.len(),
        }));
        m["key_spec_completeness"][&name] = json!(sequence);
    }
    let bytes = serde_json::to_vec_pretty(&m).unwrap();
    std::fs::write(index.join("manifest.json"), &bytes).unwrap();
    std::fs::write(
        index.join("manifests").join(format!("{sequence:020}.json")),
        &bytes,
    )
    .unwrap();
}

fn planned(r: &PlanResult) -> BTreeSet<(String, usize)> {
    let Record::Evidence(e) = &r.evidence.record else {
        panic!("not evidence")
    };
    let EvidenceBody::Plan { plan } = &e.body else {
        panic!("not a plan")
    };
    plan.entries
        .iter()
        .flat_map(|en| {
            let name = Path::new(&en.file.path)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            en.row_groups
                .iter()
                .map(move |r| (name.clone(), r as usize))
        })
        .collect()
}

/// The row groups where key `sym`'s own `[min, max]` of a time column
/// overlaps `[lo, hi]` (what per-key bounds plan).
fn overlapping(
    rows: &[Row],
    sym: &str,
    time: fn(&Row) -> Option<i64>,
    lo: i64,
    hi: i64,
) -> BTreeSet<(String, usize)> {
    let mut per: BTreeMap<(String, usize), (i64, i64)> = BTreeMap::new();
    for r in rows.iter().filter(|r| r.2.as_deref() == Some(sym)) {
        if let Some(t) = time(r) {
            let e = per.entry((r.0.clone(), r.1)).or_insert((t, t));
            e.0 = e.0.min(t);
            e.1 = e.1.max(t);
        }
    }
    per.into_iter()
        .filter(|(_, (a, b))| *a <= hi && lo <= *b)
        .map(|(k, _)| k)
        .collect()
}

/// The row groups holding key `sym`'s bitemporal winner at `(v, k)` on
/// the clocks `valid` and `known`: the greatest valid at or before `v`
/// among versions known at or before `k`, then the greatest known.
fn winners(
    rows: &[Row],
    sym: &str,
    valid: fn(&Row) -> Option<i64>,
    known: fn(&Row) -> Option<i64>,
    v: i64,
    k: i64,
) -> BTreeSet<(String, usize)> {
    let versions = || {
        rows.iter()
            .filter(|r| r.2.as_deref() == Some(sym))
            .filter_map(|r| Some((r, valid(r)?, known(r)?)))
            .filter(|(_, a, b)| *a <= v && *b <= k)
    };
    let Some(best) = versions().map(|(_, a, b)| (a, b)).max() else {
        return BTreeSet::new();
    };
    versions()
        .filter(|(_, a, b)| (*a, *b) == best)
        .map(|(r, _, _)| (r.0.clone(), r.1))
        .collect()
}

#[test]
fn a_pre_m5_index_whose_sidecars_cannot_all_fold_keeps_them_and_plans_the_same() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let rows = corpus(&data);
    let a = keys(&dir.path().join("a.parquet"), &["A"]);
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    let exact = KeySpec::Exact {
        columns: vec!["sym".into()],
        counts: false,
        bounds: None,
    };
    e.index(std::slice::from_ref(&exact)).unwrap();
    add_legacy_sidecars(&data);

    let ts = |r: &Row| r.3;
    let valid = |r: &Row| r.4;
    let known = |r: &Row| r.5;
    let lo = BASE + 2 * DAY;
    let hi = BASE + 4 * DAY;
    let all_a: BTreeSet<(String, usize)> = rows
        .iter()
        .filter(|r| r.2.as_deref() == Some("A"))
        .map(|r| (r.0.clone(), r.1))
        .collect();
    let v_at = (BASE + 6 * DAY).to_string();
    let k_at = (BASE + 7 * DAY).to_string();
    let (vi, ki) = (BASE + 6 * DAY, BASE + 7 * DAY);

    // Every lookup the four sidecars serve, with the oracle's answer.
    let check = |e: &Engine, when: &str| {
        let on_valid = planned(
            &e.locate_window(&a, &lo.to_string(), &hi.to_string(), Some("valid"))
                .unwrap(),
        );
        let want = overlapping(&rows, "A", valid, lo, hi);
        assert_eq!(on_valid, want, "{when}: window on valid");
        assert!(
            !want.is_empty() && want.len() < all_a.len(),
            "{when}: the valid bounds prune ({} of {})",
            want.len(),
            all_a.len()
        );
        let on_ts = planned(
            &e.locate_window(&a, &lo.to_string(), &hi.to_string(), Some("ts"))
                .unwrap(),
        );
        assert_eq!(
            on_ts,
            overlapping(&rows, "A", ts, lo, hi),
            "{when}: window on ts"
        );
        for ((vc, kc), (vf, kf)) in [
            (
                ("valid", "known"),
                (
                    valid as fn(&Row) -> Option<i64>,
                    known as fn(&Row) -> Option<i64>,
                ),
            ),
            (
                ("ts", "known"),
                (
                    ts as fn(&Row) -> Option<i64>,
                    known as fn(&Row) -> Option<i64>,
                ),
            ),
        ] {
            let got = planned(
                &e.asof_bitemporal(&a, Some(&v_at), Some(&k_at), Some((vc, kc)), false)
                    .unwrap(),
            );
            assert_eq!(
                got,
                winners(&rows, "A", vf, kf, vi, ki),
                "{when}: {vc},{kc}"
            );
        }
        // Two bitemporal sources on sym: a lookup naming no clocks is
        // refused, naming both and the clocks that pick each.
        let err = e
            .asof_bitemporal(&a, Some(&v_at), Some(&k_at), None, false)
            .unwrap_err()
            .to_string();
        for want in [
            "several bitemporal keys serve a lookup on (sym)",
            "temporal2:sym@valid,known (--valid-by valid --known-by known)",
            "temporal2:sym@ts,known (--valid-by ts --known-by known)",
            "pick one by its clock columns with --valid-by",
        ] {
            assert!(err.contains(want), "{when}: {err}");
        }
        (on_valid, on_ts)
    };
    let before = check(&e, "before the fold");

    // The first build with work (a new key spec) folds what it can.
    let r = e
        .index(&[KeySpec::Range {
            column: "ts".into(),
        }])
        .unwrap();
    let folded: Vec<(String, String)> = r
        .sidecars_folded
        .iter()
        .map(|f| (f.sidecar.clone(), f.into.clone()))
        .collect();
    assert_eq!(
        folded,
        [
            ("bounds:sym@ts".to_string(), "exact:sym@ts".to_string()),
            (
                "temporal2:sym@valid,known".to_string(),
                "temporal2:sym@valid,known".to_string()
            ),
        ]
    );
    assert_eq!(
        r.kept_legacy,
        ["bounds:sym@valid", "temporal2:sym@ts,known"]
    );
    let m: Value =
        serde_json::from_slice(&std::fs::read(data.join(".bellwether/manifest.json")).unwrap())
            .unwrap();
    let kept: Vec<String> = m["schema"]["sidecars"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            format!(
                "{}:{}@{}",
                d["kind"].as_str().unwrap(),
                d["columns"][0].as_str().unwrap(),
                d["time_columns"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| c.as_str().unwrap())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect();
    assert_eq!(kept, ["bounds:sym@valid", "temporal2:sym@ts,known"]);
    for name in &kept {
        assert!(
            m["key_spec_completeness"].get(name).is_some(),
            "{name} is still complete"
        );
    }
    let after = check(&e, "after the fold");
    assert_eq!(before, after);

    // A no-op build folds nothing more and still keeps both.
    let r = e.index(&[]).unwrap();
    assert_eq!(r.committed, None);
    assert!(r.sidecars_folded.is_empty() && r.fold_pending.is_empty());
    assert_eq!(
        r.kept_legacy,
        ["bounds:sym@valid", "temporal2:sym@ts,known"]
    );
}
