//! Builds over a synthetic versioned ledger shaped like `bellwether-bench`'s
//! (WP10): the same six columns and types, `data/day=YYYY-MM-DD/part-N`
//! files, entities re-versioned every day and written round-robin across a
//! day's files, 20-minute batch stamps that look like ids, back-dated
//! versions, a skewed `status`, and a Parquet-native bloom on `entity_id`.
//! The layering (`dev/check-layering.py`) does not let this crate depend on
//! the bench crate, not even for tests, so the generator is restated here;
//! `real_bench_corpus` runs the same checks over a corpus the bench crate
//! wrote (`BELLWETHER_BENCH_CORPUS=<corpus dir>`, `--ignored`).

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::cast_possible_wrap,
    clippy::assert_is_empty,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_precision_loss,
    clippy::redundant_closure_for_method_calls,
    clippy::format_push_string
)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_build::{completeness, BuildOptions, CompactOptions, Lookup, SourceKind};
use bellwether_core::encoding::Value;
use bellwether_types::KeySpec;
use common::*;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

const DAY_US: i64 = 86_400_000_000;
const BATCH_WINDOW_US: i64 = 20 * 60 * 1_000_000;
/// 2026-01-01T00:00:00Z.
const EPOCH_DAY: i64 = 20_454;
const STATUSES: [(&str, u64); 5] = [
    ("posted", 60),
    ("settled", 25),
    ("pending", 10),
    ("reversed", 4),
    ("void", 1),
];

fn ledger_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::UInt64, false),
        Field::new("batch_id", DataType::Int64, false),
        Field::new(
            "version_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("status", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(18, 2), false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// `YYYYMMDDHHMM` of the 20-minute window holding `us`.
fn batch_id(us: i64) -> i64 {
    let w = us.div_euclid(BATCH_WINDOW_US) * BATCH_WINDOW_US;
    let days = w.div_euclid(DAY_US);
    let minute = (w - days * DAY_US) / 60_000_000;
    let (y, m, d) = civil(days);
    ((y * 100 + i64::from(m)) * 100 + i64::from(d)) * 10_000 + (minute / 60) * 100 + minute % 60
}

#[derive(Clone, Copy, Debug)]
struct Params {
    days: u32,
    files_per_day: u32,
    rgs_per_file: u32,
    rows_per_rg: u32,
    entities: u64,
    churn: f64,
    late: u64,
}

/// Writes day `day` of the ledger under `root/data`.
fn write_day(root: &Path, p: &Params, day: u32) {
    let rows_per_day = u64::from(p.files_per_day * p.rgs_per_file * p.rows_per_rg);
    let active = ((p.churn * p.entities as f64).round() as u64)
        .clamp(1, p.entities)
        .min(rows_per_day);
    let start = (EPOCH_DAY + i64::from(day)) * DAY_US;
    let (y, m, d) = civil(EPOCH_DAY + i64::from(day));
    let dir = root.join(format!("data/day={y:04}-{m:02}-{d:02}"));
    std::fs::create_dir_all(&dir).unwrap();
    let props = WriterProperties::builder()
        .set_column_bloom_filter_enabled(ColumnPath::from("entity_id"), true)
        .set_column_bloom_filter_max_ndv(ColumnPath::from("entity_id"), u64::from(p.rows_per_rg))
        .set_column_bloom_filter_fpp(ColumnPath::from("entity_id"), 0.01)
        .set_max_row_group_row_count(Some(p.rows_per_rg as usize + 1))
        .build();
    for f in 0..p.files_per_day {
        let global = day * p.files_per_day + f;
        let path = dir.join(format!("part-{global:05}.parquet"));
        let mut w = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            ledger_schema(),
            Some(props.clone()),
        )
        .unwrap();
        for rg in 0..p.rgs_per_file {
            let mut cols: (
                Vec<u64>,
                Vec<i64>,
                Vec<i64>,
                Vec<&str>,
                Vec<i128>,
                Vec<String>,
            ) = Default::default();
            for i in 0..p.rows_per_rg {
                let pos = u64::from(rg * p.rows_per_rg + i);
                let j = u64::from(f) + u64::from(p.files_per_day) * pos;
                // a day-keyed shuffle of the day's active entities
                let e = (j % active)
                    .wrapping_mul(7919)
                    .wrapping_add(u64::from(day) * 104_729)
                    % p.entities;
                let h = mix(u64::from(day) << 40 ^ u64::from(global) << 20 ^ pos);
                let write_us =
                    start + (j as i64) * DAY_US / rows_per_day as i64 + (h % 1000) as i64;
                let back = i64::from(day.min(3));
                let version_us = if back > 0 && h % 100 < p.late {
                    write_us - (1 + (h >> 8) as i64 % back) * DAY_US
                } else {
                    write_us
                };
                let mut roll = (h >> 16) % 100;
                let status = STATUSES
                    .iter()
                    .find(|(_, wgt)| {
                        let hit = roll < *wgt;
                        roll = roll.saturating_sub(*wgt);
                        hit
                    })
                    .map_or("posted", |s| s.0);
                cols.0.push(e);
                cols.1.push(batch_id(write_us));
                cols.2.push(version_us);
                cols.3.push(status);
                cols.4.push(i128::from((h >> 24) % 5_500_000) - 500_000);
                cols.5.push(format!("{h:032x}"));
            }
            let batch = RecordBatch::try_new(
                ledger_schema(),
                vec![
                    Arc::new(UInt64Array::from(cols.0)) as ArrayRef,
                    Arc::new(Int64Array::from(cols.1)),
                    Arc::new(TimestampMicrosecondArray::from(cols.2).with_timezone("UTC")),
                    Arc::new(StringArray::from(cols.3)),
                    Arc::new(
                        Decimal128Array::from(cols.4)
                            .with_precision_and_scale(18, 2)
                            .unwrap(),
                    ),
                    Arc::new(StringArray::from(cols.5)),
                ],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
}

fn parquet_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            parquet_files(&p, out);
        } else if p.extension().is_some_and(|e| e == "parquet") {
            out.push(p);
        }
    }
}

/// Ground-truth rows, decoded with the `parquet` crate, keeping only the
/// columns the key specs name (the payload would dominate memory).
fn ledger_rows(data: &Path) -> Vec<Row> {
    let keyed = Schema::new(
        ledger_schema()
            .fields()
            .iter()
            .filter(|f| !matches!(f.name().as_str(), "payload" | "amount"))
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    );
    let mut files = Vec::new();
    parquet_files(data, &mut files);
    files.iter().flat_map(|f| parquet_rows(f, &keyed)).collect()
}

fn specs() -> Vec<KeySpec> {
    vec![
        exact(&["entity_id"]),
        temporal(&["entity_id"], "version_ts"),
        bloom(&["status"]),
        range("batch_id"),
        exact(&["entity_id", "batch_id"]),
        range("version_ts"),
    ]
}

fn entity(e: u64) -> Vec<u8> {
    enc(&Value::UInt(e), &DataType::UInt64)
}

/// The checks shared by the synthetic and the real corpus.
fn check_ledger(
    store: &Arc<bellwether_store::ObjectStoreBackend>,
    rows: &[Row],
    sample: &[u64],
    absent: u64,
) {
    let p = planner(store);
    let mut keys: Vec<Vec<u8>> = sample.iter().map(|&e| entity(e)).collect();
    keys.push(entity(absent));
    let ent = cols(&["entity_id"]);
    check_keys(&p, rows, &[exact(&["entity_id"])], &ent, &keys);
    let present = rows[0].tuple(&ent).unwrap();
    let one = check_keys(&p, rows, &[exact(&["entity_id"])], &ent, &[present]);
    assert!(one.row_groups_selected >= 1);
    assert!(
        one.row_groups_selected < one.row_groups_total,
        "an entity id prunes"
    );
    check_keys(
        &p,
        rows,
        &[temporal(&["entity_id"], "version_ts")],
        &ent,
        &keys,
    );
    check_keys(
        &p,
        rows,
        &[bloom(&["status"])],
        &cols(&["status"]),
        &[enc_str("void")],
    );
    // latest and asof: the newest version is often back-dated into another
    // day's files or pruned by the batch stamp; the bound still applies
    let t = temporal(&["entity_id"], "version_ts");
    check_latest(
        &p,
        rows,
        std::slice::from_ref(&t),
        &ent,
        &keys,
        &ent,
        "version_ts",
        None,
    );
    let ts_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let mid = rows
        .iter()
        .filter_map(|r| r.get("version_ts"))
        .min()
        .unwrap()
        .clone();
    let mid_us = i64::from_be_bytes(mid[3..11].try_into().unwrap()) ^ i64::MIN;
    let at = mid_us + DAY_US;
    let at_enc = enc(
        &Value::Timestamp {
            value: at,
            unit: TimeUnit::Microsecond,
        },
        &ts_type,
    );
    check_latest(
        &p,
        rows,
        std::slice::from_ref(&t),
        &ent,
        &keys,
        &ent,
        "version_ts",
        Some((at, at_enc)),
    );
    // (entity, batch) tuples: projection onto both specs, intersected per key
    let tuples: Vec<Vec<u8>> = rows
        .iter()
        .step_by(97)
        .take(12)
        .map(|r| r.tuple(&cols(&["entity_id", "batch_id"])).unwrap())
        .collect();
    let eb = cols(&["entity_id", "batch_id"]);
    check_keys(
        &p,
        rows,
        &[exact(&["entity_id"]), range("batch_id")],
        &eb,
        &tuples,
    );
    check_keys(&p, rows, &[exact(&["entity_id", "batch_id"])], &eb, &tuples);
    check_latest(
        &p,
        rows,
        &[t, range("batch_id")],
        &eb,
        &tuples,
        &ent,
        "version_ts",
        None,
    );
    // one batch window by range
    let b = rows[rows.len() / 2].get("batch_id").unwrap().clone();
    let plan = p
        .plan(
            &[range("batch_id")],
            &Lookup::Between {
                column: "batch_id".into(),
                lo: b.clone(),
                hi: b,
            },
        )
        .unwrap();
    let residual = plan.residual.as_ref().unwrap();
    let truth = sorted(apply_residual(residual, rows.to_vec()));
    assert!(!truth.is_empty());
    assert_eq!(
        sorted(apply_residual(residual, planned_rows(&plan, rows))),
        truth
    );
}

#[test]
fn builds_over_a_bench_shaped_ledger() {
    let s = Scratch::new();
    let params = Params {
        days: 3,
        files_per_day: 2,
        rgs_per_file: 2,
        rows_per_rg: 64,
        entities: 2_000,
        churn: 0.1,
        late: 15,
    };
    for d in 0..params.days {
        write_day(s.root(), &params, d);
    }
    let data = s.path("data");
    let data_root = data.to_str().unwrap().to_string();
    let store = memory_store();
    let b = builder(&store);
    let probe = Arc::new(Probe::default());
    let opts = BuildOptions {
        observer: Some(probe.clone()),
        max_segment_keys: 150,
        max_segment_row_groups: 4,
        threads: Some(3),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    // no schema supplied: the bench schema is inferred and recorded as is
    let r = build_ok(&b, &data_root, &specs(), &opts);
    assert_eq!((r.files_new, r.row_groups_read), (6, 12));
    let m = b.manifest().unwrap().unwrap();
    let recorded: Vec<String> = m
        .schema
        .columns
        .iter()
        .map(|c| c.data_type.clone())
        .collect();
    let expected: Vec<String> = ledger_schema()
        .fields()
        .iter()
        .map(|f| f.data_type().to_string())
        .collect();
    assert_eq!(recorded, expected);
    for spec in specs() {
        assert_eq!(completeness(&m, &spec), Some(1), "{}", spec.name());
    }
    let rows = ledger_rows(&data);
    let sample: Vec<u64> = (0..params.entities).step_by(97).collect();
    check_ledger(&store, &rows, &sample, 10_000);

    // a new day arrives: only its files are read; latest moves forward
    write_day(s.root(), &params, params.days);
    let probe = Arc::new(Probe::default());
    let r = build_ok(
        &b,
        &data_root,
        &specs(),
        &BuildOptions {
            observer: Some(probe.clone()),
            ..opts.clone()
        },
    );
    assert_eq!((r.files_new, r.files_unchanged), (2, 6));
    assert_eq!(probe.reads.load(Ordering::SeqCst), 4);
    let rows = ledger_rows(&data);
    check_ledger(&store, &rows, &sample, 10_000);

    // compaction changes no answer
    let c = b.compact(&CompactOptions::default()).unwrap();
    assert!(c.merges >= 1);
    check_ledger(&store, &rows, &sample, 10_000);
}

/// The same checks over a corpus written by the bench crate:
/// `cargo run -p bellwether-bench -- generate --out <dir> ...`, then
/// `BELLWETHER_BENCH_CORPUS=<dir> cargo test -p bellwether-build --test ledger -- --ignored`.
#[test]
#[ignore = "needs a corpus generated by bellwether-bench; set BELLWETHER_BENCH_CORPUS"]
fn real_bench_corpus() {
    let corpus = std::env::var("BELLWETHER_BENCH_CORPUS").expect("BELLWETHER_BENCH_CORPUS");
    let data = PathBuf::from(corpus).join("data");
    let data = std::fs::canonicalize(data).unwrap();
    let store = memory_store();
    let b = builder(&store);
    let probe = Arc::new(Probe::default());
    let opts = BuildOptions {
        observer: Some(probe.clone()),
        ..BuildOptions::new(SourceKind::Parquet)
    };
    let r = build_ok(&b, data.to_str().unwrap(), &specs(), &opts);
    let m = b.manifest().unwrap().unwrap();
    for spec in specs() {
        assert_eq!(completeness(&m, &spec), Some(1), "{}", spec.name());
    }
    let rows = ledger_rows(&data);
    let mut ents: Vec<u64> = rows
        .iter()
        .map(|r| {
            let b = r.get("entity_id").unwrap();
            u64::from_be_bytes(b[1..9].try_into().unwrap())
        })
        .collect();
    ents.sort_unstable();
    ents.dedup();
    let sample: Vec<u64> = ents
        .iter()
        .step_by((ents.len() / 64).max(1))
        .copied()
        .collect();
    check_ledger(&store, &rows, &sample, u64::MAX);
    eprintln!(
        "real corpus: {} files, {} row groups read, {} segments, peak {} row groups held, {:?}",
        r.files_new, r.row_groups_read, r.segments_written, r.peak_row_groups_held, r.elapsed
    );
    assert_eq!(probe.reads.load(Ordering::SeqCst), r.row_groups_read);
}
