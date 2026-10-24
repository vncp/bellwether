//! The PR #22 review's probes, kept as regression tests. Oracles decode
//! files with the `parquet` crate or recompute from raw text; no planner
//! code.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::needless_pass_by_value,
    clippy::cast_possible_truncation,
    clippy::format_push_string,
    clippy::type_complexity
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, RecordBatch,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether::types::{EvidenceBody, Predicate, Record, ScanPlan};
use bellwether::{spec, Engine, EngineOptions, KeyFile, PlanResult, SourceFormat, WhyRequest};
use bellwether_core::encoding::{encode_tuple_as, encode_value_as, Value};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_size(usize::MAX)
        .build()
}

fn write_groups(path: &Path, schema: Arc<Schema>, groups: Vec<Vec<ArrayRef>>) {
    let mut w =
        ArrowWriter::try_new(File::create(path).unwrap(), schema.clone(), Some(props())).unwrap();
    for cols in groups {
        w.write(&RecordBatch::try_new(schema.clone(), cols).unwrap())
            .unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

fn plan_of(r: &PlanResult) -> &ScanPlan {
    let Record::Evidence(e) = &r.evidence.record else {
        panic!()
    };
    let EvidenceBody::Plan { plan } = &e.body else {
        panic!()
    };
    plan
}

fn planned(r: &PlanResult) -> BTreeSet<(String, u32)> {
    plan_of(r)
        .entries
        .iter()
        .flat_map(|en| {
            let n = Path::new(&en.file.path)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            en.row_groups.iter().map(move |g| (n.clone(), g))
        })
        .collect()
}

/// Every (file, rg) and its decoded batch, sorted by file name.
fn decode(dir: &Path) -> Vec<(String, u32, RecordBatch)> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    names.sort();
    let mut out = Vec::new();
    for p in names {
        let n = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
            .unwrap()
            .metadata()
            .num_row_groups();
        for rg in 0..n {
            let r = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            let s = r.schema();
            let bs: Vec<_> = r.map(|b| b.unwrap()).collect();
            let b = arrow::compute::concat_batches(&s, &bs).unwrap();
            out.push((
                p.file_name().unwrap().to_string_lossy().to_string(),
                rg as u32,
                b,
            ));
        }
    }
    out
}

fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn enc_ts(t: i64) -> Vec<u8> {
    let mut o = Vec::new();
    encode_value_as(
        &Value::Timestamp {
            value: t,
            unit: TimeUnit::Microsecond,
        },
        &ts_type(),
        &mut o,
    )
    .unwrap();
    o
}

fn enc_id(k: i64) -> Vec<u8> {
    encode_tuple_as(&[Value::Int(k)], &[DataType::Int64]).unwrap()
}

/// Versioned corpus with null timestamps and ties.
fn temporal_corpus(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("ts", ts_type(), true),
    ]));
    // (id, ts) per row group, 3 files x 3 rgs.
    let layout: Vec<Vec<Vec<(Option<i64>, Option<i64>)>>> = vec![
        vec![
            vec![(Some(1), Some(10)), (Some(2), Some(10)), (Some(3), None)],
            vec![(Some(1), Some(20)), (Some(4), Some(5)), (None, Some(99))],
            vec![(Some(2), Some(30)), (Some(5), Some(50))],
        ],
        vec![
            vec![(Some(1), Some(40)), (Some(3), None)],
            vec![(Some(2), Some(30)), (Some(4), Some(5))], // tie for 2 and 4
            vec![(Some(5), Some(45)), (Some(6), Some(1))],
        ],
        vec![
            vec![(Some(1), Some(15)), (Some(5), Some(60))],
            vec![(Some(2), Some(25)), (Some(6), Some(-3))],
            vec![(Some(7), Some(100)), (Some(1), Some(39))],
        ],
    ];
    for (f, groups) in layout.into_iter().enumerate() {
        let cols = groups
            .into_iter()
            .map(|g| {
                vec![
                    Arc::new(Int64Array::from(g.iter().map(|r| r.0).collect::<Vec<_>>()))
                        as ArrayRef,
                    Arc::new(
                        TimestampMicrosecondArray::from(g.iter().map(|r| r.1).collect::<Vec<_>>())
                            .with_timezone("UTC"),
                    ),
                ]
            })
            .collect();
        write_groups(&dir.join(format!("p{f}.parquet")), schema.clone(), cols);
    }
}

fn int_keys(path: &Path, name: &str, keys: &[Option<i64>]) {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
}

/// Oracle for latest/asof: per key, the newest non-null ts <= bound, and the
/// row groups holding (key, that ts).
fn temporal_oracle(
    rgs: &[(String, u32, RecordBatch)],
    keys: &[i64],
    at: Option<i64>,
) -> (BTreeMap<i64, i64>, BTreeSet<(String, u32)>) {
    let mut best: BTreeMap<i64, i64> = BTreeMap::new();
    for (_, _, b) in rgs {
        let id = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let ts = b
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            if id.is_null(i) || ts.is_null(i) || !keys.contains(&id.value(i)) {
                continue;
            }
            if at.is_some_and(|a| ts.value(i) > a) {
                continue;
            }
            let e = best.entry(id.value(i)).or_insert(i64::MIN);
            *e = (*e).max(ts.value(i));
        }
    }
    let mut holding = BTreeSet::new();
    for (f, g, b) in rgs {
        let id = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let ts = b
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            if !id.is_null(i) && !ts.is_null(i) && best.get(&id.value(i)) == Some(&ts.value(i)) {
                holding.insert((f.clone(), *g));
            }
        }
    }
    (best, holding)
}

fn residual_parts(
    plan: &ScanPlan,
) -> (
    Vec<Vec<u8>>,
    Option<(Vec<u8>, Vec<u8>)>,
    Vec<(Vec<u8>, Vec<u8>)>,
) {
    let Some(Predicate::And { all }) = &plan.residual else {
        panic!("{:?}", plan.residual)
    };
    let mut inn = vec![];
    let mut between = None;
    let mut bounds = vec![];
    for p in all {
        match p {
            Predicate::In { values, .. } => inn.clone_from(values),
            Predicate::Between { lo, hi, .. } => between = Some((lo.clone(), hi.clone())),
            Predicate::NotOlderThan { bounds: b, .. } => bounds.clone_from(b),
            _ => {}
        }
    }
    (inn, between, bounds)
}

#[test]
fn probe_latest_and_asof_against_a_decoded_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("d");
    temporal_corpus(&data);
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    e.index(&[spec::parse("temporal:id@ts").unwrap()]).unwrap();
    let keys = [1i64, 2, 3, 4, 5, 6, 42];
    let kf = dir.path().join("k.parquet");
    int_keys(
        &kf,
        "id",
        &keys
            .iter()
            .map(|k| Some(*k))
            .chain([None])
            .collect::<Vec<_>>(),
    );
    let rgs = decode(&data);

    let r = e.latest(&KeyFile::new(&kf)).unwrap();
    assert_eq!(r.null_tuples, 1);
    let (best, holding) = temporal_oracle(&rgs, &keys, None);
    let got = planned(&r);
    assert!(
        holding.is_subset(&got),
        "latest misses {:?}",
        holding.difference(&got).collect::<Vec<_>>()
    );
    let (inn, between, bounds) = residual_parts(plan_of(&r));
    assert!(between.is_none());
    let want: Vec<(Vec<u8>, Vec<u8>)> =
        best.iter().map(|(k, t)| (enc_id(*k), enc_ts(*t))).collect();
    let mut b = bounds.clone();
    b.sort();
    let mut w = want.clone();
    w.sort();
    assert_eq!(
        b, w,
        "latest NotOlderThan bounds = index-wide newest per key"
    );
    let mut inn_s = inn.clone();
    inn_s.sort();
    let mut want_in: Vec<_> = best.keys().map(|k| enc_id(*k)).collect();
    want_in.sort();
    assert_eq!(
        inn_s, want_in,
        "In lists only keys that have a version (3 and 42 have none)"
    );

    for at in [0i64, 12, 30, 44, 1000, -1] {
        let r = e.asof(&KeyFile::new(&kf), &at.to_string()).unwrap();
        let (best, holding) = temporal_oracle(&rgs, &keys, Some(at));
        let got = planned(&r);
        assert!(
            holding.is_subset(&got),
            "asof {at} misses {:?}",
            holding.difference(&got).collect::<Vec<_>>()
        );
        let (_inn, between, bounds) = residual_parts(plan_of(&r));
        let want: BTreeSet<(Vec<u8>, Vec<u8>)> =
            best.iter().map(|(k, t)| (enc_id(*k), enc_ts(*t))).collect();
        assert_eq!(
            bounds.into_iter().collect::<BTreeSet<_>>(),
            want,
            "asof {at} bounds"
        );
        if best.is_empty() {
            assert!(between.is_none());
        } else {
            let (lo, hi) = between.expect("asof carries Between");
            assert_eq!(hi, enc_ts(at), "asof {at}: Between hi is the instant");
            assert_eq!(lo, enc_ts(*best.values().min().unwrap()));
        }
    }
    // RFC 3339 instant equals the integer form.
    let a = e
        .asof(&KeyFile::new(&kf), "1970-01-01T00:00:00.000030Z")
        .unwrap();
    let b = e.asof(&KeyFile::new(&kf), "30").unwrap();
    assert_eq!(plan_of(&a).residual, plan_of(&b).residual);
}

#[test]
fn probe_decimals_at_two_file_scales_and_signed_zero() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("d");
    std::fs::create_dir_all(&data).unwrap();
    // File a: Decimal(10,2); file b: Decimal(12,4). Same column `amount`, and a
    // float column `x` with both zeros.
    let sa = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new("x", DataType::Float64, true),
    ]));
    let sb = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Decimal128(12, 4), true),
        Field::new("x", DataType::Float64, true),
    ]));
    let dec = |v: Vec<Option<i128>>, p: u8, s: i8| -> ArrayRef {
        Arc::new(
            Decimal128Array::from(v)
                .with_precision_and_scale(p, s)
                .unwrap(),
        )
    };
    let fl = |v: Vec<Option<f64>>| -> ArrayRef { Arc::new(Float64Array::from(v)) };
    write_groups(
        &data.join("b.parquet"),
        sa,
        vec![
            vec![
                dec(vec![Some(150), Some(300)], 10, 2),
                fl(vec![Some(-0.0), Some(1.0)]),
            ],
            vec![dec(vec![Some(225), None], 10, 2), fl(vec![Some(2.0), None])],
            vec![
                dec(vec![Some(1), Some(2)], 10, 2),
                fl(vec![Some(0.0), Some(3.0)]),
            ],
        ],
    );
    write_groups(
        &data.join("a.parquet"),
        sb,
        vec![
            vec![
                dec(vec![Some(15000), Some(15001)], 12, 4),
                fl(vec![Some(5.0), Some(6.0)]),
            ],
            vec![
                dec(vec![Some(22500), Some(9)], 12, 4),
                fl(vec![Some(7.0), Some(-0.0)]),
            ],
            vec![
                dec(vec![None, Some(1)], 12, 4),
                fl(vec![Some(8.0), Some(9.0)]),
            ],
        ],
    );
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    // Key file at a third scale: Decimal(5,1) 1.5 and 2.2 (2.2 is absent).
    let kschema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(5, 1),
        true,
    )]));
    let kf = dir.path().join("dk.parquet");
    let b = RecordBatch::try_new(
        kschema.clone(),
        vec![dec(vec![Some(15), Some(22), None], 5, 1)],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(&kf).unwrap(), kschema, None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
    // why before any index: floor = oracle
    let why = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    e.index(&[
        spec::parse("exact:amount").unwrap(),
        spec::parse("exact:x").unwrap(),
    ])
    .unwrap();
    let r = e.locate(&KeyFile::new(&kf)).unwrap();
    // oracle: 1.5 at a.rg0 (150@2) and b.rg0 (15000@4)
    let want: BTreeSet<(String, u32)> = [
        ("a.parquet".to_string(), 0u32),
        ("b.parquet".to_string(), 0),
    ]
    .into();
    assert_eq!(planned(&r), want, "decimal 1.5 at scales 1, 2, 4");
    assert_eq!(r.null_tuples, 1);
    assert_eq!(why.candidates[0].proposed.row_groups, 2);
    assert_eq!(
        why.candidates[0].proposed.bytes, r.planned.bytes,
        "why predicted = locate planned"
    );

    let fkf = dir.path().join("fk.parquet");
    let fschema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
    for z in [0.0f64, -0.0] {
        let b = RecordBatch::try_new(fschema.clone(), vec![fl(vec![Some(z)])]).unwrap();
        let mut w =
            ArrowWriter::try_new(File::create(&fkf).unwrap(), fschema.clone(), None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
        let r = e.locate(&KeyFile::new(&fkf)).unwrap();
        let want: BTreeSet<(String, u32)> = [
            ("b.parquet".to_string(), 0u32),
            ("b.parquet".to_string(), 2),
            ("a.parquet".to_string(), 1),
        ]
        .into();
        assert_eq!(
            planned(&r),
            want,
            "x = {z:?} matches both zeros (SQL equality)"
        );
    }
}

fn text_oracle_files(dir: &Path, ext: &str, needle: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.extension().is_some_and(|x| x == ext) {
            let s = std::fs::read_to_string(&p).unwrap();
            if s.lines()
                .any(|l| l.split([',', ':', '}']).any(|t| t.trim() == needle))
            {
                out.insert(p.file_name().unwrap().to_string_lossy().to_string());
            }
        }
    }
    out
}

#[test]
fn probe_text_sources_csv_and_jsonl_and_index_dir_not_discovered() {
    for (fmt, ext) in [
        (
            SourceFormat::Csv {
                delimiter: b',',
                has_header: true,
            },
            "csv",
        ),
        (SourceFormat::JsonLines, "jsonl"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("d");
        std::fs::create_dir_all(&data).unwrap();
        for f in 0..3 {
            let mut s = String::new();
            if ext == "csv" {
                s.push_str("id,v\n");
            }
            for r in 0..40 {
                let id = f * 100 + r % 13;
                if ext == "csv" {
                    s.push_str(&format!("{id},{r}\n"));
                } else {
                    s.push_str(&format!("{{\"id\":{id},\"v\":{r}}}\n"));
                }
            }
            std::fs::write(data.join(format!("f{f}.{ext}")), s).unwrap();
        }
        let e = Engine::open(
            data.to_str().unwrap(),
            EngineOptions {
                format: fmt,
                chunk_size: Some(64),
                ..EngineOptions::default()
            },
        )
        .unwrap();
        let kf = dir.path().join("k.parquet");
        int_keys(&kf, "id", &[Some(105), Some(203)]);
        let why = e
            .why(&WhyRequest {
                keys: Some(KeyFile::new(&kf)),
                ..WhyRequest::default()
            })
            .unwrap();
        let i1 = e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
        assert_eq!(i1.files_new, 3, "{ext}");
        let r = e.locate(&KeyFile::new(&kf)).unwrap();
        let files: BTreeSet<String> = planned(&r).into_iter().map(|(f, _)| f).collect();
        let mut want = text_oracle_files(&data, ext, "105");
        want.extend(text_oracle_files(&data, ext, "203"));
        assert_eq!(files, want, "{ext}: files holding a key");
        assert!(
            r.planned.row_groups < r.row_groups_total,
            "{ext}: chunked plan prunes"
        );
        assert_eq!(
            why.candidates[0].proposed.bytes, r.planned.bytes,
            "{ext}: why = locate"
        );
        // The index directory (ledger .rec JSON, manifest.json, cache .bin)
        // must never be discovered as data.
        let i2 = e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
        assert_eq!(
            (i2.committed, i2.files_new),
            (None, 0),
            "{ext}: second index sees no new files"
        );
    }
}

#[test]
fn probe_duplicate_key_columns_and_odd_inputs_do_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("d");
    temporal_corpus(&data);
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    let kf = dir.path().join("k.parquet");
    int_keys(&kf, "id", &[Some(1)]);
    let dup = KeyFile::new(&kf).with_columns(vec!["id".into(), "id".into()]);
    let err = e.locate(&dup).unwrap_err().to_string();
    assert!(err.contains("appears twice"), "{err}");
    let err = e
        .why(&WhyRequest {
            keys: Some(dup),
            ..WhyRequest::default()
        })
        .unwrap_err()
        .to_string();
    assert!(err.contains("appears twice"), "{err}");
    // An empty key file plans nothing, never a full scan.
    int_keys(&kf, "id", &[]);
    let r = e.locate(&KeyFile::new(&kf)).unwrap();
    assert_eq!((r.keys, r.planned), (0, bellwether::Cost::default()));
    let r = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    assert_eq!(
        r.candidates.len(),
        0,
        "the floor exact:id is already current"
    );
    // asof without a temporal spec says what to do.
    let err = e.asof(&KeyFile::new(&kf), "5").unwrap_err().to_string();
    assert!(err.contains("bellwether index"), "{err}");
    // index location as a file:// URL and a relative path
    let idx = dir.path().join("elsewhere");
    let e2 = Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            index: Some(format!("file://{}", idx.display())),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let r = e2.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    assert_eq!(r.sequence, 1);
    assert!(idx.join("manifest.json").exists());
    let e3 = Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            index: Some("memory://".into()),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let r = e3.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    assert_eq!(r.sequence, 1);
    int_keys(&kf, "id", &[Some(1)]);
    let r = e3.locate(&KeyFile::new(&kf)).unwrap();
    assert!(r.planned.row_groups > 0);
}

#[test]
fn probe_why_over_unrepresentable_mixed_scales() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("d");
    std::fs::create_dir_all(&data).unwrap();
    let s2 = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(10, 2),
        true,
    )]));
    let s4 = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(12, 4),
        true,
    )]));
    let dec = |v: Vec<Option<i128>>, p: u8, s: i8| -> ArrayRef {
        Arc::new(
            Decimal128Array::from(v)
                .with_precision_and_scale(p, s)
                .unwrap(),
        )
    };
    write_groups(
        &data.join("a.parquet"),
        s2,
        vec![vec![dec(vec![Some(150)], 10, 2)]],
    );
    write_groups(
        &data.join("b.parquet"),
        s4,
        vec![
            vec![dec(vec![Some(15001)], 12, 4)],
            vec![dec(vec![Some(15000)], 12, 4)],
        ],
    );
    let e = Engine::open(data.to_str().unwrap(), EngineOptions::default()).unwrap();
    let kschema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(10, 2),
        true,
    )]));
    let kf = dir.path().join("k.parquet");
    let b = RecordBatch::try_new(kschema.clone(), vec![dec(vec![Some(150)], 10, 2)]).unwrap();
    let mut w = ArrowWriter::try_new(File::create(&kf).unwrap(), kschema, None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
    // Inferred from the first file (scale 2), 1.5001 does not fit: refused,
    // and the error says how to supply the schema.
    let why = e.why(&WhyRequest {
        keys: Some(KeyFile::new(&kf)),
        ..WhyRequest::default()
    });
    let err = why.unwrap_err().to_string();
    assert!(err.contains("--schema"), "{err}");
    let err = e
        .index(&[spec::parse("exact:amount").unwrap()])
        .unwrap_err()
        .to_string();
    assert!(err.contains("--schema"), "{err}");
    // With the dataset schema supplied at scale 4, both work and agree.
    let schema = bellwether::schema_from_json(
        r#"[{"name": "amount", "type": "Decimal128(12, 4)", "nullable": true}]"#,
    )
    .unwrap();
    let e = Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            schema: Some(schema),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let why = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    assert_eq!(why.dataset.row_groups, 3);
    e.index(&[spec::parse("exact:amount").unwrap()]).unwrap();
    let r = e.locate(&KeyFile::new(&kf)).unwrap();
    // 1.50 lives in a.rg0 (150 at scale 2) and b.rg1 (15000 at scale 4).
    let want: BTreeSet<(String, u32)> = [
        ("a.parquet".to_string(), 0u32),
        ("b.parquet".to_string(), 1),
    ]
    .into();
    assert_eq!(planned(&r), want);
    assert_eq!(
        why.candidates[0].proposed, r.planned,
        "why predicted = locate planned"
    );
}
