//! The facade's index, lookup, propose, receipt, and report operations
//! against corpora written directly with the `parquet` crate.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeSet;

use bellwether::types::{
    EvidenceBody, Hash, KeySpec, Predicate, ProposalBody, Record, RewriteShape,
};
use bellwether::{spec, Engine, EngineOptions, KeyFile, LookupKind, Written};
use common::{ledger_bytes, ledger_corpus, oracle, write_keys};

fn open(root: &std::path::Path) -> Engine {
    Engine::open(root.to_str().unwrap(), EngineOptions::default()).unwrap()
}

/// The record bytes the facade returned are exactly the ledger's file.
fn assert_stored(index: &std::path::Path, w: &Written) {
    let bytes = serde_json::to_vec(&w.record).unwrap();
    assert_eq!(Hash::of(&bytes), w.hash);
    assert_eq!(ledger_bytes(index, &w.hash.to_hex()), bytes);
}

fn planned(w: &Written) -> BTreeSet<(String, usize)> {
    let Record::Evidence(e) = &w.record else {
        panic!("not evidence")
    };
    let EvidenceBody::Plan { plan } = &e.body else {
        panic!("not a plan")
    };
    plan.entries
        .iter()
        .flat_map(|en| {
            let name = std::path::Path::new(&en.file.path)
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

#[test]
fn open_canonicalizes_and_refuses_remote_data_roots() {
    let dir = tempfile::tempdir().unwrap();
    ledger_corpus(&dir.path().join("d"), 1, 1, 10, 5);
    let e = open(&dir.path().join("d").join("..").join("d"));
    assert_eq!(
        e.root(),
        std::fs::canonicalize(dir.path().join("d"))
            .unwrap()
            .to_str()
            .unwrap()
    );
    let err = Engine::open("s3://bucket/data", EngineOptions::default()).unwrap_err();
    assert!(err.to_string().contains("local files only"), "{err}");
    let err = Engine::open(
        dir.path().join("missing").to_str().unwrap(),
        EngineOptions::default(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("pass an existing directory"),
        "{err}"
    );
}

#[test]
fn locate_plans_every_row_group_holding_a_key_and_records_the_plan() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // 300 rows, ids k % 1000: each id is in exactly one row group, so a key
    // set selects a strict subset and a raw scan would fail the oracle.
    ledger_corpus(&data, 3, 4, 25, 1_000);
    let e = open(&data);
    let err = e
        .locate(&KeyFile::new(dir.path().join("k.parquet")))
        .unwrap_err();
    assert!(err.to_string().contains("bellwether index"), "{err}");

    let idx = e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    assert_eq!(idx.committed, Some(1));
    assert_eq!(idx.specs[0].complete_at, Some(1));
    let again = e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    assert_eq!(again.committed, None, "nothing new to index");

    let keys = [3i64, 17, 139, 260, 1_000];
    write_keys(&dir.path().join("k.parquet"), "id", &keys);
    let r = e
        .locate(&KeyFile::new(dir.path().join("k.parquet")))
        .unwrap();
    assert_eq!(r.keys, 5);
    assert_eq!(r.null_tuples, 0);
    assert_eq!(r.sequence, 1);
    let want = oracle(&data, &keys);
    assert_eq!(
        want.len(),
        3,
        "keys 3 and 17 share a row group; 1000 is absent"
    );
    assert_eq!(planned(&r.evidence), want);
    assert_eq!(r.row_groups_total, 12);
    assert_eq!(r.files_total, 3);
    // The typed summary is the plan Evidence's numbers.
    let Record::Evidence(ev) = &r.evidence.record else {
        unreachable!()
    };
    let EvidenceBody::Plan { plan } = &ev.body else {
        unreachable!()
    };
    assert_eq!(r.planned.files, plan.entries.len() as u64);
    assert_eq!(r.planned.row_groups, plan.row_groups_selected);
    assert_eq!(r.planned.bytes, plan.bytes);
    assert_eq!(
        plan.bytes,
        plan.entries.iter().map(|e| e.bytes).sum::<u64>()
    );
    assert_eq!((r.planned.files, r.planned.row_groups), (3, 3));
    assert!(r.planned.bytes > 0);
    let index = data.join(".bellwether");
    assert_stored(&index, &r.evidence);
    let Record::Evidence(ev) = &r.evidence.record else {
        unreachable!()
    };
    assert_eq!(ev.provenance.manifest_version.as_deref(), Some("1"));
    assert_eq!(ev.provenance.inputs, vec![idx.manifest, r.key_set]);
    assert!(
        index
            .join("cache")
            .join(format!("{}.bin", r.key_set.to_hex()))
            .exists(),
        "the cited key set resolves in the cache"
    );
}

#[test]
fn latest_and_asof_carry_the_invariant_5_residual_and_convert_instants_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 3, 20, 7);
    let e = open(&data);
    e.index(&[spec::parse("temporal:id@ts").unwrap()]).unwrap();
    write_keys(&dir.path().join("k.parquet"), "id", &[2, 5]);
    let keys = KeyFile::new(dir.path().join("k.parquet"));
    for r in [
        e.latest(&keys).unwrap(),
        e.asof(&keys, "1970-01-01T00:00:30Z").unwrap(),
    ] {
        let Record::Evidence(ev) = &r.evidence.record else {
            unreachable!()
        };
        let EvidenceBody::Plan { plan } = &ev.body else {
            unreachable!()
        };
        let Some(Predicate::And { all }) = &plan.residual else {
            panic!("temporal residual is an And: {:?}", plan.residual)
        };
        let n = all
            .iter()
            .position(|p| matches!(p, Predicate::NotOlderThan { .. }))
            .unwrap();
        let l = all
            .iter()
            .position(|p| matches!(p, Predicate::LatestPerKey { .. }))
            .unwrap();
        assert!(n < l, "NotOlderThan applies before LatestPerKey");
    }
    let r = e.asof(&keys, "1970-01-01T00:00:30Z").unwrap();
    assert_eq!(
        r.lookup,
        LookupKind::Asof {
            at: "1970-01-01T00:00:30Z".into(),
            ts: 30_000_000,
            ts_column: "ts".into()
        }
    );
    let err = e.asof(&keys, "1970-01-01T00:00:30.0000001Z").unwrap_err();
    assert!(err.to_string().contains("finer than"), "{err}");
    let err = e.latest(&keys.clone().with_columns(vec!["batch".into()]));
    assert!(err.is_err());
}

#[test]
fn propose_writes_a_scoreless_proposal_citing_resolving_evidence_only() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 2, 10, 5);
    let e = open(&data);
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    write_keys(&dir.path().join("k.parquet"), "id", &[1]);
    let ev = e
        .locate(&KeyFile::new(dir.path().join("k.parquet")))
        .unwrap()
        .evidence
        .hash;
    let key = ProposalBody::Key {
        spec: KeySpec::Exact {
            columns: vec!["batch".into()],
        },
    };
    assert!(e.propose(key.clone(), &[], "agent").is_err(), "no evidence");
    let missing = Hash::of(b"nothing");
    let err = e.propose(key.clone(), &[missing], "agent").unwrap_err();
    assert!(err.to_string().contains("not in the ledger"), "{err}");
    let unknown = ProposalBody::Key {
        spec: KeySpec::Exact {
            columns: vec!["nope".into()],
        },
    };
    assert!(e.propose(unknown, &[ev], "agent").is_err());
    let empty = ProposalBody::Rewrite {
        shape: RewriteShape {
            before: "a".into(),
            after: " ".into(),
            invariant: "c".into(),
        },
    };
    assert!(e.propose(empty, &[ev], "agent").is_err());

    let p = e.propose(key, &[ev], "agent").unwrap();
    let written = p.proposal().unwrap();
    assert_stored(&data.join(".bellwether"), written);
    // Citing a proposal as evidence is refused by the facade itself, before
    // the ledger, with what to do.
    let err = e
        .propose(
            ProposalBody::Layout {
                cluster_by: vec!["ts".into()],
            },
            &[written.hash],
            "agent",
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("not an evidence record"), "{err}");
    let json = serde_json::to_value(&p).unwrap().to_string();
    for word in ["score", "estimate", "confidence", "cost"] {
        assert!(!json.contains(word), "{word} in {json}");
    }
}

#[test]
fn receipt_refuses_a_hash_that_is_not_a_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 1, 10, 5);
    let e = open(&data);
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    write_keys(&dir.path().join("k.parquet"), "id", &[1]);
    let ev = e
        .locate(&KeyFile::new(dir.path().join("k.parquet")))
        .unwrap()
        .evidence
        .hash;
    assert!(e.receipt(&ev).is_err());
    let err = e.receipt(&Hash::of(b"absent")).unwrap_err();
    assert!(err.to_string().contains("not in the ledger"), "{err}");
}

#[test]
fn report_is_one_self_contained_html_file() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 1, 1, 10, 5);
    let e = open(&data);
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let r = e.report(&out).unwrap();
    let html = std::fs::read_to_string(out.join("report.html")).unwrap();
    assert_eq!(r.bytes, html.len() as u64);
    assert_eq!(r.blake3, Hash::of(html.as_bytes()));
    let lower = html.to_lowercase();
    for banned in ["<script", "src=", "href=", "@import", "url("] {
        assert!(!lower.contains(banned), "{banned}");
    }
}

#[test]
fn ordered_plans_order_entries_by_timestamp_range_key_against_decoded_files() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // Write three files whose alphabetical path order is a.parquet, b.parquet, c.parquet,
    // but whose timestamp minima are:
    //   a.parquet: ts 300
    //   b.parquet: ts 100
    //   c.parquet: ts 200
    common::write_file(
        &data.join("a.parquet"),
        &[vec![(1, 0, Some(300), "a".into())]],
    );
    common::write_file(
        &data.join("b.parquet"),
        &[vec![(1, 0, Some(100), "b".into())]],
    );
    common::write_file(
        &data.join("c.parquet"),
        &[vec![(1, 0, Some(200), "c".into())]],
    );
    write_keys(&dir.path().join("k.parquet"), "id", &[1]);
    let kf = KeyFile::new(dir.path().join("k.parquet"));

    let e = open(&data);
    // Without a timestamp range key, order is unspecified (entries in path order) and said so.
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    let r_unord = e.locate(&kf).unwrap();
    assert_eq!(r_unord.ordered_by, None);
    let Record::Evidence(ev1) = &r_unord.evidence.record else {
        unreachable!()
    };
    let EvidenceBody::Plan { plan: p1 } = &ev1.body else {
        unreachable!()
    };
    let names1: Vec<&str> = p1
        .entries
        .iter()
        .map(|en| {
            std::path::Path::new(&en.file.path)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
        })
        .collect();
    assert_eq!(names1, vec!["a.parquet", "b.parquet", "c.parquet"]);

    // Add range:ts -> plan entries are ordered by row-group minimum of ts (b, c, a) and ordered_by is Some("ts").
    e.index(&[spec::parse("range:ts").unwrap()]).unwrap();
    let r_ord = e.locate(&kf).unwrap();
    assert_eq!(r_ord.ordered_by.as_deref(), Some("ts"));
    let Record::Evidence(ev2) = &r_ord.evidence.record else {
        unreachable!()
    };
    let EvidenceBody::Plan { plan: p2 } = &ev2.body else {
        unreachable!()
    };
    let names2: Vec<&str> = p2
        .entries
        .iter()
        .map(|en| {
            std::path::Path::new(&en.file.path)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
        })
        .collect();
    assert_eq!(names2, vec!["b.parquet", "c.parquet", "a.parquet"]);

    // Independently decode the planned files in plan order with the parquet crate and verify
    // their row-group minimum timestamps are strictly non-decreasing (100, 200, 300).
    let mut decoded_mins = Vec::new();
    for entry in &p2.entries {
        let file = std::fs::File::open(&entry.file.path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_row_groups(entry.row_groups.iter().map(|i| i as usize).collect())
            .build()
            .unwrap();
        let mut min_ts = i64::MAX;
        for batch in reader {
            let b = batch.unwrap();
            let ts_arr = b
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .unwrap();
            for i in 0..ts_arr.len() {
                min_ts = min_ts.min(ts_arr.value(i));
            }
        }
        decoded_mins.push(min_ts);
    }
    assert_eq!(decoded_mins, vec![100, 200, 300]);
}

#[test]
fn pinned_lookup_at_manifest_returns_earlier_plan_byte_for_byte_and_refuses_gc_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    common::write_file(
        &data.join("part-0.parquet"),
        &[vec![
            (1, 0, Some(100), "v1".into()),
            (2, 0, Some(100), "v1".into()),
        ]],
    );
    write_keys(&dir.path().join("k.parquet"), "id", &[1]);
    let kf = KeyFile::new(dir.path().join("k.parquet"));

    let fixed_clock = "2026-10-25T12:00:00Z".to_string();
    let opts_v1 = EngineOptions {
        clock: Some(fixed_clock.clone()),
        ..EngineOptions::default()
    };
    let e1 = Engine::open(data.to_str().unwrap(), opts_v1).unwrap();
    let idx1 = e1
        .index(&[
            spec::parse("exact:id").unwrap(),
            spec::parse("temporal:id@ts").unwrap(),
        ])
        .unwrap();
    assert_eq!(idx1.sequence, 1);

    let locate_v1 = e1.locate(&kf).unwrap();
    let latest_v1 = e1.latest(&kf).unwrap();
    let asof_v1 = e1.asof(&kf, "200").unwrap();
    let why_v1 = e1
        .why(&bellwether::WhyRequest {
            keys: Some(kf.clone()),
            ..bellwether::WhyRequest::default()
        })
        .unwrap();
    let locate_v1_bytes = serde_json::to_vec(&locate_v1).unwrap();
    let latest_v1_bytes = serde_json::to_vec(&latest_v1).unwrap();
    let asof_v1_bytes = serde_json::to_vec(&asof_v1).unwrap();
    let why_v1_bytes = serde_json::to_vec(&why_v1).unwrap();

    // Add a second file holding key 1 with a newer timestamp and rebuild -> sequence 2.
    common::write_file(
        &data.join("part-1.parquet"),
        &[vec![(1, 0, Some(500), "v2".into())]],
    );
    let idx2 = e1
        .index(&[
            spec::parse("exact:id").unwrap(),
            spec::parse("temporal:id@ts").unwrap(),
        ])
        .unwrap();
    assert_eq!(idx2.sequence, 2);

    // Unpinned locate at sequence 2 plans both files.
    let locate_v2 = e1.locate(&kf).unwrap();
    assert_eq!(locate_v2.sequence, 2);
    assert_eq!(locate_v2.planned.files, 2);

    // Pinned lookup at sequence 1 returns the earlier plan byte for byte!
    let e_pinned = Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            at_manifest: Some(1),
            clock: Some(fixed_clock),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    assert_eq!(
        serde_json::to_vec(&e_pinned.locate(&kf).unwrap()).unwrap(),
        locate_v1_bytes
    );
    assert_eq!(
        serde_json::to_vec(&e_pinned.latest(&kf).unwrap()).unwrap(),
        latest_v1_bytes
    );
    assert_eq!(
        serde_json::to_vec(&e_pinned.asof(&kf, "200").unwrap()).unwrap(),
        asof_v1_bytes
    );
    assert_eq!(
        serde_json::to_vec(
            &e_pinned
                .why(&bellwether::WhyRequest {
                    keys: Some(kf.clone()),
                    at_manifest: Some(1),
                    ..bellwether::WhyRequest::default()
                })
                .unwrap()
        )
        .unwrap(),
        why_v1_bytes
    );

    // Now simulate GC reclaiming a segment of sequence 1 after compaction at sequence 3:
    // remove a segment file referenced only by sequence 1, or remove sequence 1's archived manifest,
    // and verify that pinning sequence 1 refuses and names sequence 2 as the oldest available.
    let manifest_seq1 = data
        .join(".bellwether")
        .join("manifests")
        .join(format!("{:020}.json", 1));
    std::fs::remove_file(&manifest_seq1).unwrap();
    let err = e_pinned.locate(&kf).unwrap_err().to_string();
    assert!(
        err.contains("oldest available") && err.contains('2'),
        "expected refusal naming oldest available sequence 2, got: {err}"
    );
}

#[test]
fn bounds_sidecar_and_lookback_windows_prune_against_decoded_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // Write 3 row groups in part-0.parquet and 1 row group in part-1.parquet.
    // Key 1 has rows straddling row groups 0 and 1 (ts 100..=150 in rg 0, ts 160..=200 in rg 1),
    // and a sentinel timestamp (year 9999 in microseconds: 253_402_300_799_000_000) in rg 2.
    // Key 2 has rows in rg 0 at ts 500 (so rg 0's global ts range is 100..=500, overlapping 400..=600,
    // even though key 1's own range in rg 0 is only 100..=150!).
    let sentinel_ts = 253_402_300_799_000_000i64;
    common::write_file(
        &data.join("part-0.parquet"),
        &[
            vec![
                (1, 0, Some(100), "k1-rg0-a".into()),
                (1, 0, Some(150), "k1-rg0-b".into()),
                (2, 0, Some(500), "k2-rg0".into()),
            ],
            vec![
                (1, 0, Some(160), "k1-rg1-a".into()),
                (1, 0, Some(200), "k1-rg1-b".into()),
            ],
            vec![(1, 0, Some(sentinel_ts), "k1-sentinel".into())],
        ],
    );
    common::write_file(
        &data.join("part-1.parquet"),
        &[vec![(1, 0, Some(450), "k1-p1-rg0".into())]],
    );
    write_keys(&dir.path().join("k1.parquet"), "id", &[1]);
    let kf1 = KeyFile::new(dir.path().join("k1.parquet"));

    let e = open(&data);
    // Index exact:id@ts (which writes both exact:id and bounds:id) and range:ts.
    let specs = [
        spec::parse_many("exact:id@ts").unwrap(),
        vec![spec::parse("range:ts").unwrap()],
    ]
    .concat();
    e.index(&specs).unwrap();

    // Query window 400..600 for key 1:
    // - rg 0 of part-0 has global ts 100..=500 (because of key 2 at 500), so `range:ts` alone
    //   could NOT prune rg 0 of part-0!
    // - `bounds:id` knows key 1's own ts range in rg 0 of part-0 is 100..=150, which does NOT
    //   overlap 400..=600, so rg 0 of part-0 is pruned!
    // - Only rg 0 of part-1 (where key 1 has ts 450) is planned.
    let res = e.locate_window(&kf1, "400..600").unwrap();
    assert_eq!(res.planned.files, 1);
    assert_eq!(res.planned.row_groups, 1);
    assert_eq!(
        planned(&res.evidence),
        BTreeSet::from([("part-1.parquet".to_string(), 0usize)])
    );

    // Query window 140..170 for key 1 (straddling rg 0 and rg 1 of part-0):
    let res_straddle = e.locate_window(&kf1, "140..170").unwrap();
    assert_eq!(
        planned(&res_straddle.evidence),
        BTreeSet::from([
            ("part-0.parquet".to_string(), 0usize),
            ("part-0.parquet".to_string(), 1usize),
        ])
    );

    // Query sentinel window for key 1:
    let res_sentinel = e
        .locate_window(&kf1, &format!("{sentinel_ts}..{sentinel_ts}"))
        .unwrap();
    assert_eq!(
        planned(&res_sentinel.evidence),
        BTreeSet::from([("part-0.parquet".to_string(), 2usize)])
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn bitemporal_temporal2_handles_restatements_and_late_arriving_rows_against_oracle() {
    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("bitemporal");
    std::fs::create_dir_all(&data).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "valid_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "known_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("val", DataType::Utf8, false),
    ]));
    let write_bi = |path: &std::path::Path, rows: &[(i64, Option<i64>, Option<i64>, &str)]| {
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(
                    TimestampMicrosecondArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
                        .with_timezone("UTC"),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut w = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            Arc::clone(&schema),
            None,
        )
        .unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
    };

    // File 0 (initial prints):
    //   id=1, valid=100, known=105 ("v100-initial")
    //   id=1, valid=200, known=205 ("v200-initial")
    // File 1 (restatement of valid=200 known at 300, plus a late print for valid=150 known at 250,
    //         plus null-clock rows that must never win):
    //   id=1, valid=200, known=300 ("v200-restated")
    //   id=1, valid=150, known=250 ("v150-late")
    //   id=1, valid=None, known=Some(110) ("null-valid")
    //   id=1, valid=Some(210), known=None ("null-known")
    write_bi(
        &data.join("part-0.parquet"),
        &[
            (1, Some(100), Some(105), "v100-initial"),
            (1, Some(200), Some(205), "v200-initial"),
        ],
    );
    write_bi(
        &data.join("part-1.parquet"),
        &[
            (1, Some(200), Some(300), "v200-restated"),
            (1, Some(150), Some(250), "v150-late"),
            (1, None, Some(110), "null-valid"),
            (1, Some(210), None, "null-known"),
        ],
    );
    write_keys(&dir.path().join("k1.parquet"), "id", &[1]);
    let kf1 = KeyFile::new(dir.path().join("k1.parquet"));

    let e = open(&data);
    e.index(&[spec::parse("temporal2:id@valid_ts,known_ts").unwrap()])
        .unwrap();

    // 1. At valid=180, known=220: the late print (valid=150, known=250) was NOT known yet at 220!
    //    So the winner at (valid<=180, known<=220) is valid=100, known=105 in part-0.parquet.
    let r1 = e
        .asof_with(&kf1, None, Some("180"), Some("220"), None)
        .unwrap();
    assert_eq!(
        planned(&r1.evidence),
        BTreeSet::from([("part-0.parquet".to_string(), 0usize)])
    );

    // 2. At valid=180, known=260: now the late print (valid=150, known=250) IS known, and 150 > 100!
    //    So the winner is valid=150, known=250 in part-1.parquet.
    let r2 = e
        .asof_with(&kf1, None, Some("180"), Some("260"), None)
        .unwrap();
    assert_eq!(
        planned(&r2.evidence),
        BTreeSet::from([("part-1.parquet".to_string(), 0usize)])
    );

    // 3. At valid=250, known=260: valid=210 has known=None (null never wins), and valid=200 has
    //    known=205 in part-0 (<=260) and known=300 in part-1 (>260), so only part-0 is planned.
    let r3 = e
        .asof_with(&kf1, None, Some("250"), Some("260"), None)
        .unwrap();
    assert_eq!(
        planned(&r3.evidence),
        BTreeSet::from([("part-0.parquet".to_string(), 0usize)])
    );

    // 4. `latest --known 350`: valid=200 is the newest valid time with known<=350, and both
    //    part-0 (known=205) and part-1 (known=300) hold valid=200 with known<=350 (ties on valid
    //    are kept so consumer can pick the restatement).
    let r4 = e.latest_with(&kf1, Some("350"), None).unwrap();
    assert_eq!(
        planned(&r4.evidence),
        BTreeSet::from([
            ("part-0.parquet".to_string(), 0usize),
            ("part-1.parquet".to_string(), 0usize),
        ])
    );
}
