//! `why` on folders with no index, with and without a workload, and over a
//! current index that does not serve the workload.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use bellwether::types::{EvidenceBody, Hash, KeySpec, ProposalBody, Record};
use bellwether::{
    spec, CandidateOrigin, CulpritReason, Engine, EngineOptions, FixOutcome, FixRequest, KeyFile,
    LookupKind, WhyRequest, Written, ENGINE_PROPOSER,
};
use common::{ledger_bytes, ledger_corpus, oracle, scattered_corpus, write_keys};

fn open(root: &std::path::Path) -> Engine {
    Engine::open(root.to_str().unwrap(), EngineOptions::default()).unwrap()
}

fn assert_stored(index: &std::path::Path, w: &Written) {
    let bytes = serde_json::to_vec(&w.record).unwrap();
    assert_eq!(Hash::of(&bytes), w.hash);
    assert_eq!(ledger_bytes(index, &w.hash.to_hex()), bytes);
}

/// Compressed row-group bytes of every file under `dir`, from the footers.
fn footer_bytes(dir: &std::path::Path) -> u64 {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let mut total = 0u64;
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.extension().is_some_and(|x| x == "parquet") {
            let r = SerializedFileReader::new(std::fs::File::open(&p).unwrap()).unwrap();
            for rg in r.metadata().row_groups() {
                total += u64::try_from(rg.compressed_size()).unwrap();
            }
        }
    }
    total
}

#[test]
fn why_reports_an_unindexed_folder_from_its_footers_with_no_workload() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 3, 30, 40);
    let r = open(&data).why(&WhyRequest::default()).unwrap();
    assert_eq!(r.sequence, 0);
    assert!(r.workload.is_none() && r.current.is_none() && r.candidates.is_empty());
    assert!(r.recommended.is_none() && r.culprit.is_none());
    assert_eq!(
        (r.dataset.files, r.dataset.row_groups, r.dataset.rows),
        (2, 6, 180)
    );
    assert_eq!(r.dataset.bytes, footer_bytes(&data));
    // The profile is computed and its Evidence recorded: the only record.
    assert!(r.evidence.recorded && r.evidence.note.is_none());
    assert!(matches!(
        r.evidence.evidence.body,
        EvidenceBody::Profile { .. }
    ));
    assert_eq!(r.evidence.evidence.provenance.manifest_version, None);
    assert_ne!(r.evidence.evidence.provenance.inputs.len(), 0);
    let bytes = serde_json::to_vec(&Record::Evidence(r.evidence.evidence.clone())).unwrap();
    assert_eq!(Some(Hash::of(&bytes)), r.evidence.hash);
    assert_eq!(
        ledger_bytes(
            &data.join(".bellwether"),
            &r.evidence.hash.unwrap().to_hex()
        ),
        bytes
    );
    assert_eq!(
        std::fs::read_dir(data.join(".bellwether").join("ledger"))
            .unwrap()
            .count(),
        1
    );
    assert_eq!(r.profile.files_total, r.dataset.files);
    assert_eq!(r.profile.row_groups_total, r.dataset.row_groups);
    // The report round-trips through its JSON exactly (it is the --json
    // output and the MCP result).
    let json = serde_json::to_string(&r).unwrap();
    let back: bellwether::WhyReport = serde_json::from_str(&json).unwrap();
    assert_eq!(serde_json::to_string(&back).unwrap(), json);
}

#[test]
fn why_with_keys_replays_each_candidate_and_the_floor_is_the_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    scattered_corpus(&data, 3, 4, 25, 400);
    let keys = [5i64, 77, 123];
    write_keys(&dir.path().join("k.parquet"), "id", &keys);
    let request = WhyRequest {
        keys: Some(KeyFile::new(dir.path().join("k.parquet"))),
        ..WhyRequest::default()
    };
    let r = open(&data).why(&request).unwrap();
    let index = data.join(".bellwether");
    let w = r.workload.as_ref().unwrap();
    assert_eq!(w.runs, 1);
    assert_eq!(w.columns, vec![vec!["id".to_string()]]);
    let current = r.current.unwrap();
    assert_eq!(current.row_groups, 12, "no index: every row group");
    assert_eq!(current.bytes, r.dataset.bytes);
    // Only the floor: exact:id,batch and exact:id,payload serve an id lookup
    // only by prefix, so they select at least the floor's row groups and are
    // not replayed; the rest serve no run.
    let floor = &r.candidates[0];
    let names = |v: &mut dyn Iterator<Item = &KeySpec>| v.map(KeySpec::name).collect::<Vec<_>>();
    assert_eq!(
        names(&mut r.candidates.iter().map(|c| &c.spec)),
        ["exact:id"]
    );
    assert_eq!(
        names(&mut r.not_serving.iter()),
        ["exact:batch", "exact:payload", "exact:batch,payload"]
    );
    assert_eq!(floor.origin, CandidateOrigin::Workload);
    assert_eq!(
        floor.spec,
        KeySpec::Exact {
            columns: vec!["id".into()],
            counts: false,
            bounds: None,
        }
    );
    assert_eq!(floor.proposed.row_groups, oracle(&data, &keys).len() as u64);
    for c in &r.candidates {
        assert_stored(&index, &c.proposal);
        assert_stored(&index, &c.replay);
        let Record::Proposal(p) = &c.proposal.record else {
            panic!()
        };
        assert_eq!(p.proposed_by, ENGINE_PROPOSER);
        assert_eq!(
            p.evidence,
            vec![r.evidence.hash.unwrap()],
            "cites the profile"
        );
        assert!(matches!(&p.body, ProposalBody::Key { spec, .. } if *spec == c.spec));
        let Record::Replay(rp) = &c.replay.record else {
            panic!()
        };
        assert_eq!(rp.proposal, c.proposal.hash);
        assert_eq!(rp.workload, w.hash);
        assert_eq!(rp.manifest_version, "0");
        assert_ne!(rp.inputs.len(), 0, "contract v2.1: inputs");
        assert_eq!(rp.model, None, "a key replay is exact and names no model");
        assert!(rp.current.iter().chain(&rp.proposed).all(|c| c.exact));
    }
    let rec = r.recommended.as_ref().unwrap();
    assert_eq!(rec.spec, floor.spec);
    assert!(rec.proposed.bytes < rec.current.bytes);
    let culprit = r.culprit.as_ref().unwrap();
    assert_eq!(culprit.columns, vec!["id".to_string()]);
    assert_eq!(culprit.reason, CulpritReason::NoIndexServes);
}

#[test]
fn why_over_an_index_that_does_not_serve_the_workload() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 4, 25, 300);
    let e = open(&data);
    e.index(&[spec::parse("exact:batch").unwrap()]).unwrap();
    write_keys(&dir.path().join("k.parquet"), "id", &[1, 2]);
    let r = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(dir.path().join("k.parquet"))),
            ..WhyRequest::default()
        })
        .unwrap();
    assert_eq!(r.sequence, 1);
    assert_eq!(r.current_specs.len(), 1);
    // exact:batch cannot narrow id lookups; the files' own min/max
    // statistics can (ids are sorted: 1 and 2 sit in the first row group),
    // and an indexed folder's replay prices the unserved run with them too.
    assert_eq!(r.current.unwrap().row_groups, 1);
    assert!(r.full_scan.is_none(), "a key spec is current");
    for c in &r.candidates {
        let Record::Replay(rp) = &c.replay.record else {
            panic!()
        };
        assert_eq!(rp.manifest_version, "1");
        assert_eq!(rp.unserved.as_deref(), Some("parquet_stats"));
    }
    assert_eq!(r.culprit.unwrap().reason, CulpritReason::NoIndexServes);

    // Once the floor is indexed it is current, not a candidate, and serves.
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    let r = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(dir.path().join("k.parquet"))),
            ..WhyRequest::default()
        })
        .unwrap();
    // No floor candidate: exact:id is current. The profile's composites
    // still replay, and none beats the serving key.
    assert!(r
        .candidates
        .iter()
        .all(|c| c.origin == CandidateOrigin::Profile));
    assert!(r.recommended.is_none());
    assert!(matches!(
        r.culprit.unwrap().reason,
        CulpritReason::ServingSpec { .. }
    ));
}

/// Records `runs` (run id, key column, keys) into the engine's cache as one
/// workload of `locate` runs and returns its hash.
fn record_workload(data: &std::path::Path, runs: &[(&str, &str, Vec<i64>)]) -> Hash {
    let runs: Vec<_> = runs
        .iter()
        .map(|(id, c, k)| (*id, *c, k.clone(), bellwether_replay::RunQuery::Locate))
        .collect();
    record_queries(data, &runs)
}

/// Records `runs` (run id, key column, keys, query) into the engine's
/// cache as one workload and returns its hash.
fn record_queries(
    data: &std::path::Path,
    runs: &[(&str, &str, Vec<i64>, bellwether_replay::RunQuery)],
) -> Hash {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bellwether::types::Source;
    use bellwether_replay::{KeyInput, WorkloadRecorder};
    use bellwether_store::{Cache, ObjectStoreBackend};

    let index = data.join(".bellwether");
    let store = Arc::new(ObjectStoreBackend::from_url(index.to_str().unwrap()).unwrap());
    let cache = Cache::new(store, "");
    let root = std::fs::canonicalize(data).unwrap();
    let schema = bellwether_source::ParquetSource::new()
        .schema(root.to_str().unwrap())
        .unwrap();
    let mut rec = WorkloadRecorder::new(&cache, schema);
    for (id, column, keys, query) in runs {
        let s = Arc::new(Schema::new(vec![Field::new(
            *column,
            DataType::Int64,
            false,
        )]));
        let b = RecordBatch::try_new(
            s,
            vec![Arc::new(Int64Array::from(keys.clone())) as ArrayRef],
        )
        .unwrap();
        rec.record_query(id, KeyInput::Batch(&b), query.clone(), None)
            .unwrap();
    }
    rec.finish().unwrap().hash()
}

/// Count runs ahead of their locates, over an index that serves the key
/// without counts: `why` prices `exact:<cols>,counts` (each count then
/// reads nothing) and recommends it, since it reads strictly less than the
/// current configuration; once it is current it is no longer a candidate.
#[test]
fn why_prices_and_recommends_counts_for_count_runs() {
    use bellwether_replay::RunQuery::{Count, Locate};
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 3, 4, 25, 1_000);
    let e = open(&data);
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    let runs = [
        ("c1", "id", vec![5, 260, 700], Count),
        ("l1", "id", vec![5, 260, 700], Locate),
        ("c2", "id", vec![30, 999], Count),
        ("l2", "id", vec![30, 999], Locate),
    ];
    let hash = record_queries(&data, &runs);
    let r = e
        .why(&WhyRequest {
            workload: Some(hash),
            max_candidates: 0,
            ..WhyRequest::default()
        })
        .unwrap();
    let counted = spec::parse("exact:id,counts").unwrap();
    let c = r
        .candidates
        .iter()
        .find(|c| c.spec == counted)
        .unwrap_or_else(|| panic!("exact:id,counts is not priced: {:?}", r.candidates));
    assert_eq!(c.origin, CandidateOrigin::Workload);
    let Record::Replay(rp) = &c.replay.record else {
        panic!()
    };
    // Each count reads nothing and carries its rows; each locate reads what
    // the current configuration reads.
    for (now, then) in rp.current.iter().zip(&rp.proposed) {
        if now.run_id.starts_with('c') {
            assert!(now.bytes > 0, "{now:?}");
            assert_eq!((then.bytes, then.row_groups, then.files), (0, 0, 0));
            assert!(then.rows.is_some(), "{then:?}");
        } else {
            assert_eq!(then.bytes, now.bytes, "{then:?}");
        }
    }
    let current = r.current.unwrap();
    assert!(c.proposed.bytes < current.bytes);
    let rec = r.recommended.as_ref().expect("a recommendation");
    assert_eq!(rec.spec, counted);
    assert_eq!(rec.proposed, c.proposed);

    // Declared, the counted spec is current: nothing is left to price.
    e.index(std::slice::from_ref(&counted)).unwrap();
    let r = e
        .why(&WhyRequest {
            workload: Some(hash),
            max_candidates: 0,
            ..WhyRequest::default()
        })
        .unwrap();
    assert!(
        r.candidates.iter().all(|c| !c.spec.counts()),
        "{:?}",
        r.candidates
    );
    assert!(r.recommended.is_none());
}

#[test]
fn the_recommendation_is_the_strictly_cheapest_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // Row k has id 37k mod 1000 (unique, scattered: every row group's id
    // range spans nearly all ids, so min/max pruning skips nothing for an
    // id lookup) and batch k / 50 (sorted: a batch spans two of the 12 row
    // groups of 25 rows, which min/max pruning finds by itself).
    scattered_corpus(&data, 3, 4, 25, 1_000);
    let e = open(&data);
    // ids of rows 10 and 200, in row groups 0 and 8.
    let hash = record_workload(
        &data,
        &[("a", "id", vec![370, 400]), ("b", "batch", vec![0, 1, 2])],
    );
    let r = e
        .why(&WhyRequest {
            workload: Some(hash),
            max_candidates: 0,
            ..WhyRequest::default()
        })
        .unwrap();
    assert_eq!(r.candidates.len(), 2, "exact:batch and exact:id");
    let current = r.current.unwrap();
    assert_eq!(
        current.row_groups,
        12 + 6,
        "a skips nothing, b 6 by min/max"
    );
    let cheapest = r
        .candidates
        .iter()
        .min_by_key(|c| c.proposed.bytes)
        .unwrap();
    let dearest = r
        .candidates
        .iter()
        .max_by_key(|c| c.proposed.bytes)
        .unwrap();
    assert!(cheapest.proposed.bytes < dearest.proposed.bytes);
    assert_eq!(cheapest.spec.name(), "exact:id");
    assert_eq!(
        cheapest.proposed.row_groups,
        2 + 6,
        "b still min/max-pruned"
    );
    assert_eq!(
        dearest.proposed, current,
        "exact:batch adds nothing to min/max"
    );
    let rec = r.recommended.as_ref().unwrap();
    assert_eq!(rec.spec, cheapest.spec);
    assert_eq!(rec.proposed, cheapest.proposed);
    assert_eq!(rec.current, current);
}

/// A workload mixing lookup columns, on an unindexed folder: `exact:id`
/// serves the id runs, and the batch runs it does not serve are priced as
/// the files' min/max statistics read them, in `why`'s numbers and in the
/// Replay the recommendation cites, which `fix` recomputes and a receipt
/// cites (the receipt half is `where_min_max_keeps_extra_row_groups_...`
/// in the CLI's `output_defects.rs`: `fix` verifies one key-column set at
/// a time, so it refuses this workload). Charging the batch runs a full
/// scan instead (as a raw-scan replay does) would make `exact:id` dearer
/// than reading with min/max pruning, and nothing would be recommended.
#[test]
fn a_key_serving_some_runs_is_recommended_on_the_replay_it_cites() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    scattered_corpus(&data, 3, 4, 25, 1_000);
    let e = open(&data);
    // id run: rows 10, 110, 210 (row groups 0, 4, 8), so the key saves 9
    // of 12 row groups; batch run: batch 0, rows 0..50, row groups 0 and 1.
    let ids: Vec<i64> = [10, 110, 210].iter().map(|k| (k * 37) % 1_000).collect();
    let hash = record_workload(&data, &[("a", "id", ids), ("b", "batch", vec![0])]);
    let r = e
        .why(&WhyRequest {
            workload: Some(hash),
            max_candidates: 0,
            ..WhyRequest::default()
        })
        .unwrap();
    let current = r.current.unwrap();
    let full = r
        .full_scan
        .expect("no index, Parquet: the full scan is context");
    assert_eq!(full.row_groups, 24, "every row group, both runs");
    assert_eq!(
        current.row_groups,
        12 + 2,
        "min/max narrows only the batch run"
    );
    let floor = r
        .candidates
        .iter()
        .find(|c| c.spec.name() == "exact:id")
        .unwrap();
    assert_eq!(floor.proposed.row_groups, 3 + 2);
    // Under a full scan for the batch run, the key would read 3 + 12 row
    // groups, more than min/max pruning's 12 + 2.
    let rec = r.recommended.as_ref().expect("exact:id is recommended");
    assert_eq!(rec.spec, floor.spec);
    assert_eq!((rec.current, rec.proposed), (current, floor.proposed));
    // The recommendation's number is its Replay's, run by run.
    assert_eq!(rec.replay, floor.replay.hash);
    let Record::Replay(replay) = &floor.replay.record else {
        panic!()
    };
    assert_eq!(replay.unserved.as_deref(), Some("parquet_stats"));
    let sum = |c: &[bellwether::types::RunCost]| c.iter().map(|c| c.bytes).sum::<u64>();
    assert_eq!(sum(&replay.current), rec.current.bytes);
    assert_eq!(sum(&replay.proposed), rec.proposed.bytes);
    assert_eq!(
        replay.proposed[1], replay.current[1],
        "batch run: min/max both"
    );
}

/// A window run that no key serves is priced as the files' own statistics
/// read it, the window included: a row group whose `ts` range lies wholly
/// outside the window is skipped even though its id range spans every key.
#[test]
fn an_unserved_window_run_skips_row_groups_outside_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // Rows 0..300, 25 per row group; ts = k seconds (µs), ids scattered so
    // min/max on id keeps every row group.
    scattered_corpus(&data, 3, 4, 25, 1_000);
    // ids of rows 10 and 110: row groups 0 and 4.
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "id", &[370, 70]);
    let r = open(&data)
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            window: Some(bellwether::TimeWindow {
                from: "0".into(),
                to: "24000000".into(),
            }),
            ts_column: Some("ts".into()),
            max_candidates: 0,
            ..WhyRequest::default()
        })
        .unwrap();
    // Only row group 0 holds ts in [0, 24 s]; min/max on id alone keeps 12.
    assert_eq!(r.current.unwrap().row_groups, 1, "{:?}", r.current);
}

#[test]
fn a_candidate_that_saves_nothing_is_not_recommended() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 3, 4, 25, 1_000);
    let e = open(&data);
    // One key in every row group: even the floor reads everything.
    let keys: Vec<i64> = (0..12).map(|g| g * 25).collect();
    let hash = record_workload(&data, &[("all", "id", keys)]);
    let r = e
        .why(&WhyRequest {
            workload: Some(hash),
            max_candidates: 0,
            ..WhyRequest::default()
        })
        .unwrap();
    assert_eq!(r.candidates.len(), 1);
    assert_eq!(r.candidates[0].proposed, r.current.unwrap());
    assert!(r.recommended.is_none(), "equal cost is no saving");
}

/// A read-only index that exists: `why` profiles in memory with the index's
/// manifest (the Evidence names its sequence), records nothing, and says how
/// to record it; `--columns` limits the profile's candidates.
#[test]
fn why_on_a_read_only_index_profiles_in_memory_with_its_manifest() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 2, 3, 30, 40);
    open(&data)
        .index(&[spec::parse("exact:batch").unwrap()])
        .unwrap();
    let index = data.join(".bellwether");
    let before = std::fs::read_dir(index.join("ledger")).map_or(0, Iterator::count);
    std::fs::set_permissions(&index, std::fs::Permissions::from_mode(0o555)).unwrap();
    let r = open(&data).why(&WhyRequest {
        columns: Some(vec!["id".into()]),
        ..WhyRequest::default()
    });
    std::fs::set_permissions(&index, std::fs::Permissions::from_mode(0o755)).unwrap();
    let r = r.unwrap();
    assert!(!r.evidence.recorded && r.evidence.hash.is_none());
    assert!(r.evidence.note.as_deref().unwrap().contains("--index"));
    assert_eq!(
        r.evidence.evidence.provenance.manifest_version.as_deref(),
        Some("1")
    );
    let profiled: Vec<_> = r
        .profile
        .fanout
        .iter()
        .map(|e| e.fanout.columns.clone())
        .collect();
    assert_eq!(profiled, vec![vec!["id".to_string()]]);
    let after = std::fs::read_dir(index.join("ledger")).map_or(0, Iterator::count);
    assert_eq!(after, before, "nothing recorded");
}

/// A run keyed on (id, batch): the floor is exact:id,batch, and the
/// profile's single columns id and batch serve it by point lookup, so they
/// are replayed as profile candidates after the floor, each citing the
/// Evidence; keys on payload serve no run.
#[test]
fn profile_candidates_that_serve_by_point_follow_the_floor() {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 3, 4, 25, 400);
    let kf = dir.path().join("k.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("batch", DataType::Int64, false),
    ]));
    let b = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5i64, 77])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0i64, 1])),
        ],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(std::fs::File::create(&kf).unwrap(), schema, None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
    let r = open(&data)
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    let got: Vec<(String, CandidateOrigin)> = r
        .candidates
        .iter()
        .map(|c| (c.spec.name(), c.origin))
        .collect();
    assert_eq!(
        got,
        [
            ("exact:id,batch".to_string(), CandidateOrigin::Workload),
            ("exact:id".to_string(), CandidateOrigin::Profile),
            ("exact:batch".to_string(), CandidateOrigin::Profile),
        ]
    );
    let names: Vec<String> = r.not_serving.iter().map(KeySpec::name).collect();
    assert_eq!(
        names,
        ["exact:payload", "exact:id,payload", "exact:batch,payload"]
    );
    for c in &r.candidates {
        let Record::Proposal(p) = &c.proposal.record else {
            panic!()
        };
        assert_eq!(p.evidence, vec![r.evidence.hash.unwrap()]);
    }
    // max_candidates caps the profile's candidates, never the floor.
    let capped = open(&data)
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            max_candidates: 1,
            ..WhyRequest::default()
        })
        .unwrap();
    let got: Vec<String> = capped.candidates.iter().map(|c| c.spec.name()).collect();
    assert_eq!(got, ["exact:id,batch", "exact:id"]);
}

#[test]
fn why_with_latest_by_prices_temporal_and_locate_and_equals_latest_and_asof_plans() {
    use std::fs::File;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("pu", DataType::Int64, false),
        Field::new(
            "pickup_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    // 4 row groups across 2 files; every key 1..=3 is in every row group,
    // while the newest versions live only in the last row group of file 1.
    for (name, rgs) in [
        (
            "f0.parquet",
            [
                vec![(1i64, 1_000_000i64), (2, 1_000_000), (3, 1_000_000)],
                vec![(1, 2_000_000), (2, 2_000_000), (3, 2_000_000)],
            ],
        ),
        (
            "f1.parquet",
            [
                vec![(1, 3_000_000), (2, 3_000_000), (3, 3_000_000)],
                vec![(1, 9_000_000), (2, 9_000_000), (3, 9_000_000)],
            ],
        ),
    ] {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let mut w = ArrowWriter::try_new(
            File::create(data.join(name)).unwrap(),
            Arc::clone(&schema),
            Some(props),
        )
        .unwrap();
        for rg in rgs {
            let pu: Vec<i64> = rg.iter().map(|r| r.0).collect();
            let ts: Vec<i64> = rg.iter().map(|r| r.1).collect();
            let b = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(pu)) as ArrayRef,
                    Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                ],
            )
            .unwrap();
            w.write(&b).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
    let kf = dir.path().join("k.parquet");
    write_keys(&kf, "pu", &[1, 2, 3]);
    let e = open(&data);

    let locate_why = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            ..WhyRequest::default()
        })
        .unwrap();
    assert!(locate_why.recommended.is_none());

    let latest_why = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            latest_by: Some("pickup_ts".into()),
            ..WhyRequest::default()
        })
        .unwrap();
    let rec = latest_why.recommended.as_ref().unwrap();
    assert_eq!(
        rec.spec,
        KeySpec::Temporal {
            columns: vec!["pu".into()],
            ts_column: "pickup_ts".into(),
            counts: false,
        }
    );
    assert_eq!(rec.lookup, LookupKind::Latest);
    assert_eq!(rec.proposed.row_groups, 1);
    assert_eq!(latest_why.candidates[0].locate.unwrap().row_groups, 4);

    let at = "1970-01-01T00:00:02Z";
    let asof_why = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&kf)),
            latest_by: Some("pickup_ts".into()),
            at: Some(at.into()),
            ..WhyRequest::default()
        })
        .unwrap();
    let asof_rec = asof_why.recommended.as_ref().unwrap();
    assert_eq!(asof_rec.proposed.row_groups, 1);

    let fixed = e
        .fix(&FixRequest {
            proposal: Some(rec.proposal),
            workload: Some(latest_why.workload.as_ref().unwrap().hash),
            ..FixRequest::default()
        })
        .unwrap();
    assert!(matches!(fixed.outcome, FixOutcome::Applied { .. }));

    let latest_plan = e.latest(&KeyFile::new(&kf)).unwrap();
    assert_eq!(latest_plan.planned, rec.proposed);
    let asof_plan = e.asof(&KeyFile::new(&kf), at).unwrap();
    assert_eq!(asof_plan.planned, asof_rec.proposed);
    let locate_plan = e.locate(&KeyFile::new(&kf)).unwrap();
    assert_eq!(
        locate_plan.planned,
        latest_why.candidates[0].locate.unwrap()
    );
}

/// A key proposal's `sidecars` are applied with its spec in one commit:
/// `fix` of `exact:id` proposed with `bloom:id@0.01` beside it scores,
/// verifies and applies both, so the index after the receipt holds both, and
/// the same proposal then has nothing to apply.
#[test]
fn fix_applies_a_proposals_sidecars_with_its_spec() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    scattered_corpus(&data, 3, 4, 25, 1_000);
    let e = open(&data);
    let ids: Vec<i64> = [10, 110, 210].iter().map(|k| (k * 37) % 1_000).collect();
    let hash = record_workload(&data, &[("a", "id", ids)]);
    let r = e
        .why(&WhyRequest {
            workload: Some(hash),
            max_candidates: 0,
            ..WhyRequest::default()
        })
        .unwrap();
    let exact = |c: &str| KeySpec::Exact {
        columns: vec![c.into()],
        counts: false,
        bounds: None,
    };
    let bloom = KeySpec::Bloom {
        columns: vec!["id".into()],
        fpr: 0.01,
        bounds: None,
    };
    let proposal = match e
        .propose(
            ProposalBody::Key {
                spec: exact("id"),
                sidecars: vec![bloom.clone()],
            },
            &[r.evidence.hash.unwrap()],
            "agent",
        )
        .unwrap()
    {
        bellwether::ProposeResult::Written { proposal } => proposal.hash,
        other => panic!("{other:?}"),
    };
    let fixed = e
        .fix(&FixRequest {
            proposal: Some(proposal),
            workload: Some(hash),
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(fixed.outcome, FixOutcome::Applied { .. }),
        "{:?}",
        fixed.outcome
    );
    let after = e
        .why(&WhyRequest {
            workload: Some(hash),
            max_candidates: 0,
            ..WhyRequest::default()
        })
        .unwrap();
    let mut current = after.current_specs.clone();
    current.sort_by_key(KeySpec::name);
    assert_eq!(current, [bloom, exact("id")]);
    // Applying the same proposal again commits nothing: both are complete.
    let again = e
        .fix(&FixRequest {
            proposal: Some(proposal),
            workload: Some(hash),
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(
            again.outcome,
            FixOutcome::Refused {
                refusal: bellwether::Refusal::NothingToApply { .. }
            }
        ),
        "{:?}",
        again.outcome
    );
}
