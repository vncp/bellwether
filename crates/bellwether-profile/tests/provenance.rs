//! Invariant 6: every Evidence carries provenance. A profile's Evidence
//! names the cache artifacts it was computed from (which resolve), the
//! recorded workload and the manifest when there are any, the age of the
//! oldest of them, and an RFC 3339 creation time; the manifest version is
//! the manifest sequence when the folder is indexed and absent otherwise.
//! Evidence with no inputs is refused before the ledger, and nothing is
//! written.

#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use bellwether_ledger::{rfc3339::is_rfc3339, Ledger, RecordKind};
use bellwether_profile::{
    evidence, profile, profile_and_record, record, InputKind, ProfileOptions,
};
use bellwether_store::{commit, Cache, StagedSegments, StoreExt};
use bellwether_types::{Error, EvidenceBody, Hash, KeySpec, Record, Store};
use common::{path_str, temp_store, write_bench_workload, Layout};

#[test]
fn every_profile_evidence_carries_resolvable_provenance() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::planted().write(&tmp.path().join("data"));
    let wl_dir = tmp.path().join("wl");
    let wl_hash = write_bench_workload(&wl_dir, &[vec![1, 2, 3], vec![4, 5], (0..40).collect()]);
    let (_s, store) = temp_store();
    let options = ProfileOptions {
        workload: Some(wl_dir),
        ..ProfileOptions::for_source("parquet")
    };
    let (report, written, hash) = profile_and_record(
        &common::SpySource::parquet(),
        &path_str(&data),
        &store,
        &options,
    )
    .unwrap();

    let ledger = Ledger::new(Arc::clone(&store));
    assert_eq!(ledger.kind(&hash).unwrap(), RecordKind::Evidence);
    let Record::Evidence(e) = ledger.get(&hash).unwrap() else {
        panic!("expected evidence");
    };
    assert_eq!(e, written);
    let p = &e.provenance;
    assert_ne!(p.inputs, Vec::<Hash>::new());
    assert!(is_rfc3339(&p.created_at), "{}", p.created_at);
    assert_eq!(
        p.manifest_version, None,
        "an unindexed folder has no manifest"
    );
    assert_eq!(
        p.inputs,
        report.inputs.iter().map(|i| i.hash).collect::<Vec<_>>()
    );
    assert_eq!(
        p.oldest_input_age,
        report.inputs.iter().map(|i| i.age).max().unwrap()
    );
    // Every cited cache artifact resolves; the workload is cited by its hash.
    let cache = Cache::new(Arc::clone(&store), "");
    for i in &report.inputs {
        match i.kind {
            InputKind::FooterStats | InputKind::FanoutTable | InputKind::WorkloadSummary => {
                assert!(
                    cache.get_cached(&i.hash).unwrap().is_some(),
                    "{:?} does not resolve",
                    i.kind
                );
            }
            InputKind::Workload => assert_eq!(i.hash, wl_hash),
            InputKind::Manifest => panic!("no manifest here"),
        }
    }
    let kinds: Vec<InputKind> = report.inputs.iter().map(|i| i.kind).collect();
    assert_eq!(
        kinds,
        [
            InputKind::FooterStats,
            InputKind::FanoutTable,
            InputKind::WorkloadSummary,
            InputKind::Workload
        ]
    );
    let EvidenceBody::Profile {
        root,
        fanout,
        lints,
        workload_keys,
    } = &e.body
    else {
        panic!("expected a profile body");
    };
    assert_eq!(root, &path_str(&data));
    assert_eq!(
        fanout,
        &report
            .fanout
            .iter()
            .map(|f| f.fanout.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(lints, &report.lints);
    assert_eq!(workload_keys, &Some([3, 40, 40]));
}

#[test]
fn evidence_without_inputs_is_refused_and_nothing_is_written() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let (_s, store) = temp_store();
    let mut report = profile(
        &bellwether_source::ParquetSource::new(),
        &path_str(&data),
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    report.inputs.clear();
    let err = evidence(&report).unwrap_err();
    assert!(
        matches!(err, Error::LedgerRule(ref m) if m.contains("invariant 6")),
        "{err}"
    );
    let ledger = Ledger::new(Arc::clone(&store));
    assert!(record(&ledger, &report).is_err());
    assert_eq!(ledger.list(RecordKind::Evidence).unwrap().count(), 0);
    assert!(
        store.list("ledger").unwrap().is_empty(),
        "nothing reached the store"
    );
}

#[test]
fn an_indexed_folder_cites_the_manifest_and_its_sequence() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let root = path_str(&data);
    let (_s, store) = temp_store();
    for _ in 0..2 {
        commit(&*store, "", &StagedSegments::new(), |m| {
            let mut m = m.clone();
            m.source.root.clone_from(&root);
            m.schema.key_specs = vec![KeySpec::Exact {
                columns: vec!["entity_id".into()],
            }];
            Ok(m)
        })
        .unwrap();
    }
    let (bytes, _) = store.get_opt("manifest.json").unwrap().unwrap();
    let (report, e, _) = profile_and_record(
        &bellwether_source::ParquetSource::new(),
        &root,
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    assert_eq!(report.manifest.as_ref().unwrap().sequence, 2);
    assert_eq!(e.provenance.manifest_version.as_deref(), Some("2"));
    assert!(
        e.provenance.inputs.contains(&Hash::of(&bytes)),
        "the manifest is cited by content hash"
    );
}

#[test]
fn a_manifest_of_another_root_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let (_s, store) = temp_store();
    commit(&*store, "", &StagedSegments::new(), |m| {
        let mut m = m.clone();
        m.source.root = "/somewhere/else".into();
        Ok(m)
    })
    .unwrap();
    let err = profile(
        &bellwether_source::ParquetSource::new(),
        &path_str(&data),
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("indexes"), "{err}");
}

/// 2020-01-01T00:00:00Z: years before any test run.
const LONG_AGO_SECS: u64 = 1_577_836_800;
const ONE_YEAR: std::time::Duration = std::time::Duration::from_secs(365 * 86_400);

#[test]
fn an_old_workload_file_is_the_oldest_input() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let wl = tmp.path().join("wl");
    write_bench_workload(&wl, &[vec![1, 2, 3]]);
    std::fs::File::options()
        .write(true)
        .open(wl.join("workload.json"))
        .unwrap()
        .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(LONG_AGO_SECS))
        .unwrap();
    let (_s, store) = temp_store();
    let options = ProfileOptions {
        workload: Some(wl),
        ..ProfileOptions::for_source("parquet")
    };
    let (report, e, _) = profile_and_record(
        &bellwether_source::ParquetSource::new(),
        &path_str(&data),
        &store,
        &options,
    )
    .unwrap();
    let workload = report
        .inputs
        .iter()
        .find(|i| i.kind == InputKind::Workload)
        .unwrap();
    assert!(workload.age > ONE_YEAR, "{:?}", workload.age);
    assert_eq!(e.provenance.oldest_input_age, workload.age);
}

#[test]
fn an_old_manifest_is_the_oldest_input() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let root = path_str(&data);
    let (_s, store) = temp_store();
    commit(&*store, "", &StagedSegments::new(), |m| {
        let mut m = m.clone();
        m.source.root.clone_from(&root);
        Ok(m)
    })
    .unwrap();
    // Back-date the commit: the manifest's age is the time since it.
    let (bytes, _) = store.get_opt("manifest.json").unwrap().unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    json["committed_at"] = "2020-01-01T00:00:00.000Z".into();
    store
        .put_content_addressed("manifest.json", serde_json::to_vec(&json).unwrap().into())
        .unwrap();
    let (report, e, _) = profile_and_record(
        &bellwether_source::ParquetSource::new(),
        &root,
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    let manifest = report
        .inputs
        .iter()
        .find(|i| i.kind == InputKind::Manifest)
        .unwrap();
    assert!(manifest.age > ONE_YEAR, "{:?}", manifest.age);
    assert_eq!(e.provenance.oldest_input_age, manifest.age);
}
