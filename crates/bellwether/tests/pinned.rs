//! WP21 item 3: lookups pinned to an earlier manifest (`at_manifest`).
//! The pinned plan is compared byte for byte with the plan recorded before
//! the folder grew, and against the `parquet`-crate oracle.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_wrap,
    clippy::assert_is_empty
)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bellwether::types::{EvidenceBody, Record, ScanPlan};
use bellwether::{spec, Engine, EngineOptions, KeyFile, PlanResult, WhyRequest};
use bellwether_build::{Builder, CompactOptions};
use bellwether_store::ObjectStoreBackend;
use common::{oracle, write_file, write_keys, Row};

fn open(root: &Path, at: Option<u64>) -> Engine {
    Engine::open(
        root.to_str().unwrap(),
        EngineOptions {
            at_manifest: at,
            ..EngineOptions::default()
        },
    )
    .unwrap()
}

fn plan_of(r: &PlanResult) -> &ScanPlan {
    let Record::Evidence(e) = &r.evidence.record else {
        panic!("not evidence")
    };
    let EvidenceBody::Plan { plan } = &e.body else {
        panic!("not a plan")
    };
    plan
}

/// One file of two row groups whose ids are `ids` (twice each, at
/// increasing times starting at `t0` seconds).
fn write_ids(path: &Path, ids: &[i64], t0: i64) {
    let rows = |half: usize| -> Vec<Row> {
        ids.iter()
            .enumerate()
            .map(|(i, id)| {
                let t = t0 + (half * ids.len() + i) as i64;
                (*id, 0, Some(t * 1_000_000), format!("p{t}"))
            })
            .collect()
    };
    write_file(path, &[rows(0), rows(1)]);
}

#[test]
fn a_pinned_lookup_returns_the_earlier_plan_byte_for_byte() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    write_ids(&data.join("a.parquet"), &[1, 2, 3], 0);
    write_ids(&data.join("b.parquet"), &[3, 4, 5], 100);
    let keys = dir.path().join("keys.parquet");
    write_keys(&keys, "id", &[3, 5]);
    let kf = KeyFile::new(&keys);
    let specs = [
        spec::parse("exact:id").unwrap(),
        spec::parse("temporal:id@ts").unwrap(),
    ];

    let e = open(&data, None);
    assert_eq!(e.index(&specs).unwrap().sequence, 1);
    let before = e.locate(&kf).unwrap();
    let before_latest = e.latest(&kf).unwrap();
    let before_asof = e.asof(&kf, "1970-01-01T00:01:00Z").unwrap();
    assert_eq!(before.sequence, 1);

    // The folder grows: a later file holds both keys again, newer.
    write_ids(&data.join("c.parquet"), &[3, 5, 6], 1000);
    assert_eq!(e.index(&[]).unwrap().sequence, 2);
    let now = e.locate(&kf).unwrap();
    assert_eq!(now.sequence, 2);
    assert!(now.planned.row_groups > before.planned.row_groups);
    let now_latest = e.latest(&kf).unwrap();
    assert_ne!(plan_of(&now_latest), plan_of(&before_latest));

    let pinned = open(&data, Some(1));
    assert_eq!(pinned.retained_manifests().unwrap(), vec![1, 2]);
    for (old, new) in [
        (&before, pinned.locate(&kf).unwrap()),
        (&before_latest, pinned.latest(&kf).unwrap()),
        (
            &before_asof,
            pinned.asof(&kf, "1970-01-01T00:01:00Z").unwrap(),
        ),
    ] {
        assert_eq!(new.sequence, 1);
        assert_eq!(
            serde_json::to_vec(plan_of(&new)).unwrap(),
            serde_json::to_vec(plan_of(old)).unwrap(),
            "the pinned plan is the earlier plan, byte for byte"
        );
        assert_eq!(
            (new.planned, new.files_total),
            (old.planned, old.files_total)
        );
        // Provenance cites the same manifest content.
        let (Record::Evidence(a), Record::Evidence(b)) =
            (&new.evidence.record, &old.evidence.record)
        else {
            panic!()
        };
        assert_eq!(a.provenance.inputs, b.provenance.inputs);
    }
    // The oracle over the files sequence 1 recorded.
    let only_ab = tempfile::tempdir().unwrap();
    for f in ["a.parquet", "b.parquet"] {
        std::fs::copy(data.join(f), only_ab.path().join(f)).unwrap();
    }
    let want = oracle(only_ab.path(), &[3, 5]);
    let got: std::collections::BTreeSet<(String, usize)> = plan_of(&pinned.locate(&kf).unwrap())
        .entries
        .iter()
        .flat_map(|en| {
            let n = Path::new(&en.file.path)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();
            en.row_groups.iter().map(move |r| (n.clone(), r as usize))
        })
        .collect();
    assert_eq!(got, want);

    // `why` pinned sees the files sequence 1 recorded and nothing newer.
    let w = pinned
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&keys)),
            ..WhyRequest::default()
        })
        .unwrap();
    assert_eq!((w.sequence, w.dataset.files), (1, 2));
    assert!(w.untracked.is_empty());

    // Writes refuse to run pinned.
    let err = pinned.index(&[]).unwrap_err().to_string();
    assert!(err.contains("--at-manifest 1"), "{err}");
    let err = pinned
        .fix(&bellwether::FixRequest::default())
        .unwrap_err()
        .to_string();
    assert!(err.contains("current manifest"), "{err}");

    // A sequence never committed names the retained range.
    let err = open(&data, Some(9)).locate(&kf).unwrap_err().to_string();
    assert!(
        err.contains("has not been committed") && err.contains("1..=2"),
        "{err}"
    );
}

#[test]
fn a_sequence_garbage_collection_pruned_is_refused_with_the_oldest_retained() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    write_ids(&data.join("a.parquet"), &[1, 2], 0);
    let keys = dir.path().join("keys.parquet");
    write_keys(&keys, "id", &[1]);
    let kf = KeyFile::new(&keys);
    let e = open(&data, None);
    e.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    write_ids(&data.join("b.parquet"), &[1, 3], 100);
    e.index(&[]).unwrap();
    // Compaction replaces sequence 1's segment; GC with no retention
    // deletes it, and with it every manifest up to the last that used it.
    let store = Arc::new(ObjectStoreBackend::local(data.join(".bellwether")).unwrap());
    let c = Builder::new(Arc::clone(&store), "")
        .compact(&CompactOptions::default())
        .unwrap();
    assert!(c.merges >= 1, "{c:?}");
    let report = bellwether_store::gc(&*store, "", Duration::ZERO).unwrap();
    assert!(report.manifests_pruned >= 2, "{report:?}");
    // GC's own commits (claim, release) follow the compaction at 3.
    let retained = e.retained_manifests().unwrap();
    assert_eq!(retained.first(), Some(&3), "{retained:?}");
    assert!(
        retained.windows(2).all(|w| w[1] == w[0] + 1),
        "contiguous: {retained:?}"
    );
    let err = open(&data, Some(1)).locate(&kf).unwrap_err().to_string();
    assert!(
        err.contains("no longer retained") && err.contains("oldest retained is 3"),
        "{err}"
    );
    // The retained one still plans.
    assert_eq!(open(&data, Some(3)).locate(&kf).unwrap().sequence, 3);
}
