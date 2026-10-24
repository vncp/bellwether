//! An indexed side: the real build planner end to end, and every refusal
//! a wrong planner must meet.

#![allow(clippy::unwrap_used, missing_docs)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema};
use bellwether_build::{BuildOptions, Builder, SourceKind};
use bellwether_core::encoding::{encode_tuple_as, Value};
use bellwether_ledger::{Ledger, RecordKind};
use bellwether_source::ParquetSource;
use bellwether_store::{read_manifest, ObjectStoreBackend};
use bellwether_types::{
    EvidenceBody, FileFingerprint, Hash, KeySpec, Record, ScanEntry, ScanPlan, Source,
};
use bellwether_verify::{
    record, CertificateContext, CoverageRequest, IndexPlanner, ManifestAnchor, ManifestIdentity,
    Options, ReconcileSpec, Reconciler, Side, SidePath, SidePlanner, VerifyError, WorkloadKeys,
};
use common::{dirs, r, root, schema_at, write, Spy, R};
use roaring::RoaringBitmap;

fn key(id: i64) -> Vec<u8> {
    encode_tuple_as(&[Value::Int(id)], &[DataType::Int64]).unwrap()
}

fn workload(ids: &[i64]) -> CoverageRequest {
    CoverageRequest::Workload(WorkloadKeys {
        workload: Hash::of(b"w"),
        age: Duration::from_secs(1),
        keys: ids.iter().map(|&i| key(i)).collect(),
    })
}

/// Four files, each holding a disjoint id range with three versions, in
/// row groups of 30 rows: a key lives in one file's row groups only.
fn clustered(dir: &std::path::Path, change: Option<i64>) {
    for f in 0..4i64 {
        let mut rows = Vec::new();
        for v in 0..3 {
            for id in f * 50..(f + 1) * 50 {
                let mut x = r(id, 1_000 * (v + 1), i128::from(id), "a");
                if Some(id) == change && v == 2 {
                    x.status = "changed".into();
                }
                rows.push(x);
            }
        }
        write(dir, &format!("part-{f}.parquet"), &rows, 30);
    }
}

fn canonical(dir: &std::path::Path) -> String {
    std::fs::canonicalize(dir)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

#[test]
fn a_planned_side_reads_only_planned_row_groups_and_cites_the_plan() {
    let (data, other) = dirs();
    clustered(data.path(), None);
    clustered(other.path(), Some(4));
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    Builder::new(Arc::clone(&store), "idx")
        .build(
            &canonical(data.path()),
            &[KeySpec::Exact {
                columns: vec!["id".into()],
            }],
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    let planner = IndexPlanner::open(&store, "idx").unwrap();
    let left_root = planner.source_root().to_string();
    let left = Spy::new(ParquetSource::new().with_schema(Arc::clone(planner.schema())));
    let right = ParquetSource::new();
    let right_root = root(other.path());
    let spec = ReconcileSpec::new(["id"])
        .latest_by("ts")
        .totals(["amount"])
        .coverage(workload(&[3, 4, 120]));
    let reconciler = Reconciler::new(spec.clone(), Options::default()).unwrap();
    let certified = reconciler
        .certify(
            &Side::new(&left, &left_root).indexed(&planner),
            &Side::new(&right, &right_root),
            &CertificateContext::default(),
        )
        .unwrap();
    let out = &certified.outcome;
    let SidePath::Planned {
        manifest_sequence,
        plan_evidence,
        row_groups_selected,
        row_groups_total,
    } = out.sides[0].path.clone()
    else {
        panic!("{:?}", out.sides[0].path);
    };
    assert_eq!(manifest_sequence, planner.sequence());
    assert_eq!(row_groups_total, 20);
    assert!(
        row_groups_selected < row_groups_total,
        "{row_groups_selected}"
    );
    assert_eq!(out.sides[0].row_groups, row_groups_selected);
    assert_eq!(left.reads(), usize::try_from(row_groups_selected).unwrap());
    assert_eq!(out.sides[1].path, SidePath::Scan);
    assert_eq!(out.counts.mismatched, 1, "{out:#?}");
    assert_eq!(out.counts.matched, 2);

    // The same comparison with both sides scanned agrees.
    let scanned = reconciler
        .run(
            &Side::new(&ParquetSource::new(), &left_root),
            &Side::new(&right, &right_root),
        )
        .unwrap();
    assert_eq!(scanned.counts, out.counts);
    assert_eq!(scanned.mismatches, out.mismatches);
    assert_eq!(scanned.totals, out.totals);

    // The certificate is anchored to the index and cites the plan it read.
    let c = &certified.certificate;
    let identity = planner.manifest().unwrap();
    assert_eq!(
        c.provenance.manifest_version,
        Some(planner.sequence().to_string())
    );
    assert!(c.provenance.inputs.contains(&identity.hash));
    assert!(c.provenance.inputs.contains(&plan_evidence));
    // The indexed side cites its catalog digest: the manifest's ids with
    // the folder's own row-group reports.
    let expected = manifest_catalog(&store, &left_root);
    assert_eq!(c.provenance.inputs[0], expected);
    assert_eq!(out.sides[0].catalog, expected);
    let ledger = Ledger::new(Arc::clone(&store));
    let hash = record(&ledger, &certified).unwrap();
    assert_eq!(ledger.kind(&hash).unwrap(), RecordKind::Certificate);
    let Record::Evidence(ev) = ledger.get(&plan_evidence).unwrap() else {
        panic!("the plan is not evidence");
    };
    let EvidenceBody::Plan { plan } = ev.body else {
        panic!("not a plan");
    };
    assert_eq!(plan.manifest_version, planner.sequence().to_string());
    assert_eq!(plan.row_groups_selected, row_groups_selected);
    assert_eq!(ev.provenance.inputs, [identity.hash]);
}

#[test]
fn an_indexed_side_under_full_coverage_scans_and_still_anchors() {
    let (data, other) = dirs();
    clustered(data.path(), None);
    clustered(other.path(), None);
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    Builder::new(Arc::clone(&store), "idx")
        .build(
            &canonical(data.path()),
            &[KeySpec::Exact {
                columns: vec!["id".into()],
            }],
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    let planner = IndexPlanner::open(&store, "idx").unwrap();
    let left_root = planner.source_root().to_string();
    let left = ParquetSource::new().with_schema(Arc::clone(planner.schema()));
    let right = ParquetSource::new();
    let right_root = root(other.path());
    let certified = Reconciler::new(ReconcileSpec::new(["id"]), Options::default())
        .unwrap()
        .certify(
            &Side::new(&left, &left_root).indexed(&planner),
            &Side::new(&right, &right_root),
            &CertificateContext::default(),
        )
        .unwrap();
    assert!(certified.certificate.pass);
    assert_eq!(certified.outcome.sides[0].path, SidePath::Scan);
    assert_eq!(
        certified.certificate.provenance.manifest_version,
        Some(planner.sequence().to_string())
    );
    assert_eq!(certified.evidence.len(), 0);
}

/// The schema identity with core's recorded type spelling.
fn identity(schema: &arrow::datatypes::Schema) -> Vec<bellwether_source::catalog::SchemaColumn> {
    bellwether_source::catalog::schema_identity(schema, |t| {
        bellwether_core::segment::ColumnType::of(t)
            .ok()
            .and_then(|c| serde_json::to_string(&c).ok())
    })
}

/// The index's catalog digest (docs/format.md), computed here from the
/// stored manifest and the folder's row-group reports.
fn manifest_catalog(store: &ObjectStoreBackend, root: &str) -> Hash {
    let (m, _) = read_manifest(store, "idx").unwrap().unwrap();
    let source = ParquetSource::new();
    let files: Vec<_> = m
        .rowgroup_space
        .files
        .iter()
        .map(|f| {
            let ids = f.rowgroup_ids.iter().map(|id| id.0);
            let rgs = source.row_groups(&f.fingerprint).unwrap();
            (f.fingerprint.clone(), ids.zip(rgs).collect::<Vec<_>>())
        })
        .collect();
    let tombstoned: Vec<u32> = m
        .rowgroup_space
        .tombstones
        .iter()
        .flat_map(|t| t.rowgroup_ids.iter().map(|id| id.0))
        .collect();
    let schema = source.schema(root).unwrap();
    bellwether_source::catalog::indexed(m.sequence, &identity(&schema), &files, &tombstoned)
        .unwrap()
}

/// A planner that says whatever it is told.
struct Fake {
    identity: ManifestIdentity,
    plan: ScanPlan,
}

impl SidePlanner for Fake {
    fn manifest(&self) -> bellwether_types::Result<ManifestIdentity> {
        Ok(self.identity.clone())
    }

    fn plan_keys(&self, _: &[String], _: &[Vec<u8>]) -> bellwether_types::Result<ScanPlan> {
        Ok(self.plan.clone())
    }
}

fn honest(dir: &std::path::Path) -> Fake {
    let source = ParquetSource::new();
    let files: Vec<FileFingerprint> = source.discover(&root(dir)).unwrap();
    let mut rowgroup_ids = Vec::new();
    let mut next = 0u32;
    let entries = files
        .iter()
        .map(|f| {
            let rgs = source.row_groups(f).unwrap();
            let n = u32::try_from(rgs.len()).unwrap();
            rowgroup_ids.push((next..next + n).collect::<Vec<u32>>());
            next += n;
            ScanEntry {
                file: f.clone(),
                row_groups: (0..n).collect::<RoaringBitmap>(),
                bytes: rgs.iter().map(|rg| rg.bytes).sum(),
            }
        })
        .collect();
    Fake {
        identity: ManifestIdentity {
            sequence: 9,
            hash: Hash::of(b"fake manifest"),
            chunks: vec![None; files.len()],
            files,
            rowgroup_ids,
            tombstoned: vec![],
            age: Duration::from_secs(2),
            dataset_schema: source.schema(&root(dir)).unwrap(),
        },
        plan: ScanPlan {
            manifest_version: "9".into(),
            entries,
            residual: None,
            bytes: 0,
            row_groups_total: 0,
            row_groups_selected: 0,
        },
    }
}

fn run_with(
    planner: &Fake,
    l: &std::path::Path,
    rt: &std::path::Path,
) -> bellwether_verify::Result<bellwether_verify::Reconciliation> {
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let (lr, rr) = (root(l), root(rt));
    Reconciler::new(
        ReconcileSpec::new(["id"]).coverage(workload(&[1, 2])),
        Options::default(),
    )
    .unwrap()
    .run(&Side::new(&ls, &lr).indexed(planner), &Side::new(&rs, &rr))
}

fn pair() -> (tempfile::TempDir, tempfile::TempDir) {
    let (l, rt) = dirs();
    let rows: Vec<R> = (0..20).map(|i| r(i, 1, 1, "a")).collect();
    write(l.path(), "a.parquet", &rows[..10], 4);
    write(l.path(), "b.parquet", &rows[10..], 4);
    write(rt.path(), "a.parquet", &rows, 4);
    (l, rt)
}

#[test]
fn an_honest_fake_planner_passes() {
    let (l, rt) = pair();
    let out = run_with(&honest(l.path()), l.path(), rt.path()).unwrap();
    assert!(out.pass);
    assert!(matches!(
        out.sides[0].path,
        SidePath::Planned {
            manifest_sequence: 9,
            ..
        }
    ));
    // The typed result, planned path and workload coverage included,
    // round-trips through its JSON.
    let json = serde_json::to_string(&out).unwrap();
    let back: bellwether_verify::Reconciliation = serde_json::from_str(&json).unwrap();
    assert_eq!(back, out);
}

#[test]
fn a_stale_index_is_refused() {
    let (l, rt) = pair();
    let mut fake = honest(l.path());
    fake.identity.files.pop();
    fake.identity.rowgroup_ids.pop();
    fake.identity.chunks.pop();
    let err = run_with(&fake, l.path(), rt.path()).unwrap_err();
    assert!(
        matches!(err, VerifyError::Index(ref m) if m.contains("rebuild")),
        "{err}"
    );
    let mut fake = honest(l.path());
    fake.identity.files[0].size += 1;
    assert!(matches!(
        run_with(&fake, l.path(), rt.path()),
        Err(VerifyError::Index(_))
    ));
}

#[test]
fn a_plan_against_another_manifest_is_refused() {
    let (l, rt) = pair();
    let mut fake = honest(l.path());
    fake.plan.manifest_version = "8".into();
    let err = run_with(&fake, l.path(), rt.path()).unwrap_err();
    assert!(
        matches!(err, VerifyError::Index(ref m) if m.contains("manifest")),
        "{err}"
    );
}

#[test]
fn a_plan_naming_an_unknown_file_or_row_group_is_refused() {
    let (l, rt) = pair();
    let mut fake = honest(l.path());
    fake.plan.entries[0].file.path.push_str(".gone");
    assert!(matches!(
        run_with(&fake, l.path(), rt.path()),
        Err(VerifyError::Index(_))
    ));
    let mut fake = honest(l.path());
    fake.plan.entries[0].row_groups.insert(99);
    assert!(matches!(
        run_with(&fake, l.path(), rt.path()),
        Err(VerifyError::Index(_))
    ));
    let mut fake = honest(l.path());
    let dup = fake.plan.entries[0].clone();
    fake.plan.entries.push(dup);
    assert!(matches!(
        run_with(&fake, l.path(), rt.path()),
        Err(VerifyError::Index(_))
    ));
}

#[test]
fn an_index_that_encodes_the_key_differently_is_refused() {
    let (l, rt) = pair();
    let mut fake = honest(l.path());
    let mut fields: Vec<Field> = schema_at(2)
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields[0] = Field::new("id", DataType::UInt64, true);
    fake.identity.dataset_schema = Arc::new(Schema::new(fields));
    let err = run_with(&fake, l.path(), rt.path()).unwrap_err();
    assert!(
        matches!(err, VerifyError::Index(ref m) if m.contains("key column")),
        "{err}"
    );
}

#[test]
fn two_indexes_at_different_sequences_need_an_explicit_anchor() {
    let (l, rt) = pair();
    let left = honest(l.path());
    let mut right = honest(rt.path());
    right.identity.sequence = 12;
    right.plan.manifest_version = "12".into();
    let (ls, rs) = (
        Spy::new(ParquetSource::new()),
        Spy::new(ParquetSource::new()),
    );
    let (lr, rr) = (root(l.path()), root(rt.path()));
    let reconciler = Reconciler::new(
        ReconcileSpec::new(["id"]).coverage(workload(&[1])),
        Options::default(),
    )
    .unwrap();
    let sides = (
        Side::new(&ls, &lr).indexed(&left),
        Side::new(&rs, &rr).indexed(&right),
    );
    let err = reconciler
        .certify(&sides.0, &sides.1, &CertificateContext::default())
        .unwrap_err();
    assert!(matches!(err, VerifyError::Manifest(_)), "{err}");
    assert_eq!(ls.reads() + rs.reads(), 0, "refused before reading data");
    let ctx = CertificateContext {
        manifest: Some(ManifestAnchor {
            sequence: 12,
            hash: Hash::of(b"fake manifest"),
            age: Duration::ZERO,
        }),
        ..CertificateContext::default()
    };
    let c = reconciler
        .certify(&sides.0, &sides.1, &ctx)
        .unwrap()
        .certificate;
    assert_eq!(c.provenance.manifest_version.as_deref(), Some("12"));
    assert!(c.provenance.oldest_input_age >= Duration::from_secs(2));
}

#[test]
fn a_plan_whose_bytes_are_not_the_sources_is_refused() {
    // The row-group counts agree; only the bytes say the plan names other
    // byte ranges (a text source at another chunk size).
    let (l, rt) = pair();
    let mut fake = honest(l.path());
    fake.plan.entries[0].bytes += 1;
    let err = run_with(&fake, l.path(), rt.path()).unwrap_err();
    assert!(
        matches!(err, VerifyError::Index(ref m) if m.contains("bytes")),
        "{err}"
    );
}

#[test]
fn a_source_with_another_row_group_count_than_the_index_is_refused() {
    // The bytes agree; only the row-group count differs from the manifest's.
    let (l, rt) = pair();
    let mut fake = honest(l.path());
    fake.identity.rowgroup_ids[0].push(999);
    let err = run_with(&fake, l.path(), rt.path()).unwrap_err();
    assert!(
        matches!(err, VerifyError::Index(ref m) if m.contains("row groups")),
        "{err}"
    );
}

#[test]
fn a_source_whose_byte_ranges_differ_from_the_index_is_refused() {
    // Same files, counts, and bytes; only the recorded byte ranges differ
    // from what the source reports (Parquet reports none).
    let (l, rt) = pair();
    let mut fake = honest(l.path());
    let n = fake.identity.rowgroup_ids[0].len();
    fake.identity.chunks[0] = Some((0..n as u64).map(|i| (i, i + 1)).collect());
    let err = run_with(&fake, l.path(), rt.path()).unwrap_err();
    assert!(
        matches!(err, VerifyError::Index(ref m) if m.contains("recorded")),
        "{err}"
    );
}
