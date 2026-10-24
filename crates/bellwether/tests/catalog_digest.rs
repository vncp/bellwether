//! Contract v3 entry 30: the catalog digest replay cites in `Replay.inputs`
//! and the one the verifier cites in certificates
//! (`bellwether_source::catalog`) agree byte for byte on the same folder,
//! unindexed and indexed (including after a rewrite retires row groups).

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::Arc;

use bellwether::types::{FileFingerprint, RowGroupMeta, Source};
use bellwether::{spec, Engine, EngineOptions};
use bellwether_replay::{Catalog, Replayer};
use bellwether_source::catalog::{indexed, schema_identity, unindexed, SchemaColumn};
use bellwether_source::ParquetSource;
use bellwether_store::{read_manifest, ObjectStoreBackend};
use common::{ledger_corpus, write_file};

/// The schema as both digests cover it: core's recorded type spelling.
fn spelled(schema: &arrow::datatypes::Schema) -> Vec<SchemaColumn> {
    schema_identity(schema, |t| {
        bellwether_core::segment::ColumnType::of(t)
            .ok()
            .and_then(|c| serde_json::to_string(&c).ok())
    })
}

fn indexed_digest(root: &str, source: &ParquetSource) -> (bellwether::types::Hash, Catalog) {
    let store = Arc::new(ObjectStoreBackend::from_url(&format!("{root}/.bellwether")).unwrap());
    let (manifest, _) = read_manifest(&*store, "").unwrap().unwrap();
    let schema = source.schema(root).unwrap();
    let catalog = Catalog::from_manifest(&manifest, source, Arc::clone(&schema)).unwrap();
    let files: Vec<(FileFingerprint, Vec<(u32, RowGroupMeta)>)> = manifest
        .rowgroup_space
        .files
        .iter()
        .map(|f| {
            let metas = source.row_groups(&f.fingerprint).unwrap();
            let rgs = f
                .rowgroup_ids
                .iter()
                .zip(metas)
                .map(|(id, m)| (id.0, m))
                .collect();
            (f.fingerprint.clone(), rgs)
        })
        .collect();
    let tombstoned: Vec<u32> = manifest
        .rowgroup_space
        .tombstones
        .iter()
        .flat_map(|t| t.rowgroup_ids.iter().map(|i| i.0))
        .collect();
    let digest = indexed(manifest.sequence, &spelled(&schema), &files, &tombstoned).unwrap();
    (digest, catalog)
}

#[test]
fn replay_and_source_catalog_digests_agree() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    ledger_corpus(&data, 3, 2, 15, 20);
    let root = std::fs::canonicalize(&data).unwrap();
    let root = root.to_str().unwrap();

    // Unindexed: replay's discover catalog against source's `unindexed`.
    let source = ParquetSource::new();
    let replayer = Replayer::unindexed(&source, root).unwrap();
    let schema = source.schema(root).unwrap();
    let files: Vec<(FileFingerprint, Vec<RowGroupMeta>)> = source
        .discover(root)
        .unwrap()
        .into_iter()
        .map(|fp| {
            let metas = source.row_groups(&fp).unwrap();
            (fp, metas)
        })
        .collect();
    assert_eq!(
        replayer.catalog().digest(),
        unindexed(&spelled(&schema), &files).unwrap()
    );

    // Indexed: replay's manifest catalog against source's `indexed`.
    let engine = Engine::open(root, EngineOptions::default()).unwrap();
    engine.index(&[spec::parse("exact:id").unwrap()]).unwrap();
    let (digest, catalog) = indexed_digest(root, &source);
    assert_eq!(catalog.digest(), digest);

    // After a rewrite retires a file's row groups (tombstones) and assigns
    // fresh ids, the two still agree.
    write_file(
        &data.join("part-001.parquet"),
        &[vec![(7, 0, Some(1), "rewritten".into())]],
    );
    engine.index(&[]).unwrap();
    let (digest, catalog) = indexed_digest(root, &source);
    assert_eq!(catalog.digest(), digest);
}
