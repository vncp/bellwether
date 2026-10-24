//! Review probe for PR #21, adopted: the replay crate's `Catalog` digest and
//! `bellwether_source::catalog` agree byte for byte, for an unindexed
//! folder and for a built index. (The probe's call was adapted to the full
//! formula's signature: the source function takes the schema identity and
//! each file's row-group reports.)

#![allow(clippy::unwrap_used, missing_docs)]

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Schema};
use bellwether_build::{BuildOptions, Builder, SourceKind};
use bellwether_core::segment::ColumnType;
use bellwether_replay::Catalog;
use bellwether_source::catalog::{self, SchemaColumn};
use bellwether_source::ParquetSource;
use bellwether_store::{read_manifest, ObjectStoreBackend};
use bellwether_types::{KeySpec, RowGroupMeta, Source};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn write_folder(dir: &std::path::Path) {
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from((0..10).collect::<Vec<i64>>())) as ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from(
                (0..10).map(|i| format!("n{i}")).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap();
    for name in ["b.parquet", "a.parquet"] {
        let f = std::fs::File::create(dir.join(name)).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(4)
            .build();
        let mut w = ArrowWriter::try_new(f, batch.schema(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
}

/// The schema identity with core's recorded type spelling.
fn identity(schema: &Schema) -> Vec<SchemaColumn> {
    catalog::schema_identity(schema, |t: &DataType| {
        ColumnType::of(t)
            .ok()
            .and_then(|c| serde_json::to_string(&c).ok())
    })
}

#[test]
fn replay_and_source_catalog_digests_agree_on_one_folder() {
    let dir = tempfile::tempdir().unwrap();
    write_folder(dir.path());
    let root = dir.path().to_str().unwrap();
    let source = ParquetSource::new();
    let schema = source.schema(root).unwrap();
    let replay = Catalog::discover(&source, root, Arc::clone(&schema))
        .unwrap()
        .digest();
    let files: Vec<_> = source
        .discover(root)
        .unwrap()
        .into_iter()
        .map(|fp| {
            let rgs = source.row_groups(&fp).unwrap();
            (fp, rgs)
        })
        .collect();
    let ours = catalog::unindexed(&identity(&schema), &files).unwrap();
    assert_eq!(replay, ours, "replay {replay} != source::catalog {ours}");
}

#[test]
fn a_different_dataset_schema_over_the_same_files_is_another_catalog() {
    let dir = tempfile::tempdir().unwrap();
    write_folder(dir.path());
    let root = dir.path().to_str().unwrap();
    let source = ParquetSource::new();
    let schema = source.schema(root).unwrap();
    let mut fields: Vec<_> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields[0] = fields[0].clone().with_nullable(!fields[0].is_nullable());
    let other = Arc::new(Schema::new(fields));
    let a = Catalog::discover(&source, root, schema).unwrap().digest();
    let b = Catalog::discover(&source, root, other).unwrap().digest();
    assert_ne!(a, b);
}

#[test]
fn replay_and_source_catalog_digests_agree_on_a_built_index() {
    let dir = tempfile::tempdir().unwrap();
    write_folder(dir.path());
    let root = std::fs::canonicalize(dir.path())
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    Builder::new(Arc::clone(&store), "idx")
        .build(
            &root,
            &[KeySpec::Exact {
                columns: vec!["id".into()],
            }],
            &BuildOptions::new(SourceKind::Parquet),
        )
        .unwrap();
    let (m, _) = read_manifest(&*store, "idx").unwrap().unwrap();
    let source = ParquetSource::new();
    let schema = source.schema(&root).unwrap();
    let replay = Catalog::from_manifest(&m, &source, Arc::clone(&schema))
        .unwrap()
        .digest();
    let files: Vec<_> = m
        .rowgroup_space
        .files
        .iter()
        .map(|f| {
            let rgs: Vec<(u32, RowGroupMeta)> = f
                .rowgroup_ids
                .iter()
                .map(|id| id.0)
                .zip(source.row_groups(&f.fingerprint).unwrap())
                .collect();
            (f.fingerprint.clone(), rgs)
        })
        .collect();
    let tombstoned: Vec<u32> = m
        .rowgroup_space
        .tombstones
        .iter()
        .flat_map(|t| t.rowgroup_ids.iter().map(|id| id.0))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let ours = catalog::indexed(m.sequence, &identity(&schema), &files, &tombstoned).unwrap();
    assert_eq!(replay, ours, "replay {replay} != source::catalog {ours}");
}
