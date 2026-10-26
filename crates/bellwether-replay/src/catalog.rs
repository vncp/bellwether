//! The row-group catalog: every live row group of one manifest version, with
//! its global id, its file, and its footer statistics.
//!
//! The manifest maps files to row-group ids but carries no per-row-group
//! sizes, so the catalog takes `rows`, `bytes`, and column statistics from
//! [`Source::row_groups`]: footers for Parquet, chunk scans for text. Nothing
//! else of a file is read. Every disagreement between the manifest and the
//! source is an error, never a best guess: a different row-group count, a
//! row group out of position, a fingerprint the source does not confirm, a
//! text chunk whose byte range is not the one the manifest recorded, a live
//! id that is also tombstoned, or an id used twice.
//!
//! A [`ScanPlan`]'s bytes are exact sums of `RowGroupMeta::bytes` over the
//! planned row groups ([`Catalog::plan_for`]).
//!
//! The catalog carries the dataset schema (`Source::schema`), the single
//! source of key types (ADR 016), and its [`Catalog::digest`] covers
//! everything replay reads about the data: the manifest version, the schema,
//! every file's fingerprint and row-group ids with each row group's footer
//! report (rows, bytes, byte range, column statistics), and the tombstoned
//! ids. Two catalogs with equal digests price every plan identically.

use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::{Schema, SchemaRef};
use bellwether_core::segment::ColumnType;
use bellwether_store::Manifest;
use bellwether_types::{
    Error, FileFingerprint, Hash, Result, RowGroupId, RowGroupMeta, ScanEntry, ScanPlan, Source,
};
use roaring::RoaringBitmap;

/// One live row group.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogRowGroup {
    /// Global id (ADR 001, ADR 013).
    pub id: RowGroupId,
    /// Index into [`Catalog::files`].
    pub file: usize,
    /// The source's report, with `id` set.
    pub meta: RowGroupMeta,
}

/// Every live row group of one manifest version.
///
/// Memory: one entry per row group with its column statistics; the manifest
/// it is built from is already that size.
#[derive(Clone, Debug)]
pub struct Catalog {
    manifest_version: String,
    schema: SchemaRef,
    files: Vec<FileFingerprint>,
    row_groups: Vec<CatalogRowGroup>,
    tombstoned: RoaringBitmap,
    digest: Hash,
    // Lookups for verifying plans; nothing iterates them, so their order
    // cannot reach any output.
    file_index: HashMap<FileFingerprint, usize>,
    cell_bytes: HashMap<(usize, u32), u64>,
}

/// The schema as the digest covers it: core's recorded spelling where it
/// has one (`bellwether_source::catalog::schema_identity`).
fn schema_identity(schema: &Schema) -> Vec<bellwether_source::catalog::SchemaColumn> {
    bellwether_source::catalog::schema_identity(schema, |t| {
        ColumnType::of(t)
            .ok()
            .and_then(|c| serde_json::to_string(&c).ok())
    })
}

impl Catalog {
    /// The catalog of a committed manifest: its files and row-group ids,
    /// with sizes and statistics from `source` (module docs), under the
    /// dataset schema `schema`. The manifest version is its `sequence`
    /// (ADR 020).
    pub fn from_manifest(
        manifest: &Manifest,
        source: &dyn Source,
        schema: SchemaRef,
    ) -> Result<Self> {
        let mut tombstoned = RoaringBitmap::new();
        for t in &manifest.rowgroup_space.tombstones {
            for id in &t.rowgroup_ids {
                tombstoned.insert(id.0);
            }
        }
        let mut files = Vec::with_capacity(manifest.rowgroup_space.files.len());
        let mut row_groups = Vec::new();
        for entry in &manifest.rowgroup_space.files {
            let fi = files.len();
            let metas = source.row_groups(&entry.fingerprint)?;
            let path = &entry.fingerprint.path;
            if metas.len() != entry.rowgroup_ids.len() {
                return Err(Error::Source(format!(
                    "{path}: the manifest assigns {} row-group ids but the source reports {} row \
                     groups",
                    entry.rowgroup_ids.len(),
                    metas.len()
                )));
            }
            if let Some(chunks) = &entry.chunks {
                if chunks.len() != metas.len() {
                    return Err(Error::Source(format!(
                        "{path}: the manifest records {} chunks but the source reports {}",
                        chunks.len(),
                        metas.len()
                    )));
                }
                for (c, m) in chunks.iter().zip(&metas) {
                    if m.byte_range != Some((c.start, c.end)) {
                        return Err(Error::Source(format!(
                            "{path}: row group {} covers {:?} but the manifest recorded \
                             [{}, {}); build the source with the manifest's chunk size",
                            m.index_in_file, m.byte_range, c.start, c.end
                        )));
                    }
                }
            }
            for (i, (mut meta, id)) in metas.into_iter().zip(&entry.rowgroup_ids).enumerate() {
                if meta.index_in_file as usize != i {
                    return Err(Error::Source(format!(
                        "{path}: row group {i} is reported at position {}",
                        meta.index_in_file
                    )));
                }
                if meta.file != entry.fingerprint {
                    return Err(Error::Source(format!(
                        "{path}: the source reports a different fingerprint than the manifest \
                         (drift, ADR 013)"
                    )));
                }
                if tombstoned.contains(id.0) {
                    return Err(Error::Other(format!(
                        "row-group id {} is both live and tombstoned in manifest {}",
                        id.0, manifest.sequence
                    )));
                }
                meta.id = Some(*id);
                row_groups.push(CatalogRowGroup {
                    id: *id,
                    file: fi,
                    meta,
                });
            }
            files.push(entry.fingerprint.clone());
        }
        Self::assemble(
            manifest.sequence.to_string(),
            schema,
            files,
            row_groups,
            tombstoned,
        )
    }

    /// The catalog of an unindexed folder, for replay before any index
    /// exists: files from `source.discover(root)` sorted by path, ids
    /// assigned densely in file and row-group order, under the dataset
    /// schema `schema`. Its manifest version is `"0"`, the sequence of a
    /// manifest that was never committed (ADR 020); its digest, not the
    /// version, names the data.
    pub fn discover(source: &dyn Source, root: &str, schema: SchemaRef) -> Result<Self> {
        let mut fps = source.discover(root)?;
        fps.sort_by(|a, b| a.path.cmp(&b.path));
        let mut files = Vec::with_capacity(fps.len());
        let mut row_groups = Vec::new();
        let mut next = 0u32;
        for fp in fps {
            let fi = files.len();
            for (i, mut meta) in source.row_groups(&fp)?.into_iter().enumerate() {
                if meta.index_in_file as usize != i {
                    return Err(Error::Source(format!(
                        "{}: row group {i} is reported at position {}",
                        fp.path, meta.index_in_file
                    )));
                }
                let id = RowGroupId(next);
                next = next
                    .checked_add(1)
                    .ok_or_else(|| Error::Other("more than u32::MAX row groups".into()))?;
                meta.id = Some(id);
                row_groups.push(CatalogRowGroup { id, file: fi, meta });
            }
            files.push(fp);
        }
        Self::assemble(
            "0".to_string(),
            schema,
            files,
            row_groups,
            RoaringBitmap::new(),
        )
    }

    fn assemble(
        manifest_version: String,
        schema: SchemaRef,
        files: Vec<FileFingerprint>,
        mut row_groups: Vec<CatalogRowGroup>,
        tombstoned: RoaringBitmap,
    ) -> Result<Self> {
        row_groups.sort_by_key(|r| r.id);
        for w in row_groups.windows(2) {
            if w[0].id == w[1].id {
                return Err(Error::Other(format!(
                    "row-group id {} is assigned twice in manifest {manifest_version}",
                    w[0].id.0
                )));
            }
        }
        let mut per_file: BTreeMap<usize, Vec<(u32, &RowGroupMeta)>> = BTreeMap::new();
        for rg in &row_groups {
            per_file
                .entry(rg.file)
                .or_default()
                .push((rg.id.0, &rg.meta));
        }
        let digest_files: Vec<(&FileFingerprint, Vec<(u32, &RowGroupMeta)>)> = files
            .iter()
            .enumerate()
            .map(|(i, f)| (f, per_file.remove(&i).unwrap_or_default()))
            .collect();
        let retired: Vec<u32> = tombstoned.iter().collect();
        let digest = bellwether_source::catalog::digest(
            &manifest_version,
            &schema_identity(&schema),
            &digest_files,
            &retired,
        )?;
        drop(digest_files);
        let file_index = files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.clone(), i))
            .collect();
        let cell_bytes = row_groups
            .iter()
            .map(|rg| ((rg.file, rg.meta.index_in_file), rg.meta.bytes))
            .collect();
        Ok(Self {
            manifest_version,
            schema,
            files,
            row_groups,
            tombstoned,
            digest,
            file_index,
            cell_bytes,
        })
    }

    /// The manifest sequence the catalog was built from, as a string.
    #[must_use]
    pub fn manifest_version(&self) -> &str {
        &self.manifest_version
    }

    /// The dataset schema keys are encoded and checked at.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// BLAKE3 over the manifest version, the dataset schema, every file
    /// fingerprint with its row-group ids and footer reports, and the
    /// tombstoned ids (module docs): the identity of the data replay reads,
    /// which keys the transient-segment cache.
    #[must_use]
    pub fn digest(&self) -> Hash {
        self.digest
    }

    /// Files, in manifest (or discovery) order.
    #[must_use]
    pub fn files(&self) -> &[FileFingerprint] {
        &self.files
    }

    /// Live row groups, sorted by id.
    #[must_use]
    pub fn row_groups(&self) -> &[CatalogRowGroup] {
        &self.row_groups
    }

    /// Number of live row groups.
    #[must_use]
    pub fn len(&self) -> usize {
        self.row_groups.len()
    }

    /// Whether there are no live row groups.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_groups.is_empty()
    }

    /// The live row group with id `id`.
    #[must_use]
    pub fn get(&self, id: u32) -> Option<&CatalogRowGroup> {
        self.row_groups
            .binary_search_by_key(&id, |r| r.id.0)
            .ok()
            .map(|i| &self.row_groups[i])
    }

    /// Whether `id` was retired by fingerprint drift (ADR 013).
    #[must_use]
    pub fn is_tombstoned(&self, id: u32) -> bool {
        self.tombstoned.contains(id)
    }

    /// Every live id.
    #[must_use]
    pub fn all_ids(&self) -> RoaringBitmap {
        self.row_groups.iter().map(|r| r.id.0).collect()
    }

    /// Total rows across live row groups.
    #[must_use]
    pub fn rows_total(&self) -> u64 {
        self.row_groups.iter().map(|r| r.meta.rows).sum()
    }

    /// Total compressed bytes across live row groups.
    #[must_use]
    pub fn bytes_total(&self) -> u64 {
        self.row_groups.iter().map(|r| r.meta.bytes).sum()
    }

    /// The plan that reads exactly the row groups in `selected`. Ids
    /// retired by drift are dropped (a segment may still name them until
    /// compaction); an id that is neither live nor tombstoned means the
    /// index and the manifest disagree and is an error. Entries follow file
    /// order; bytes are exact sums of the row groups' compressed sizes.
    pub fn plan_for(&self, selected: &RoaringBitmap) -> Result<ScanPlan> {
        let mut per_file: BTreeMap<usize, (RoaringBitmap, u64)> = BTreeMap::new();
        let mut count = 0u64;
        for id in selected {
            if let Some(rg) = self.get(id) {
                let e = per_file.entry(rg.file).or_default();
                e.0.insert(rg.meta.index_in_file);
                e.1 = checked(e.1, rg.meta.bytes)?;
                count += 1;
            } else if !self.tombstoned.contains(id) {
                return Err(Error::Other(format!(
                    "row-group id {id} is neither live nor tombstoned in manifest {}",
                    self.manifest_version
                )));
            }
        }
        let mut bytes = 0u64;
        let mut entries = Vec::with_capacity(per_file.len());
        for (fi, (row_groups, file_bytes)) in per_file {
            bytes = checked(bytes, file_bytes)?;
            entries.push(ScanEntry {
                file: self.files[fi].clone(),
                row_groups,
                bytes: file_bytes,
                columns: None,
            });
        }
        Ok(ScanPlan {
            manifest_version: self.manifest_version.clone(),
            entries,
            residual: None,
            bytes,
            row_groups_total: self.row_groups.len() as u64,
            row_groups_selected: count,
            projected_bytes: None,
            order: None,
            rows: None,
        })
    }

    /// The ids of the live row groups `plan` reads; a file or row group
    /// outside the catalog contributes none ([`Catalog::verify_plan`]
    /// refuses such a plan).
    #[must_use]
    pub fn ids_of(&self, plan: &ScanPlan) -> RoaringBitmap {
        let read: HashMap<usize, &RoaringBitmap> = plan
            .entries
            .iter()
            .filter_map(|e| self.file_index.get(&e.file).map(|fi| (*fi, &e.row_groups)))
            .collect();
        self.row_groups
            .iter()
            .filter(|rg| {
                read.get(&rg.file)
                    .is_some_and(|b| b.contains(rg.meta.index_in_file))
            })
            .map(|rg| rg.id.0)
            .collect()
    }

    /// Checks that `plan` is internally consistent and that every entry's
    /// bytes are the exact sum of its row groups' compressed sizes in this
    /// catalog, so a plan from any [`crate::PlanSource`] is costed exactly
    /// or refused.
    pub fn verify_plan(&self, plan: &ScanPlan) -> Result<()> {
        if plan.manifest_version != self.manifest_version {
            return Err(Error::Other(format!(
                "plan computed against manifest {} but replay pinned {}",
                plan.manifest_version, self.manifest_version
            )));
        }
        let mut seen = std::collections::BTreeSet::new();
        let (mut bytes, mut selected) = (0u64, 0u64);
        for e in &plan.entries {
            let fi = *self.file_index.get(&e.file).ok_or_else(|| {
                Error::Other(format!(
                    "plan names file {} outside the catalog",
                    e.file.path
                ))
            })?;
            if !seen.insert(fi) || e.row_groups.is_empty() {
                return Err(Error::Other(format!(
                    "plan lists {} twice or with no row groups",
                    e.file.path
                )));
            }
            let mut entry_bytes = 0u64;
            for i in &e.row_groups {
                let b = self.cell_bytes.get(&(fi, i)).ok_or_else(|| {
                    Error::Other(format!(
                        "plan names row group {i} of {}, which is not live",
                        e.file.path
                    ))
                })?;
                entry_bytes = checked(entry_bytes, *b)?;
            }
            if entry_bytes != e.bytes {
                return Err(Error::Other(format!(
                    "plan reports {} bytes for {} but its row groups hold {entry_bytes}",
                    e.bytes, e.file.path
                )));
            }
            bytes = checked(bytes, e.bytes)?;
            selected += e.row_groups.len();
        }
        if bytes != plan.bytes
            || selected != plan.row_groups_selected
            || plan.row_groups_total != self.row_groups.len() as u64
        {
            return Err(Error::Other(format!(
                "plan totals ({} bytes, {} of {} row groups) disagree with its entries ({bytes} \
                 bytes, {selected} of {} row groups)",
                plan.bytes,
                plan.row_groups_selected,
                plan.row_groups_total,
                self.row_groups.len()
            )));
        }
        Ok(())
    }
}

fn checked(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| Error::Other("byte total overflows u64".into()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;

    fn fp(path: &str) -> FileFingerprint {
        FileFingerprint {
            path: path.into(),
            size: 10,
            etag: None,
            tail_blake3: Hash::of(path.as_bytes()),
        }
    }

    fn rg(id: u32, file: usize, index: u32, bytes: u64, f: &FileFingerprint) -> CatalogRowGroup {
        CatalogRowGroup {
            id: RowGroupId(id),
            file,
            meta: RowGroupMeta {
                id: Some(RowGroupId(id)),
                file: f.clone(),
                index_in_file: index,
                rows: 1,
                bytes,
                stats: vec![],
                byte_range: None,
            },
        }
    }

    fn schema(dt: DataType) -> SchemaRef {
        std::sync::Arc::new(Schema::new(vec![Field::new("k", dt, true)]))
    }

    fn catalog() -> Catalog {
        let (a, b) = (fp("a"), fp("b"));
        let mut tomb = RoaringBitmap::new();
        tomb.insert(9);
        Catalog::assemble(
            "3".into(),
            schema(DataType::Int64),
            vec![a.clone(), b.clone()],
            vec![rg(0, 0, 0, 10, &a), rg(1, 0, 1, 20, &a), rg(5, 1, 0, 7, &b)],
            tomb,
        )
        .unwrap()
    }

    #[test]
    fn plans_sum_bytes_drop_tombstones_and_refuse_unknown_ids() {
        let c = catalog();
        let sel: RoaringBitmap = [1u32, 5, 9].into_iter().collect();
        let p = c.plan_for(&sel).unwrap();
        assert_eq!(p.bytes, 27);
        assert_eq!(p.row_groups_selected, 2);
        assert_eq!(p.row_groups_total, 3);
        assert_eq!(p.entries.len(), 2);
        c.verify_plan(&p).unwrap();
        let unknown: RoaringBitmap = [4u32].into_iter().collect();
        assert!(c.plan_for(&unknown).is_err());
    }

    #[test]
    fn verify_refuses_misreported_plans() {
        let c = catalog();
        let sel: RoaringBitmap = [0u32, 1].into_iter().collect();
        let good = c.plan_for(&sel).unwrap();
        let mut bad = good.clone();
        bad.bytes += 1;
        assert!(c.verify_plan(&bad).is_err());
        let mut bad = good.clone();
        bad.entries[0].bytes -= 1;
        bad.bytes -= 1;
        assert!(c.verify_plan(&bad).is_err());
        let mut bad = good.clone();
        bad.manifest_version = "4".into();
        assert!(c.verify_plan(&bad).is_err());
        let mut bad = good;
        bad.entries[0].row_groups.insert(7);
        assert!(c.verify_plan(&bad).is_err());
    }

    #[test]
    fn duplicate_ids_are_refused_and_the_digest_tracks_content() {
        let a = fp("a");
        assert!(Catalog::assemble(
            "1".into(),
            schema(DataType::Int64),
            vec![a.clone()],
            vec![rg(0, 0, 0, 1, &a), rg(0, 0, 1, 1, &a)],
            RoaringBitmap::new()
        )
        .is_err());
        let c1 = catalog();
        let c2 = catalog();
        assert_eq!(c1.digest(), c2.digest());
        let again = |version: &str, dt: DataType, mutate: &dyn Fn(&mut Vec<CatalogRowGroup>)| {
            let mut rgs = c1.row_groups.clone();
            mutate(&mut rgs);
            Catalog::assemble(
                version.into(),
                schema(dt),
                c1.files.clone(),
                rgs,
                c1.tombstoned.clone(),
            )
            .unwrap()
            .digest()
        };
        // Another version, another key type, another footer report: another
        // digest.
        assert_ne!(again("4", DataType::Int64, &|_| {}), c1.digest());
        assert_ne!(
            again("3", DataType::Decimal128(20, 3), &|_| {}),
            c1.digest()
        );
        assert_ne!(
            again("3", DataType::Int64, &|r| r[0].meta.bytes += 1),
            c1.digest()
        );
        assert_ne!(
            again("3", DataType::Int64, &|r| r[0].meta.rows += 1),
            c1.digest()
        );
        assert_eq!(again("3", DataType::Int64, &|_| {}), c1.digest());
    }
}
