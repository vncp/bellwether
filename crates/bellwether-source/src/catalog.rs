//! The catalog digest (`docs/format.md`, "Catalog digest"): the identity of
//! the data a reader saw, so a replay and a reconcile over the same data
//! cite the same input.
//!
//! It is BLAKE3 over the canonical JSON (`serde_json`, no whitespace) of
//!
//! ```text
//! {"manifest_version":<string>,
//!  "schema":[[<name>,<type>,<nullable>],...],
//!  "files":[[<FileFingerprint>,[{"id","index_in_file","rows","bytes","byte_range","stats"},...]],...],
//!  "tombstoned":[<row-group id>,...]}
//! ```
//!
//! with the fields in that order. The schema is the dataset schema
//! ([`schema_identity`]); each row group carries its global id and the
//! source's report of it (`RowGroupMeta`: position, rows, compressed bytes,
//! byte range, column statistics), so the same files read under another
//! dataset schema or another chunk size are other data. Two cases, one
//! formula:
//!
//! - **Indexed** ([`indexed`]): the manifest's sequence; its files in
//!   manifest order, each with its row groups in ascending id order; its
//!   retired ids, ascending and distinct.
//! - **Unindexed** ([`unindexed`]): version [`UNINDEXED_VERSION`]; the
//!   discovered files sorted by path, ids assigned densely from 0 in file
//!   and row-group order; nothing retired.
//!
//! The replay crate's `Catalog` computes its digest with [`digest`], so
//! there is one definition; its tests check it on real folders.

use arrow::datatypes::{DataType, Schema};
use bellwether_types::{ColumnStats, Error, FileFingerprint, Hash, Result, RowGroupMeta};
use serde::Serialize;

/// The manifest version of a folder with no index: the sequence of a
/// manifest that was never committed (ADR 020).
pub const UNINDEXED_VERSION: &str = "0";

/// One column of the dataset schema as the digest covers it: name, type
/// spelling, nullable.
pub type SchemaColumn = (String, String, bool);

/// The dataset schema as the digest covers it: `(name, type, nullable)` per
/// column, the type in the segment format's recorded spelling where
/// `spell` gives one (pass core's `ColumnType::of` serialized as JSON), else
/// `arrow:` and Arrow's display form. The spelling comes from the caller
/// because this crate does not depend on core.
pub fn schema_identity(
    schema: &Schema,
    spell: impl Fn(&DataType) -> Option<String>,
) -> Vec<SchemaColumn> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let ty = spell(f.data_type()).unwrap_or_else(|| format!("arrow:{}", f.data_type()));
            (f.name().clone(), ty, f.is_nullable())
        })
        .collect()
}

#[derive(Serialize)]
struct DigestRowGroup<'a> {
    id: u32,
    index_in_file: u32,
    rows: u64,
    bytes: u64,
    byte_range: Option<(u64, u64)>,
    stats: &'a [ColumnStats],
}

#[derive(Serialize)]
struct DigestInput<'a> {
    manifest_version: &'a str,
    schema: &'a [SchemaColumn],
    files: Vec<(&'a FileFingerprint, Vec<DigestRowGroup<'a>>)>,
    tombstoned: &'a [u32],
}

/// The catalog digest over inputs taken exactly as given: each file with
/// its row groups as `(id, report)` pairs, in order. [`indexed`] and
/// [`unindexed`] put their inputs in the defined order first.
pub fn digest(
    manifest_version: &str,
    schema: &[SchemaColumn],
    files: &[(&FileFingerprint, Vec<(u32, &RowGroupMeta)>)],
    tombstoned: &[u32],
) -> Result<Hash> {
    Hash::of_record(&DigestInput {
        manifest_version,
        schema,
        files: files
            .iter()
            .map(|(f, rgs)| {
                let rgs = rgs
                    .iter()
                    .map(|(id, m)| DigestRowGroup {
                        id: *id,
                        index_in_file: m.index_in_file,
                        rows: m.rows,
                        bytes: m.bytes,
                        byte_range: m.byte_range,
                        stats: &m.stats,
                    })
                    .collect();
                (*f, rgs)
            })
            .collect(),
        tombstoned,
    })
}

/// The catalog digest of an unindexed folder: each discovered file with the
/// row groups its source reports, in any file order. Files are sorted by
/// path and ids assigned densely from 0 in file and row-group order; a row
/// group reported out of position is an error.
pub fn unindexed(
    schema: &[SchemaColumn],
    files: &[(FileFingerprint, Vec<RowGroupMeta>)],
) -> Result<Hash> {
    let mut sorted: Vec<&(FileFingerprint, Vec<RowGroupMeta>)> = files.iter().collect();
    sorted.sort_by(|a, b| a.0.path.cmp(&b.0.path));
    let mut next = 0u32;
    let mut with_ids: Vec<(&FileFingerprint, Vec<(u32, &RowGroupMeta)>)> =
        Vec::with_capacity(sorted.len());
    for (fp, metas) in sorted {
        let mut rgs = Vec::with_capacity(metas.len());
        for (i, m) in metas.iter().enumerate() {
            if m.index_in_file as usize != i {
                return Err(Error::Source(format!(
                    "{}: row group {i} is reported at position {}",
                    fp.path, m.index_in_file
                )));
            }
            rgs.push((next, m));
            next = next
                .checked_add(1)
                .ok_or_else(|| Error::Other("more than u32::MAX row groups".into()))?;
        }
        with_ids.push((fp, rgs));
    }
    digest(UNINDEXED_VERSION, schema, &with_ids, &[])
}

/// The catalog digest of an index's manifest at `sequence`: its files in
/// manifest order, each with its row groups as `(assigned id, the source's
/// report)`, and its `tombstoned` ids. Row groups are put in ascending id
/// order and the retired ids deduplicated, as the manifest may list them in
/// any order.
pub fn indexed(
    sequence: u64,
    schema: &[SchemaColumn],
    files: &[(FileFingerprint, Vec<(u32, RowGroupMeta)>)],
    tombstoned: &[u32],
) -> Result<Hash> {
    let with_ids: Vec<(&FileFingerprint, Vec<(u32, &RowGroupMeta)>)> = files
        .iter()
        .map(|(f, rgs)| {
            let mut rgs: Vec<(u32, &RowGroupMeta)> = rgs.iter().map(|(id, m)| (*id, m)).collect();
            rgs.sort_by_key(|(id, _)| *id);
            (f, rgs)
        })
        .collect();
    let mut retired = tombstoned.to_vec();
    retired.sort_unstable();
    retired.dedup();
    digest(&sequence.to_string(), schema, &with_ids, &retired)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    fn fp(path: &str, size: u64) -> FileFingerprint {
        FileFingerprint {
            path: path.into(),
            size,
            etag: None,
            tail_blake3: Hash::of(path.as_bytes()),
        }
    }

    fn rg(file: &FileFingerprint, index: u32, rows: u64, min: u8) -> RowGroupMeta {
        RowGroupMeta {
            id: None,
            file: file.clone(),
            index_in_file: index,
            rows,
            bytes: rows * 10,
            stats: vec![ColumnStats {
                column: "id".into(),
                min: Some(vec![0x02, min]),
                max: Some(vec![0x02, min + 1]),
                null_count: Some(0),
                distinct_count: None,
                has_bloom: false,
                bytes: Some(rows * 8),
            }],
            byte_range: (index == 1).then_some((5, 9)),
        }
    }

    // The replay crate's digest input, restated exactly as
    // `bellwether_replay::catalog` declares it.
    #[derive(Serialize)]
    struct ReplayDigestRowGroup<'a> {
        id: u32,
        index_in_file: u32,
        rows: u64,
        bytes: u64,
        byte_range: Option<(u64, u64)>,
        stats: &'a [ColumnStats],
    }

    #[derive(Serialize)]
    struct ReplayDigestInput<'a> {
        manifest_version: &'a str,
        schema: Vec<(String, String, bool)>,
        files: Vec<(&'a FileFingerprint, Vec<ReplayDigestRowGroup<'a>>)>,
        tombstoned: Vec<u32>,
    }

    fn replay_rg(id: u32, m: &RowGroupMeta) -> ReplayDigestRowGroup<'_> {
        ReplayDigestRowGroup {
            id,
            index_in_file: m.index_in_file,
            rows: m.rows,
            bytes: m.bytes,
            byte_range: m.byte_range,
            stats: &m.stats,
        }
    }

    fn schema() -> Vec<SchemaColumn> {
        let s = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tags", DataType::new_list(DataType::Utf8, true), true),
        ]);
        // A speller that knows Int64 only, so both spellings appear.
        schema_identity(&s, |t| {
            (t == &DataType::Int64).then(|| r#"{"type":"int64"}"#.to_string())
        })
    }

    #[test]
    fn an_unindexed_folder_matches_the_replay_formula_byte_for_byte() {
        let (a, b) = (fp("/d/a.parquet", 10), fp("/d/b.parquet", 20));
        let (a0, a1) = (rg(&a, 0, 4, 1), rg(&a, 1, 3, 5));
        let b0 = rg(&b, 0, 2, 9);
        let s = schema();
        assert!(s[1].1.starts_with("arrow:List"), "{}", s[1].1);
        assert_eq!(s[0].1, r#"{"type":"int64"}"#);
        // Given out of order.
        let got = unindexed(
            &s,
            &[
                (b.clone(), vec![b0.clone()]),
                (a.clone(), vec![a0.clone(), a1.clone()]),
            ],
        )
        .unwrap();
        let expected = Hash::of_record(&ReplayDigestInput {
            manifest_version: "0",
            schema: s.clone(),
            files: vec![
                (&a, vec![replay_rg(0, &a0), replay_rg(1, &a1)]),
                (&b, vec![replay_rg(2, &b0)]),
            ],
            tombstoned: vec![],
        })
        .unwrap();
        assert_eq!(got, expected);
        assert_eq!(got.to_hex(), GOLDEN_UNINDEXED);
        // A row group out of position is refused, as replay refuses it.
        assert!(unindexed(&s, &[(a.clone(), vec![a1.clone()])]).is_err());
    }

    #[test]
    fn a_manifest_matches_the_replay_formula_byte_for_byte() {
        let (z, a) = (fp("/d/z.parquet", 5), fp("/d/a.parquet", 6));
        let (z0, z1) = (rg(&z, 0, 4, 1), rg(&z, 1, 3, 5));
        let a0 = rg(&a, 0, 2, 9);
        let s = schema();
        // Manifest order kept (z first); ids and retired ids normalized.
        let got = indexed(
            7,
            &s,
            &[
                (z.clone(), vec![(4, z1.clone()), (2, z0.clone())]),
                (a.clone(), vec![(3, a0.clone())]),
            ],
            &[1, 0, 1],
        )
        .unwrap();
        let expected = Hash::of_record(&ReplayDigestInput {
            manifest_version: "7",
            schema: s.clone(),
            files: vec![
                (&z, vec![replay_rg(2, &z0), replay_rg(4, &z1)]),
                (&a, vec![replay_rg(3, &a0)]),
            ],
            tombstoned: vec![0, 1],
        })
        .unwrap();
        assert_eq!(got, expected);
        assert_eq!(got.to_hex(), GOLDEN_INDEXED);
    }

    #[test]
    fn the_schema_and_every_report_field_change_the_digest() {
        let a = fp("/d/a.parquet", 10);
        let a0 = rg(&a, 0, 4, 1);
        let s = schema();
        let base = unindexed(&s, &[(a.clone(), vec![a0.clone()])]).unwrap();
        let mut nullable = s.clone();
        nullable[0].2 = true;
        let mut more = a0.clone();
        more.rows += 1;
        let mut wider = a0.clone();
        wider.bytes += 1;
        let mut ranged = a0.clone();
        ranged.byte_range = Some((0, 1));
        let mut stat = a0.clone();
        stat.stats[0].null_count = Some(1);
        for (what, other) in [
            (
                "schema",
                unindexed(&nullable, &[(a.clone(), vec![a0.clone()])]).unwrap(),
            ),
            ("rows", unindexed(&s, &[(a.clone(), vec![more])]).unwrap()),
            ("bytes", unindexed(&s, &[(a.clone(), vec![wider])]).unwrap()),
            (
                "byte range",
                unindexed(&s, &[(a.clone(), vec![ranged])]).unwrap(),
            ),
            ("stats", unindexed(&s, &[(a.clone(), vec![stat])]).unwrap()),
            (
                "version",
                digest("1", &s, &[(&a, vec![(0, &a0)])], &[]).unwrap(),
            ),
        ] {
            assert_ne!(other, base, "{what}");
        }
    }

    /// Pinned: changing either is a format change for every replay and
    /// certificate that cites a catalog. Re-pinned 2026-10-24 when Arrow 59
    /// changed the display form of the fixture's `List` column, which the
    /// digest spells as `arrow:` plus that form (docs/format.md, "Catalog
    /// digest", amendment of that date).
    const GOLDEN_UNINDEXED: &str =
        "2239565ce9f3fb6f2be5bf0eebef1fa0d59ffb83cfd07d3864b65233882efa17";
    const GOLDEN_INDEXED: &str = "ac22ea4295697791e3da83048f1a7286ea3d7ac25ad6eef7da7bbd885a901e5e";

    /// The fallback spelling the goldens above were computed over. Arrow 53
    /// spelled this type `List(Field { name: "item", data_type: Utf8,
    /// nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })`.
    #[test]
    fn the_fallback_spelling_is_arrows_current_display_form() {
        assert_eq!(schema()[1].1, "arrow:List(Utf8)");
    }
}
