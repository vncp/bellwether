//! The storage version 2 manifest (`docs/format.md`, "Manifest, storage
//! version 2"): the minimal manifest there parses and serializes back to
//! the same bytes, per-row-group arrays that do not align with the file's
//! row groups or the schema's columns are refused, a newer storage version
//! is refused, and every byte value at every position of the minimal
//! manifest gives a typed refusal or a manifest whose arrays align, never a
//! panic.

#![allow(clippy::unwrap_used, missing_docs)]

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;

use bellwether_store::Manifest;
use bellwether_types::Error;

/// The minimal manifest of `docs/format.md`, in the canonical form
/// (`Manifest::to_vec`: sorted keys, two-space indent).
const MINIMAL: &str = include_str!("manifest_v2_minimal.json");

#[test]
fn the_minimal_manifest_round_trips_byte_for_byte_and_is_in_format_md() {
    let m = Manifest::from_slice(MINIMAL.as_bytes()).unwrap();
    assert_eq!(m.storage_version, 2);
    assert_eq!(String::from_utf8(m.to_vec().unwrap()).unwrap(), MINIMAL);
    let f = &m.rowgroup_space.files[0];
    assert!(f.has_rowgroup_stats());
    assert_eq!(f.rowgroup_stats(1), Some((2, 40)));
    // Row-group-major: row group 1, column 0 (`symbol`); the partition
    // column `day` has no bytes.
    assert_eq!(f.column_bytes_of(1, 0, 2), Some(30));
    assert_eq!(f.column_bytes_of(0, 1, 2), Some(0));
    assert_eq!(m.key_spec_completeness["exact:symbol,counts"], 1);
    let doc = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/format.md"),
    )
    .unwrap();
    assert!(
        doc.contains(&format!("```json\n{MINIMAL}\n```")),
        "docs/format.md must show the minimal manifest exactly"
    );
}

fn edited(f: impl FnOnce(&mut serde_json::Value)) -> Result<Manifest, Error> {
    let mut v: serde_json::Value = serde_json::from_str(MINIMAL).unwrap();
    f(&mut v);
    Manifest::from_slice(&serde_json::to_vec(&v).unwrap())
}

#[test]
fn misaligned_arrays_and_newer_versions_are_refused() {
    let file = |v: &mut serde_json::Value| -> serde_json::Value {
        v["rowgroup_space"]["files"][0].clone()
    };
    for (what, array, value) in [
        ("rowgroup_rows", "rowgroup_rows", serde_json::json!([4])),
        (
            "rowgroup_bytes",
            "rowgroup_bytes",
            serde_json::json!([60, 40, 1]),
        ),
        (
            "column_bytes",
            "column_bytes",
            serde_json::json!([50, 0, 30]),
        ),
    ] {
        let err = edited(|v| {
            let mut f = file(v);
            f[array] = value;
            v["rowgroup_space"]["files"][0] = f;
        })
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("corrupt manifest") && err.contains(what),
            "{err}"
        );
    }
    // A text source's file has no column bytes: accepted.
    let m = edited(|v| {
        v["rowgroup_space"]["files"][0]
            .as_object_mut()
            .unwrap()
            .remove("column_bytes");
    })
    .unwrap();
    assert_eq!(m.rowgroup_space.files[0].column_bytes_of(0, 0, 2), None);
    // A newer storage version is refused before anything else.
    let err = edited(|v| v["storage_version"] = 3.into()).unwrap_err();
    assert!(matches!(err, Error::UnsupportedVersion { .. }), "{err:?}");
}

/// A manifest edit.
type Edit = Box<dyn FnOnce(&mut serde_json::Value)>;

/// The shapes the next storage version 2 release writes (ADR 028: the
/// record shapes are final in this release, the bytes of storage version 2
/// from the next) are refused by name with `UnsupportedShape`, which says
/// to upgrade: never a serde error, never "a newer storage version" (the
/// version is the same).
#[test]
fn later_storage_version_2_shapes_are_refused_by_name() {
    let shapes: Vec<(&str, &str, Edit)> = vec![
        (
            "manifest column data_type",
            "of column \"symbol\" (a type map)",
            Box::new(|v| {
                v["schema"]["columns"][0]["data_type"] = serde_json::json!({"type": "utf8"});
            }),
        ),
        (
            "manifest column data_type",
            "of column \"day\" (a type map)",
            Box::new(|v| {
                v["schema"]["columns"][1]["data_type"] = serde_json::json!({"arrow": "Date32"});
            }),
        ),
        (
            "manifest source",
            "dialect",
            Box::new(|v| {
                v["source"]["dialect"] =
                    serde_json::json!({"delimiter": ";", "header": true, "quote": "\""});
            }),
        ),
        (
            "key spec with per-key bounds",
            "exact:symbol@ts,counts",
            Box::new(|v| {
                v["schema"]["key_specs"][0]["bounds"] = "ts".into();
            }),
        ),
        (
            "bitemporal key spec",
            "temporal2:symbol@valid,known",
            Box::new(|v| {
                v["schema"]["key_specs"].as_array_mut().unwrap().push(
                    serde_json::json!({"kind": "bitemporal", "columns": ["symbol"],
                        "valid_column": "valid", "known_column": "known"}),
                );
            }),
        ),
    ];
    for (what, found, edit) in shapes {
        let err = edited(edit).unwrap_err();
        let text = err.to_string();
        match &err {
            Error::UnsupportedShape { what: w, found: f } => {
                assert_eq!((*w, f.as_str()), (what, found), "{text}");
            }
            other => panic!("{what}: expected UnsupportedShape, got {other:?}"),
        }
        assert!(
            text.contains(
                "storage version 2 segment/manifest shape not supported by this binary; upgrade"
            ) && !text.contains("newer storage version")
                && !text.contains("serde"),
            "{text}"
        );
    }
}

/// A key or time column containing `,` or `@` (the separators of a key
/// spec's identity) is read as it is: the rule is for new declarations.
/// Two declared specs that share an identity are refused as a corrupt
/// index at storage version 2, where segments are filed by identity and no
/// build commits them.
#[test]
fn identity_separators_read_and_a_shared_identity_is_corrupt() {
    for column in ["sym,bol", "sym@bol"] {
        let m = edited(|v| v["schema"]["key_specs"][0]["columns"][0] = column.into()).unwrap();
        assert_eq!(m.schema.key_specs[0].columns(), [column.to_string()]);
    }
    let err = edited(|v| {
        v["schema"]["key_specs"] = serde_json::json!([
            {"kind": "temporal", "columns": ["symbol"], "ts_column": "ts@x"},
            {"kind": "temporal", "columns": ["symbol@ts"], "ts_column": "x"},
        ]);
    })
    .unwrap_err();
    assert!(
        matches!(&err, Error::Other(m) if m.starts_with("corrupt index:")
            && m.contains("share the identity temporal:symbol@ts@x")
            && m.contains("manifests/")),
        "{err:?}"
    );
}

#[test]
fn every_byte_value_of_the_minimal_manifest_is_refused_or_aligned() {
    let bytes = MINIMAL.as_bytes();
    let mut bad = Vec::new();
    for pos in 0..bytes.len() {
        for v in 0..=u8::MAX {
            if v == bytes[pos] {
                continue;
            }
            let mut b = bytes.to_vec();
            b[pos] = v;
            let outcome = catch_unwind(AssertUnwindSafe(|| match Manifest::from_slice(&b) {
                Err(_) => true,
                Ok(m) => {
                    let columns = m.schema.columns.len();
                    m.rowgroup_space.files.iter().all(|f| {
                        let n = f.rowgroup_ids.len();
                        f.rowgroup_rows.as_ref().is_none_or(|r| r.len() == n)
                            && f.rowgroup_bytes.as_ref().is_none_or(|r| r.len() == n)
                            && f.column_bytes
                                .as_ref()
                                .is_none_or(|r| r.len() == n * columns)
                    })
                }
            }));
            match outcome {
                Ok(true) => {}
                Ok(false) => bad.push(format!("pos {pos} = {v:#x}: misaligned arrays accepted")),
                Err(_) => bad.push(format!("pos {pos} = {v:#x}: panic")),
            }
        }
    }
    assert!(
        bad.is_empty(),
        "{} failures, first: {:?}",
        bad.len(),
        &bad[..bad.len().min(20)]
    );
}
