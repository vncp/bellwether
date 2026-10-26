//! The persisted-format fixtures (ADR 017, ADR 028): every binary reads the
//! indexes and ledgers earlier binaries wrote, and answers exactly what they
//! answered.
//!
//! - `fixtures/format-v1/` (storage version 1, record version 2): every
//!   pinned command's `--json` line is the pinned one byte for byte, except
//!   what this binary must change: the plan Evidence it writes is record
//!   version 3 (so its hash), and the manifest it cites is the restored one
//!   (its bytes hold the restore root). The receipt chain reads as it did,
//!   byte for byte.
//! - `fixtures/format-v0.1/` (storage version 2, record version 3, per-key
//!   counts): every pinned line byte for byte, `count` and `locate
//!   --columns` included.
//! - Upgrade on commit: a v1 copy with nothing to do stays at version 1; with
//!   one new file it commits at version 2, says so, records per-row-group
//!   rows and bytes and per-column bytes equal to the files' footers, and
//!   every v1 plan, pinned to its sequence, is unchanged.
//! - A manifest of a newer storage version is refused; so, by name and
//!   saying to upgrade, is a storage version 2 shape the next release adds.
//! - `locate --columns` on a version 1 index reads its column bytes from
//!   the footers and says so; a bad `--columns` is a usage error.
//! - `fixtures/format-v1-separators/` (storage version 1 specs on columns
//!   named with `,` or `@`): a spec on a column `a,b` reads, plans as
//!   version 1 did, and upgrades; two specs sharing an identity read, and
//!   `index` refuses to upgrade them with a step that, followed, upgrades.
//!
//! A restore copies the corpus under a fresh root with every data file's
//! mtime at the fixture clock (a fingerprint's etag is the mtime), copies the
//! index beside it, and replaces the recorded root with the new one in the
//! manifests and in the pinned outputs. Ledger records are never touched:
//! they are named by their hashes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use bellwether::types::format::{ENCODING_VERSION, RECORD_VERSION, STORAGE_VERSION};
use bellwether::types::{Hash, Record};
use common::{bw, path};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::Value;

// A version constant that moves needs a new fixture directory written by
// the new binary, and a test here that reads it.
const _: () = assert!(
    STORAGE_VERSION == 2 && RECORD_VERSION == 3 && ENCODING_VERSION == 1,
    "a format version moved: add fixtures/format-<next>/ and read it in format_compat.rs"
);

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn corpus() -> PathBuf {
    repo()
        .join("fixtures/format-corpus")
        .canonicalize()
        .unwrap()
}

/// The fixture clock, as the mtime of every restored data file.
fn mtime() -> SystemTime {
    // 2026-10-25T00:00:00Z
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_792_886_400)
}

fn copy_tree(from: &Path, to: &Path, touch: bool) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let dest = to.join(p.file_name().unwrap());
        if p.is_dir() {
            copy_tree(&p, &dest, touch);
        } else {
            std::fs::copy(&p, &dest).unwrap();
            if touch {
                File::options()
                    .write(true)
                    .open(&dest)
                    .unwrap()
                    .set_modified(mtime())
                    .unwrap();
            }
        }
    }
}

/// A fixture restored under a fresh root.
struct Restored {
    _tmp: tempfile::TempDir,
    /// The canonical restore root (the fixture's `root` maps here).
    root: String,
    /// `<root>/data`, the indexed folder.
    data: PathBuf,
    /// `expected.json`.
    expected: Value,
    /// Every substitution a pinned output needs: the recorded root, then
    /// each fixture manifest's hash to the restored manifest's.
    subs: Vec<(String, String)>,
}

impl Restored {
    fn new(fixture: &str) -> Self {
        let dir = repo().join("fixtures").join(fixture);
        let expected: Value =
            serde_json::from_slice(&std::fs::read(dir.join("expected.json")).unwrap()).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp
            .path()
            .canonicalize()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let data = Path::new(&root).join("data");
        copy_tree(&corpus().join("data"), &data, true);
        copy_tree(&corpus().join("extra"), &data, true);
        let index = data.join(".bellwether");
        copy_tree(&dir.join("index"), &index, false);
        let old_root = expected["root"].as_str().unwrap().to_string();
        let mut subs = vec![(old_root.clone(), root.clone())];
        let mut manifests = vec![index.join("manifest.json")];
        for e in std::fs::read_dir(index.join("manifests")).unwrap() {
            manifests.push(e.unwrap().path());
        }
        for m in manifests {
            let before = std::fs::read(&m).unwrap();
            let text = String::from_utf8(before.clone()).unwrap();
            assert!(text.contains(&old_root), "{} names no root", m.display());
            let after = text.replace(&old_root, &root);
            std::fs::write(&m, &after).unwrap();
            subs.push((
                Hash::of(&before).to_hex(),
                Hash::of(after.as_bytes()).to_hex(),
            ));
        }
        Self {
            _tmp: tmp,
            root,
            data,
            expected,
            subs,
        }
    }

    fn sub(&self, s: &str) -> String {
        let mut s = s.to_string();
        for (from, to) in &self.subs {
            s = s.replace(from, to);
        }
        s
    }

    /// A pinned argument list, under this restore.
    fn args(&self, pinned: &Value) -> Vec<String> {
        let corpus = path(&corpus()).to_string();
        pinned
            .as_array()
            .unwrap()
            .iter()
            .map(|a| self.sub(&a.as_str().unwrap().replace("<corpus>", &corpus)))
            .collect()
    }

    fn manifest(&self) -> Value {
        serde_json::from_slice(&std::fs::read(self.data.join(".bellwether/manifest.json")).unwrap())
            .unwrap()
    }
}

/// Runs the binary under the fixture clock; returns (exit, stdout, stderr).
fn run(r: &Restored, args: &[String]) -> (i32, String, String) {
    let clock = r.expected["clock"].as_str().unwrap();
    let out = bw()
        .current_dir(&r.root)
        .args(["--clock", clock])
        .args(args)
        .output()
        .unwrap();
    (
        out.status.code().unwrap(),
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
    )
}

fn ok(r: &Restored, args: &[String]) -> String {
    let (code, stdout, stderr) = run(r, args);
    assert_eq!(code, 0, "{args:?}\n{stderr}");
    stdout
}

/// `want` (a pinned line, substituted) against `got`, byte for byte. A plan
/// line carries the Evidence this binary wrote: its hash must be the hash of
/// its record, and stands in for the pinned one; `upgrade` maps a record
/// version 2 Evidence to the version 3 this binary writes.
fn same_line(want: &str, got: &str, upgrade: bool, what: &str) {
    let mut want = want.to_string();
    let got_v: Value = serde_json::from_str(got).unwrap();
    if let Some(ev) = got_v.get("evidence") {
        let record: Record = serde_json::from_value(ev["record"].clone()).unwrap();
        let hash = Hash::of_record(&record).unwrap().to_hex();
        assert_eq!(ev["hash"].as_str().unwrap(), hash, "{what}: evidence hash");
        let want_v: Value = serde_json::from_str(&want).unwrap();
        let old = want_v["evidence"]["hash"].as_str().unwrap().to_string();
        want = want.replace(&old, &hash);
        if upgrade {
            want = want.replace(
                r#""record":"evidence","record_version":2,"#,
                &format!(r#""record":"evidence","record_version":{RECORD_VERSION},"#),
            );
        }
    }
    assert!(
        want == got,
        "{what}: the pinned line and this binary's differ\nwant: {want}\n got: {got}"
    );
}

/// Every pinned lookup, replayed and compared; returns how many.
fn pinned_lookups(r: &Restored, upgrade: bool, extra: &[&str]) -> usize {
    let lookups = r.expected["lookups"].as_array().unwrap();
    for l in lookups {
        let mut args = vec!["--json".to_string()];
        args.extend(r.args(&l["args"]));
        let pinned_at = args.iter().any(|a| a == "--at-manifest");
        if !pinned_at {
            args.extend(extra.iter().map(|s| (*s).to_string()));
        }
        let got = ok(r, &args);
        let want: Vec<String> = l["json"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| r.sub(x.as_str().unwrap()))
            .collect();
        let got: Vec<&str> = got.lines().collect();
        assert_eq!(got.len(), want.len(), "{args:?}");
        for (w, g) in want.iter().zip(got) {
            same_line(w, g, upgrade, &args.join(" "));
        }
    }
    lookups.len()
}

/// The receipt chain: `receipt show` prints the pinned text, every ledger
/// record is named by its hash and parses, and the pinned records are all
/// there: exactly them when `exact` (before any command that records
/// evidence), among later ones otherwise.
fn chain(r: &Restored, exact: bool) {
    let receipt = r.expected["receipt"].as_str().unwrap();
    let text = ok(
        r,
        &[
            "receipt".into(),
            "show".into(),
            path(&r.data).into(),
            receipt.into(),
        ],
    );
    // The chain is read from the ledger, whose records keep the root they
    // were written under: the text is the pinned one, unsubstituted.
    assert_eq!(
        text,
        r.expected["receipt_text"].as_str().unwrap(),
        "receipt show"
    );
    assert!(text.contains("every link present"), "{text}");
    let ledger = r.data.join(".bellwether/ledger");
    let mut names: Vec<String> = Vec::new();
    for e in std::fs::read_dir(&ledger).unwrap() {
        let p = e.unwrap().path();
        let bytes = std::fs::read(&p).unwrap();
        let name = p.file_stem().unwrap().to_str().unwrap().to_string();
        assert_eq!(Hash::of(&bytes).to_hex(), name, "{}", p.display());
        let _: Record = serde_json::from_slice(&bytes).unwrap();
        names.push(name);
    }
    names.sort();
    let want: Vec<String> = r.expected["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    if exact {
        assert_eq!(names, want);
    } else {
        for w in &want {
            assert!(names.contains(w), "pinned record {w} is gone");
        }
    }
    for key in ["proposal", "receipt", "failing_certificate"] {
        let h = r.expected[key].as_str().unwrap();
        assert!(
            names.iter().any(|n| n == h),
            "{key} {h} is not in the ledger"
        );
    }
}

#[test]
fn the_storage_version_1_fixture_reads_as_it_was_written() {
    let r = Restored::new("format-v1");
    assert_eq!(r.manifest()["storage_version"], 1);
    chain(&r, true);
    assert_eq!(pinned_lookups(&r, true, &[]), 9);
    chain(&r, false);
    // Reading changed nothing: still version 1.
    assert_eq!(r.manifest()["storage_version"], 1);
}

#[test]
fn the_storage_version_2_fixture_reads_byte_for_byte() {
    let r = Restored::new("format-v0.1");
    let m = r.manifest();
    assert_eq!(m["storage_version"], 2);
    for f in m["rowgroup_space"]["files"].as_array().unwrap() {
        assert!(f.get("rowgroup_bytes").is_some(), "{f}");
    }
    chain(&r, true);
    assert_eq!(pinned_lookups(&r, false, &[]), 12);
    chain(&r, false);
    let counts: Vec<String> = r.expected["lookups"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|l| l["args"][0] == "count")
        .map(|l| l["json"][0].as_str().unwrap().to_string())
        .collect();
    assert_eq!(counts.len(), 2);
    for c in counts {
        let v: Value = serde_json::from_str(&c).unwrap();
        assert!(v["rows"].as_u64().unwrap() > 0, "{v}");
        assert!(
            v["counted_by"].as_str().unwrap().ends_with(",counts"),
            "{v}"
        );
    }
}

/// Per-row-group rows and bytes and per-column bytes of every tracked file,
/// from the file's footer with the `parquet` crate (the oracle), in the
/// manifest's column order (0 for a column not in the file: a partition
/// column taken from the path).
fn footer_arrays(path: &str, columns: &[String]) -> (Vec<u64>, Vec<u64>, Vec<u64>) {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    let md = reader.metadata();
    let (mut rows, mut bytes, mut cols) = (Vec::new(), Vec::new(), Vec::new());
    for rg in md.row_groups() {
        rows.push(u64::try_from(rg.num_rows()).unwrap());
        bytes.push(u64::try_from(rg.compressed_size()).unwrap());
        for c in columns {
            let chunk = rg.columns().iter().find(|ch| ch.column_descr().name() == c);
            cols.push(chunk.map_or(0, |ch| u64::try_from(ch.compressed_size()).unwrap()));
        }
    }
    (rows, bytes, cols)
}

fn u64s(v: &Value) -> Vec<u64> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_u64().unwrap())
        .collect()
}

#[test]
fn a_storage_version_1_copy_upgrades_on_its_next_commit() {
    let r = Restored::new("format-v1");
    chain(&r, true);
    let index = ["index".to_string(), path(&r.data).to_string()];
    let manifest_bytes = || std::fs::read(r.data.join(".bellwether/manifest.json")).unwrap();

    // Nothing to do: nothing committed, still version 1, same bytes.
    let before = manifest_bytes();
    let out = ok(&r, &index);
    assert!(out.contains("nothing to do"), "{out}");
    assert!(!out.contains("upgraded"), "{out}");
    assert_eq!(manifest_bytes(), before);

    // One new file: committed at version 2, and the index says so.
    let new = r.data.join("day=2026-01-04/part-0.parquet");
    std::fs::create_dir_all(new.parent().unwrap()).unwrap();
    std::fs::copy(r.data.join("day=2026-01-03/part-0.parquet"), &new).unwrap();
    let out = ok(&r, &index);
    assert!(
        out.contains(
            "manifest upgraded to storage version 2 (this index can no longer be opened by a \
             version 1 binary); 5 unchanged files' row-group statistics read from their footers, \
             once"
        ),
        "{out}"
    );
    let json = ok(&r, &[vec!["--json".to_string()], index.to_vec()].concat());
    let v: Value = serde_json::from_str(&json).unwrap();
    assert!(
        v["committed"].is_null(),
        "a second run has nothing to do: {v}"
    );
    assert!(v.get("upgraded_from").is_none(), "{v}");
    let m = r.manifest();
    assert_eq!(m["storage_version"], 2);
    assert_eq!(m["sequence"], 4);
    let columns: Vec<String> = m["schema"]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect();
    let files = m["rowgroup_space"]["files"].as_array().unwrap();
    assert_eq!(files.len(), 6);
    for f in files {
        let p = f["fingerprint"]["path"].as_str().unwrap();
        let (rows, bytes, cols) = footer_arrays(p, &columns);
        assert_eq!(u64s(&f["rowgroup_rows"]), rows, "{p}");
        assert_eq!(u64s(&f["rowgroup_bytes"]), bytes, "{p}");
        assert_eq!(u64s(&f["column_bytes"]), cols, "{p}");
        assert_eq!(f["rowgroup_ids"].as_array().unwrap().len(), rows.len());
    }
    // Segments and completeness keyed by full identities; every v1 segment
    // kept, unrewritten.
    let complete = m["key_spec_completeness"].as_object().unwrap();
    for id in [
        "exact:id",
        "bloom:id@0.01",
        "range:ts",
        "temporal:id@ts",
        "exact:symbol",
    ] {
        assert!(complete.contains_key(id), "{id}: {complete:?}");
    }

    // Every v1 plan, pinned to the v1 sequence it was answered at, is the
    // pinned plan.
    assert_eq!(pinned_lookups(&r, true, &["--at-manifest", "3"]), 9);
    chain(&r, false);
}

#[test]
fn a_newer_storage_version_is_refused() {
    let r = Restored::new("format-v0.1");
    let p = r.data.join(".bellwether/manifest.json");
    let mut m = r.manifest();
    assert_eq!(m["storage_version"], 2);
    m["storage_version"] = 3.into();
    std::fs::write(&p, serde_json::to_vec_pretty(&m).unwrap()).unwrap();
    let keys = path(&corpus().join("keys-id.parquet")).to_string();
    let (code, _, stderr) = run(
        &r,
        &["locate".into(), path(&r.data).into(), "--keys".into(), keys],
    );
    assert_eq!(code, 4, "{stderr}");
    assert!(stderr.contains("storage version 3"), "{stderr}");
}

/// A manifest edit.
type ManifestEdit<'a> = &'a dyn Fn(&mut Value);

/// The storage version 2 shapes the next release writes (a column's
/// `data_type` as a type map, a source `dialect`, a key spec with per-key
/// `bounds`, a bitemporal key spec; ADR 028) are refused by name, exit 4,
/// saying to upgrade: never a serde error, never "a newer storage version".
#[test]
fn later_storage_version_2_shapes_are_refused_by_name() {
    let keys = path(&corpus().join("keys-id.parquet")).to_string();
    let edits: [(&str, ManifestEdit<'_>); 4] = [
        (
            "manifest column data_type of column \"id\" (a type map)",
            &|m| {
                let cols = m["schema"]["columns"].as_array_mut().unwrap();
                let id = cols.iter_mut().find(|c| c["name"] == "id").unwrap();
                id["data_type"] = serde_json::json!({"type": "int64"});
            },
        ),
        ("manifest source dialect", &|m| {
            m["source"]["dialect"] = serde_json::json!({"delimiter": ",", "header": true});
        }),
        ("key spec with per-key bounds exact:id@ts", &|m| {
            let specs = m["schema"]["key_specs"].as_array_mut().unwrap();
            let exact = specs
                .iter_mut()
                .find(|s| s["kind"] == "exact" && s["columns"] == serde_json::json!(["id"]))
                .unwrap();
            exact["bounds"] = "ts".into();
        }),
        ("bitemporal key spec temporal2:id@valid,known", &|m| {
            m["schema"]["key_specs"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({"kind": "bitemporal", "columns": ["id"],
                    "valid_column": "valid", "known_column": "known"}));
        }),
    ];
    for (named, edit) in edits {
        let r = Restored::new("format-v0.1");
        let p = r.data.join(".bellwether/manifest.json");
        let mut m = r.manifest();
        edit(&mut m);
        std::fs::write(&p, serde_json::to_vec_pretty(&m).unwrap()).unwrap();
        let (code, _, stderr) = run(
            &r,
            &[
                "locate".into(),
                path(&r.data).into(),
                "--keys".into(),
                keys.clone(),
            ],
        );
        assert_eq!(code, 4, "{stderr}");
        assert!(
            stderr.contains(named)
                && stderr.contains(
                    "storage version 2 segment/manifest shape not supported by this binary; \
                     upgrade"
                )
                && !stderr.contains("serde")
                && !stderr.contains("newer storage version"),
            "{named}: {stderr}"
        );
    }
}

/// `locate --columns` on a storage version 1 index reads the per-column
/// bytes its manifest lacks from the planned files' footers and says so,
/// with the bytes the footers hold; after an upgrade, a plan pinned to the
/// version 1 sequence says the same of that sequence. A column the dataset
/// does not have, or an empty one, is a usage error naming `--columns`.
#[test]
fn locate_columns_on_a_version_1_index_reads_footers_and_names_bad_columns() {
    let r = Restored::new("format-v1");
    let keys = path(&corpus().join("keys-id.parquet")).to_string();
    let locate = |extra: &[&str]| -> Vec<String> {
        [
            vec![
                "locate".to_string(),
                path(&r.data).to_string(),
                "--keys".to_string(),
                keys.clone(),
            ],
            extra.iter().map(|s| (*s).to_string()).collect(),
        ]
        .concat()
    };
    let out = ok(&r, &locate(&["--columns", "id,amount"]));
    assert!(
        out.contains(
            "manifest sequence 3 is a storage version 1 manifest, which records no per-column \
             bytes, so the projected bytes were read from"
        ),
        "{out}"
    );
    let json: Value = serde_json::from_str(&ok(
        &r,
        &[
            vec!["--json".to_string()],
            locate(&["--columns", "id,amount"]),
        ]
        .concat(),
    ))
    .unwrap();
    assert_eq!(json["column_bytes_from_footers"], json["planned"]["files"]);
    let projected = json["projected_bytes"].as_u64().unwrap();
    assert!(projected > 0 && projected <= json["planned"]["bytes"].as_u64().unwrap());

    for (bad, needle) in [
        ("nosuch", "\"nosuch\" is not in the dataset"),
        ("", "--columns"),
        ("id,id", "twice"),
    ] {
        let (code, _, stderr) = run(&r, &locate(&["--columns", bad]));
        assert_eq!(code, 2, "{bad:?}: {stderr}");
        assert!(
            stderr.contains(needle) && stderr.contains("--columns"),
            "{bad:?}: {stderr}"
        );
    }

    // Upgrade with one new file; the version 1 sequence still answers from
    // its footers, and the current one from the manifest.
    let new = r.data.join("day=2026-01-04/part-0.parquet");
    std::fs::create_dir_all(new.parent().unwrap()).unwrap();
    std::fs::copy(r.data.join("day=2026-01-03/part-0.parquet"), &new).unwrap();
    ok(&r, &["index".to_string(), path(&r.data).to_string()]);
    let pinned = ok(
        &r,
        &[
            locate(&["--columns", "id,amount"]),
            vec!["--at-manifest".into(), "3".into()],
        ]
        .concat(),
    );
    assert!(
        pinned.contains("manifest sequence 3 is a storage version 1"),
        "{pinned}"
    );
    let now = ok(&r, &locate(&["--columns", "id,amount"]));
    assert!(!now.contains("storage version 1"), "{now}");
    assert!(now.contains("planned, projected columns"), "{now}");
}

/// `fixtures/format-v1-separators/` restored under a fresh root, with the
/// index folder `index` (`index-ab` or `index-collide`) as its index.
fn restore_separators(index: &str) -> Restored {
    let dir = repo().join("fixtures/format-v1-separators");
    let expected: Value =
        serde_json::from_slice(&std::fs::read(dir.join("expected.json")).unwrap()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let root = path(&tmp.path().canonicalize().unwrap()).to_string();
    let data = Path::new(&root).join("data");
    copy_tree(&dir.join("data"), &data, true);
    let location = data.join(".bellwether");
    copy_tree(&dir.join(index), &location, false);
    let old_root = expected["root"].as_str().unwrap().to_string();
    let new_root = path(&data).to_string();
    let mut manifests = vec![location.join("manifest.json")];
    for e in std::fs::read_dir(location.join("manifests")).unwrap() {
        manifests.push(e.unwrap().path());
    }
    for m in manifests {
        let text = std::fs::read_to_string(&m).unwrap();
        assert!(text.contains(&old_root), "{} names no root", m.display());
        std::fs::write(&m, text.replace(&old_root, &new_root)).unwrap();
    }
    Restored {
        _tmp: tmp,
        root,
        data,
        expected,
        subs: vec![(old_root, new_root)],
    }
}

fn separator_keys(name: &str) -> String {
    path(&repo().join("fixtures/format-v1-separators").join(name)).to_string()
}

/// The planned files, row groups and bytes of a `--json` plan line.
fn planned(json: &str) -> Value {
    serde_json::from_str::<Value>(json).unwrap()["planned"].clone()
}

/// A storage version 1 index whose one spec is on a column named `a,b`
/// (declared in its JSON form, which version 1 allowed): this binary plans
/// the lookup the version 1 binary planned, a no-op `index` leaves it at
/// version 1, re-declaring the same spec is not a new declaration, and the
/// first build with work upgrades it. Only a new declaration naming such a
/// column is refused.
#[test]
fn a_version_1_index_on_a_column_named_a_comma_b_reads_and_upgrades() {
    let r = restore_separators("index-ab");
    let data = path(&r.data).to_string();
    let s = |v: &[&str]| v.iter().map(|x| (*x).to_string()).collect::<Vec<_>>();
    let locate = s(&[
        "--json",
        "locate",
        &data,
        "--keys",
        &separator_keys("keys-ab.parquet"),
    ]);
    assert_eq!(planned(&ok(&r, &locate)), r.expected["v1_locate_ab"]);
    let out = ok(&r, &s(&["index", &data]));
    assert!(out.contains("nothing to do"), "{out}");
    assert_eq!(r.manifest()["storage_version"], 1);
    let out = ok(
        &r,
        &s(&[
            "index",
            &data,
            "--key",
            r#"{"kind":"exact","columns":["a,b"]}"#,
        ]),
    );
    assert!(out.contains("nothing to do"), "{out}");
    std::fs::copy(r.data.join("part-0.parquet"), r.data.join("part-1.parquet")).unwrap();
    let out = ok(&r, &s(&["index", &data]));
    assert!(
        out.contains("manifest upgraded to storage version 2"),
        "{out}"
    );
    let m = r.manifest();
    assert_eq!(m["storage_version"], 2);
    assert_eq!(m["key_spec_completeness"]["exact:a,b"], 1);
    let p = planned(&ok(&r, &locate));
    assert_eq!(
        (p["files"].as_u64(), p["row_groups"].as_u64()),
        (Some(2), Some(2))
    );
    let (code, _, stderr) = run(
        &r,
        &s(&[
            "index",
            &data,
            "--key",
            r#"{"kind":"exact","columns":["a,b"],"counts":true}"#,
        ]),
    );
    assert_eq!(code, 4, "{stderr}");
    assert!(stderr.contains("\"a,b\" contains ',' or '@'"), "{stderr}");
}

/// A storage version 1 index with two temporal specs that share the
/// identity `temporal:id@ts@x` (a time column named with `@`): it reads,
/// `index` refuses to upgrade it, reading and writing nothing, and names
/// the step; following the step upgrades it, and `latest` then plans what
/// the version 1 binary planned.
#[test]
fn a_version_1_index_with_two_specs_of_one_identity_says_how_to_upgrade() {
    let r = restore_separators("index-collide");
    let data = path(&r.data).to_string();
    let s = |v: &[&str]| v.iter().map(|x| (*x).to_string()).collect::<Vec<_>>();
    let before = std::fs::read(r.data.join(".bellwether/manifest.json")).unwrap();
    let (code, _, stderr) = run(&r, &s(&["index", &data]));
    assert_eq!(code, 4, "{stderr}");
    for needle in [
        "cannot upgrade this storage version 1 index to storage version 2",
        r#"{"kind":"temporal","columns":["id"],"ts_column":"ts@x"}"#,
        r#"{"kind":"temporal","columns":["id@ts"],"ts_column":"x"}"#,
        "share the identity temporal:id@ts@x",
        "nothing was read or written",
        &format!(
            "re-declare temporal:id under its name with a time column whose name has no ',' or \
             '@', for example `bellwether index {data} --key \
             '{{\"kind\":\"temporal\",\"columns\":[\"id\"],\"ts_column\":\"<a time column whose \
             name has no ',' or '@'>\"}}'`"
        ),
    ] {
        assert!(stderr.contains(needle), "{needle}\n{stderr}");
    }
    let after = std::fs::read(r.data.join(".bellwether/manifest.json")).unwrap();
    assert_eq!(before, after, "the refusal wrote the manifest");
    assert_eq!(
        std::fs::read_dir(r.data.join(".bellwether/segments"))
            .unwrap()
            .count(),
        2
    );
    // The step, as the refusal spells it.
    let out = ok(
        &r,
        &s(&[
            "index",
            &data,
            "--key",
            r#"{"kind":"temporal","columns":["id"],"ts_column":"ts"}"#,
        ]),
    );
    assert!(
        out.contains("manifest upgraded to storage version 2"),
        "{out}"
    );
    let m = r.manifest();
    assert_eq!(m["storage_version"], 2);
    let complete: Vec<&String> = m["key_spec_completeness"]
        .as_object()
        .unwrap()
        .keys()
        .collect();
    assert_eq!(complete, ["temporal:id@ts", "temporal:id@ts@x"]);
    let latest = s(&[
        "--json",
        "latest",
        &data,
        "--keys",
        &separator_keys("keys-id.parquet"),
    ]);
    assert_eq!(planned(&ok(&r, &latest)), r.expected["v1_latest_id"]);
}
