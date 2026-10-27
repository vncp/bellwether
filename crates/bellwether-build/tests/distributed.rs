//! The multi-machine build (ADR 030): plan, workers as separate processes
//! against a `file://` store, and one commit; equal to a single-machine
//! build of the same work, manifest and lookup plans alike; and every
//! fault the README's fault model names refused with the item or plan to
//! redo.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::implicit_clone,
    clippy::assert_is_empty
)]

mod common;

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use bellwether_build::work::{
    plan_path, DoneWrite, ItemSplit, PlanOutcome, WorkOptions, WorkPlanned, WorkRefusal,
};
use bellwether_build::{BuildOptions, Builder, Lookup, Planner, DEFAULT_MAX_RESTARTS};
use bellwether_store::{
    gc_with, manifest_path, segment_path, CommitOptions, GcOptions, ObjectStoreBackend, StoreExt,
};
use bellwether_types::{Hash, KeySpec, SketchSpec, Store, Tombstone};
use bytes::Bytes;
use common::{bloom, enc_i64, exact, parquet_opts, range, simple_schema, write_numbered, Scratch};
use serde_json::Value;

const ROOT: &str = "idx";

/// A store over `dir`, through a `file://` URL as a worker on another
/// machine opens it.
fn store_at(dir: &Path) -> Arc<ObjectStoreBackend> {
    std::fs::create_dir_all(dir).unwrap();
    Arc::new(ObjectStoreBackend::from_url(&format!("file://{}", dir.display())).unwrap())
}

fn builder(dir: &Path) -> Builder<ObjectStoreBackend> {
    Builder::new(store_at(dir), ROOT)
}

/// Copies a directory tree.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dest = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_tree(&e.path(), &dest);
        } else {
            std::fs::copy(e.path(), dest).unwrap();
        }
    }
}

/// The specs the equality test adds over a base declaring `exact:id`: a
/// redefinition (exact with counts, rebuilt), a temporal key with a
/// tombstone (its tombstone sidecar), a bloom with per-key bounds (its
/// `bounds` segments), a range key, and a sketch.
fn specs() -> Vec<KeySpec> {
    vec![
        KeySpec::Exact {
            columns: vec!["id".into()],
            counts: true,
            bounds: None,
        },
        KeySpec::Temporal {
            columns: vec!["id".into()],
            ts_column: "ts".into(),
            counts: false,
            tombstone: Some(Tombstone {
                column: "grp".into(),
                value: Some("g3".into()),
            }),
        },
        KeySpec::Bloom {
            columns: vec!["grp".into()],
            fpr: 0.01,
            bounds: Some("ts".into()),
        },
        range("ts"),
    ]
}

fn sketch() -> SketchSpec {
    SketchSpec {
        items: vec![vec!["grp".into()]],
        precision: bellwether_types::SKETCH_DEFAULT_PRECISION,
        minhash: None,
    }
}

/// Small caps, so items split into several segments per target.
fn options() -> BuildOptions {
    BuildOptions {
        max_segment_row_groups: 4,
        max_segment_keys: 40,
        threads: Some(2),
        sketches: vec![sketch()],
        ..parquet_opts(&simple_schema())
    }
}

/// A data root with `files` files of 6 row groups of 10 rows each.
fn data(s: &Scratch, files: usize) {
    for f in 0..files {
        write_numbered(
            &s.path(&format!("part-{f}.parquet")),
            (f as i64) * 1000,
            60,
            10,
        );
    }
}

fn plan(b: &Builder<ObjectStoreBackend>, root: &str, split: ItemSplit) -> WorkPlanned {
    match b
        .plan_work(
            root,
            &specs(),
            &options(),
            &WorkOptions {
                split,
                index: "unused".into(),
                ..WorkOptions::default()
            },
        )
        .unwrap()
    {
        PlanOutcome::Planned(p) => *p,
        PlanOutcome::Nothing(_) => panic!("nothing to plan"),
    }
}

fn commit(
    b: &Builder<ObjectStoreBackend>,
    plan: &[u8],
) -> Result<bellwether_build::work::WorkCommitted, WorkRefusal> {
    b.commit_work(
        plan,
        Some(2),
        CommitOptions::default(),
        DEFAULT_MAX_RESTARTS,
    )
    .unwrap()
}

/// The manifest as JSON, without the fields every commit stamps from the
/// clock or a random source.
fn manifest_json(dir: &Path) -> Value {
    let (bytes, _) = store_at(dir)
        .get_opt(&manifest_path(ROOT))
        .unwrap()
        .unwrap();
    let mut v: Value = serde_json::from_slice(&bytes).unwrap();
    let o = v.as_object_mut().unwrap();
    o.remove("committed_at");
    o.remove("commit_nonce");
    for t in v["segment_tombstones"].as_array_mut().into_iter().flatten() {
        t.as_object_mut().unwrap().remove("since");
    }
    for t in v["rowgroup_space"]["tombstones"]
        .as_array_mut()
        .into_iter()
        .flatten()
    {
        t.as_object_mut().unwrap().remove("since");
    }
    v
}

// ---------------------------------------------------------------------------
// Worker processes
// ---------------------------------------------------------------------------

/// Not a test on its own: the entry point of a worker process this test
/// binary spawns of itself (`spawn_worker`). Without its environment it
/// does nothing.
#[test]
fn worker_process() {
    let Ok(dir) = std::env::var("BW_WORK_STORE") else {
        return;
    };
    let plan = std::fs::read(std::env::var("BW_WORK_PLAN").unwrap()).unwrap();
    let item: u32 = std::env::var("BW_WORK_ITEM").unwrap().parse().unwrap();
    let b = builder(Path::new(&dir));
    let done = b.work_item(&plan, item, Some(2)).unwrap().unwrap();
    println!("worker {item}: {} segments", done.segments.len());
}

fn spawn_worker(store: &Path, plan: &Path, item: u32) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "worker_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("BW_WORK_STORE", store)
        .env("BW_WORK_PLAN", plan)
        .env("BW_WORK_ITEM", item.to_string())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
}

/// Every item of `p` run by its own process, all at once.
fn run_workers(store: &Path, p: &WorkPlanned, tmp: &Path) {
    let plan_file = tmp.join(format!("{}.json", p.run.to_hex()));
    std::fs::write(&plan_file, &p.plan).unwrap();
    let children: Vec<_> = p
        .items
        .iter()
        .map(|i| spawn_worker(store, &plan_file, i.item))
        .collect();
    for c in children {
        let out = c.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "worker failed:\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

fn run_items_here(b: &Builder<ObjectStoreBackend>, p: &WorkPlanned) {
    for i in &p.items {
        b.work_item(&p.plan, i.item, Some(1)).unwrap().unwrap();
    }
}

/// Every lookup kind over both indexes: equal plans.
fn assert_same_plans(a: &Path, b: &Path) {
    let pa = Planner::open(store_at(a), ROOT).unwrap();
    let pb = Planner::open(store_at(b), ROOT).unwrap();
    let declared = pa.declared_specs().to_vec();
    assert_eq!(declared, pb.declared_specs().to_vec());
    let ids = |v: &[i64]| v.iter().map(|&i| enc_i64(i)).collect::<Vec<_>>();
    let sets = [
        ids(&[0, 1, 2]),
        ids(&[5, 1005, 2059, 999_999]),
        ids(&(0..3000).step_by(97).collect::<Vec<_>>()),
    ];
    let mut lookups = Vec::new();
    for keys in &sets {
        lookups.push(Lookup::Keys {
            columns: vec!["id".into()],
            keys: keys.clone(),
        });
        lookups.push(Lookup::Latest {
            columns: vec!["id".into()],
            keys: keys.clone(),
            live: None,
        });
        lookups.push(Lookup::AsOf {
            columns: vec!["id".into()],
            keys: keys.clone(),
            ts: 30,
            live: None,
        });
        lookups.push(Lookup::Window {
            columns: vec!["id".into()],
            keys: keys.clone(),
            ts_column: "ts".into(),
            lo: 10,
            hi: 40,
        });
    }
    lookups.push(Lookup::Keys {
        columns: vec!["grp".into()],
        keys: vec![common::enc_str("g1")],
    });
    for l in &lookups {
        let x = pa.plan(&declared, l);
        let y = pb.plan(&declared, l);
        match (x, y) {
            (Ok(x), Ok(y)) => assert_eq!(x, y, "{l:?}"),
            (Err(x), Err(y)) => assert_eq!(x.to_string(), y.to_string(), "{l:?}"),
            (x, y) => panic!("{l:?}: {x:?} vs {y:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Equality with a single-machine build
// ---------------------------------------------------------------------------

#[test]
fn independent_worker_processes_commit_what_one_machine_builds() {
    let s = Scratch::new();
    let root = s.root().join("data");
    std::fs::create_dir_all(&root).unwrap();
    let root_str = root.to_str().unwrap().to_string();
    for f in 0..2 {
        write_numbered(&root.join(format!("part-{f}.parquet")), f * 1000, 60, 10);
    }
    let single = s.root().join("single");
    let multi = s.root().join("multi");
    builder(&single)
        .build(&root_str, &[exact(&["id"])], &options())
        .unwrap();
    copy_tree(&single, &multi);
    for f in 2..5 {
        write_numbered(&root.join(format!("part-{f}.parquet")), f * 1000, 60, 10);
    }

    let one = builder(&single)
        .build(&root_str, &specs(), &options())
        .unwrap();
    let b = builder(&multi);
    let p = plan(&b, &root_str, ItemSplit::Items(3));
    assert_eq!(p.items.len(), 3);
    // Nothing committed by the plan.
    assert_eq!(b.manifest().unwrap().unwrap().sequence, 1);
    run_workers(&multi, &p, s.root());
    let c = commit(&b, &p.plan).unwrap();

    assert_eq!(c.report.sequence, one.sequence);
    assert_eq!(c.report.segments, one.segments);
    assert_eq!(c.report.row_groups_read, one.row_groups_read);
    assert_eq!(c.report.tombstone_rows, one.tombstone_rows);
    assert_eq!(c.report.specs_rebuilt, one.specs_rebuilt);
    assert_eq!(c.items.len(), 3);
    assert_eq!(manifest_json(&single), manifest_json(&multi));
    assert_same_plans(&single, &multi);
}

#[test]
fn a_new_index_and_any_split_commit_the_same_manifest() {
    let s = Scratch::new();
    data(&s, 3);
    let root = s.root_str();
    let mut manifests = Vec::new();
    for (i, split) in [
        ItemSplit::Items(1),
        ItemSplit::RowGroups(1),
        ItemSplit::RowGroups(7),
    ]
    .into_iter()
    .enumerate()
    {
        let dir = tempfile::tempdir().unwrap();
        let b = builder(dir.path());
        let p = plan(&b, &root, split);
        if i == 1 {
            assert!(p.items.len() > 3, "{}", p.items.len());
        }
        run_items_here(&b, &p);
        commit(&b, &p.plan).unwrap();
        let mut m = manifest_json(dir.path());
        m.as_object_mut().unwrap().remove("created_at");
        manifests.push((m, dir));
    }
    let dir = tempfile::tempdir().unwrap();
    builder(dir.path())
        .build(&root, &specs(), &options())
        .unwrap();
    let mut m = manifest_json(dir.path());
    m.as_object_mut().unwrap().remove("created_at");
    for (other, _) in &manifests {
        assert_eq!(&m, other);
    }
    assert_same_plans(dir.path(), manifests[0].1.path());
}

// ---------------------------------------------------------------------------
// Faults
// ---------------------------------------------------------------------------

#[test]
fn a_worker_rerun_changes_nothing() {
    let s = Scratch::new();
    data(&s, 2);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    let p = plan(&b, &s.root_str(), ItemSplit::Items(2));
    let first = b.work_item(&p.plan, 1, Some(1)).unwrap().unwrap();
    assert_eq!(first.write, DoneWrite::New);
    let done = format!("{ROOT}/work/{}/done/1.json", p.run.to_hex());
    let (before, _) = b.store().get_opt(&done).unwrap().unwrap();
    let again = b.work_item(&p.plan, 1, Some(4)).unwrap().unwrap();
    assert_eq!(again.write, DoneWrite::Unchanged);
    assert_eq!(again.segments, first.segments);
    let (after, _) = b.store().get_opt(&done).unwrap().unwrap();
    assert_eq!(before, after);
    b.work_item(&p.plan, 0, Some(1)).unwrap().unwrap();
    commit(&b, &p.plan).unwrap();
}

#[test]
fn a_missing_done_file_refuses_naming_every_item() {
    let s = Scratch::new();
    data(&s, 3);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    let p = plan(&b, &s.root_str(), ItemSplit::Items(3));
    b.work_item(&p.plan, 1, Some(1)).unwrap().unwrap();
    match commit(&b, &p.plan) {
        Err(WorkRefusal::NotDone { run, items }) => {
            assert_eq!(run, p.run);
            assert_eq!(items, vec![0, 2]);
        }
        other => panic!("{other:?}"),
    }
    assert!(b.manifest().unwrap().is_none(), "nothing committed");
    // A lost worker is repaired by running its item; the plan is reusable.
    b.work_item(&p.plan, 0, Some(1)).unwrap().unwrap();
    b.work_item(&p.plan, 2, Some(1)).unwrap().unwrap();
    commit(&b, &p.plan).unwrap();
}

#[test]
fn a_drifted_file_refuses_naming_the_item() {
    let s = Scratch::new();
    data(&s, 3);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    let p = plan(&b, &s.root_str(), ItemSplit::Items(3));
    // The last file changes after the plan: the item reading it refuses.
    std::thread::sleep(Duration::from_millis(20));
    write_numbered(&s.path("part-2.parquet"), 2000, 61, 10);
    let mut refused = Vec::new();
    for i in &p.items {
        match b.work_item(&p.plan, i.item, Some(1)).unwrap() {
            Ok(_) => {}
            Err(WorkRefusal::Drift { item, path, .. }) => {
                assert_eq!(item, i.item);
                assert!(path.ends_with("part-2.parquet"), "{path}");
                refused.push(item);
            }
            Err(r) => panic!("{r:?}"),
        }
    }
    assert!(!refused.is_empty());
    match commit(&b, &p.plan) {
        Err(WorkRefusal::NotDone { items, .. }) => assert_eq!(items, refused),
        other => panic!("{other:?}"),
    }
}

/// The compact JSON of a done file's `body`, from its pretty bytes (what its
/// checksum covers), so a test can forge a well-formed done file.
fn body_compact(pretty: &str) -> String {
    let mut out = String::new();
    let (mut in_str, mut esc) = (false, false);
    for c in pretty.chars() {
        if in_str {
            out.push(c);
            if esc {
                esc = false;
            } else if c == '\\' {
                esc = true;
            } else if c == '"' {
                in_str = false;
            }
        } else if c == '"' {
            in_str = true;
            out.push(c);
        } else if !c.is_whitespace() {
            out.push(c);
        }
    }
    let start = out.find("{\"body\":").unwrap() + "{\"body\":".len();
    let end = out.rfind(",\"checksum\":").unwrap();
    out[start..end].to_string()
}

/// Rewrites a done file with `edit` applied to its text and its checksum
/// recomputed: a forgery only the content checks can catch.
fn forge(store: &ObjectStoreBackend, path: &str, edit: impl Fn(&str) -> String) {
    let (bytes, _) = store.get_opt(path).unwrap().unwrap();
    let text = edit(std::str::from_utf8(&bytes).unwrap());
    let old: Value = serde_json::from_slice(&bytes).unwrap();
    let old_sum = old["checksum"].as_str().unwrap().to_string();
    let sum = Hash::of(body_compact(&text).as_bytes()).to_hex();
    let text = text.replace(&old_sum, &sum);
    store
        .put_content_addressed(path, Bytes::from(text))
        .unwrap();
}

#[test]
fn damaged_and_forged_done_files_are_refused_naming_the_item() {
    let s = Scratch::new();
    data(&s, 3);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    let p = plan(&b, &s.root_str(), ItemSplit::Items(3));
    run_items_here(&b, &p);
    let done = |n: u32| format!("{ROOT}/work/{}/done/{n}.json", p.run.to_hex());
    let store = Arc::clone(b.store());
    let (good, _) = store.get_opt(&done(1)).unwrap().unwrap();
    let restore = || store.put_content_addressed(&done(1), good.clone()).unwrap();
    let bad_done = |r: Result<_, WorkRefusal>| match r {
        Err(WorkRefusal::BadDone { item: 1, reason }) => reason,
        other => panic!("{other:?}"),
    };

    // Damage: a flipped byte, a truncation, a reformatted file.
    for (i, cut) in [(good.len() / 2, false), (40, true)] {
        let mut b2 = good.to_vec();
        if cut {
            b2.truncate(i);
        } else {
            b2[i] ^= 0x01;
        }
        store
            .put_content_addressed(&done(1), Bytes::from(b2))
            .unwrap();
        bad_done(commit(&b, &p.plan));
    }
    let compact: Value = serde_json::from_slice(&good).unwrap();
    store
        .put_content_addressed(&done(1), Bytes::from(serde_json::to_vec(&compact).unwrap()))
        .unwrap();
    bad_done(commit(&b, &p.plan));

    // Forged with a valid checksum: another item's number.
    restore();
    forge(&store, &done(1), |t| {
        t.replacen("\"item\": 1", "\"item\": 2", 1)
    });
    assert!(bad_done(commit(&b, &p.plan)).contains("item 2"));

    // Forged: a file the worker read with another fingerprint.
    restore();
    let v: Value = serde_json::from_slice(&good).unwrap();
    let size = v["body"]["outputs"]["files"][0]["size"].as_u64().unwrap();
    let path = v["body"]["outputs"]["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_string();
    forge(&store, &done(1), |t| {
        t.replacen(
            &format!("\"size\": {size}"),
            &format!("\"size\": {}", size + 1),
            1,
        )
    });
    match commit(&b, &p.plan) {
        Err(WorkRefusal::Drift {
            item: 1, path: p2, ..
        }) => assert_eq!(p2, path),
        other => panic!("{other:?}"),
    }

    // Forged: a segment dropped (a row group left unindexed).
    restore();
    let segs = v["body"]["outputs"]["segments"].as_array().unwrap();
    assert!(segs.len() >= 2);
    let first_id = segs[0]["id"].as_str().unwrap().to_string();
    let other_id = segs[1]["id"].as_str().unwrap().to_string();
    forge(&store, &done(1), |t| t.replacen(&first_id, &other_id, 1));
    bad_done(commit(&b, &p.plan));

    // Forged: a segment of item 0 listed by item 1 in place of its own.
    restore();
    let (zero, _) = store.get_opt(&done(0)).unwrap().unwrap();
    let z: Value = serde_json::from_slice(&zero).unwrap();
    let theirs = z["body"]["outputs"]["segments"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    forge(&store, &done(1), |t| t.replacen(&first_id, &theirs, 1));
    match commit(&b, &p.plan) {
        Err(WorkRefusal::BadDone { .. } | WorkRefusal::Segment { .. }) => {}
        other => panic!("{other:?}"),
    }

    // Forged: a segment's bytes count.
    restore();
    let bytes = segs[0]["bytes"].as_u64().unwrap();
    forge(&store, &done(1), |t| {
        t.replacen(
            &format!("\"bytes\": {bytes}"),
            &format!("\"bytes\": {}", bytes + 1),
            1,
        )
    });
    match commit(&b, &p.plan) {
        Err(WorkRefusal::Segment { item: 1, .. }) => {}
        other => panic!("{other:?}"),
    }

    // A damaged segment object: its bytes no longer hash to its id.
    restore();
    let id = Hash::from_hex(&first_id).unwrap();
    let seg = segment_path(ROOT, &id);
    let (sb, _) = store.get_opt(&seg).unwrap().unwrap();
    let mut flipped = sb.to_vec();
    let mid = flipped.len() / 2;
    flipped[mid] ^= 0xff;
    store
        .put_content_addressed(&seg, Bytes::from(flipped))
        .unwrap();
    match commit(&b, &p.plan) {
        Err(WorkRefusal::Segment {
            item: 1, segment, ..
        }) => assert_eq!(segment, id),
        other => panic!("{other:?}"),
    }
    assert!(b.manifest().unwrap().is_none(), "nothing committed");
    // Rerunning the item repairs both the segment and the done file.
    b.work_item(&p.plan, 1, Some(1)).unwrap().unwrap();
    commit(&b, &p.plan).unwrap();
}

#[test]
fn a_build_committed_between_plan_and_commit_makes_the_plan_stale() {
    let s = Scratch::new();
    data(&s, 2);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    b.build(&s.root_str(), &[exact(&["id"])], &options())
        .unwrap();
    write_numbered(&s.path("part-2.parquet"), 2000, 60, 10);
    let p = plan(&b, &s.root_str(), ItemSplit::Items(2));
    run_items_here(&b, &p);
    // A concurrent single-machine build commits another file's ids.
    write_numbered(&s.path("part-3.parquet"), 3000, 60, 10);
    let seq = b
        .build(&s.root_str(), &[exact(&["id"])], &options())
        .unwrap()
        .sequence
        .unwrap();
    match commit(&b, &p.plan) {
        Err(WorkRefusal::Stale {
            committed: false,
            base,
            current,
            what,
            ..
        }) => {
            assert_eq!((base, current), (1, seq));
            assert!(what.contains("row-group space"), "{what}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(b.manifest().unwrap().unwrap().sequence, seq);
    // A fresh plan succeeds.
    let p2 = plan(&b, &s.root_str(), ItemSplit::Items(2));
    run_items_here(&b, &p2);
    let c = commit(&b, &p2.plan).unwrap();
    assert_eq!(c.report.sequence, Some(seq + 1));
}

#[test]
fn two_committers_are_serialized_and_the_second_sees_stale() {
    let s = Scratch::new();
    data(&s, 3);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    let p = plan(&b, &s.root_str(), ItemSplit::Items(3));
    run_items_here(&b, &p);
    let plan_bytes = Arc::new(p.plan.clone());
    let outcomes: Vec<_> = (0..2)
        .map(|_| {
            let d = dir.path().to_path_buf();
            let pb = Arc::clone(&plan_bytes);
            std::thread::spawn(move || commit(&builder(&d), &pb))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    let ok = outcomes.iter().filter(|o| o.is_ok()).count();
    assert_eq!(ok, 1, "{outcomes:?}");
    assert!(outcomes.iter().any(|o| matches!(
        o,
        Err(WorkRefusal::Stale {
            committed: true,
            ..
        })
    )));
    assert_eq!(b.manifest().unwrap().unwrap().sequence, 1);
    // A committer that crashed after its commit landed and runs again.
    match commit(&b, &p.plan) {
        Err(WorkRefusal::Stale {
            committed: true, ..
        }) => {}
        other => panic!("{other:?}"),
    }
}

/// A plan that only fills a kept spec's coverage changes neither the
/// row-group space, the schema, nor the source: after its commit, and
/// after compaction merged its segments away, committing it again would
/// count those row groups twice. Both are refused as stale.
#[test]
fn filling_a_kept_spec_twice_is_refused_even_after_compaction() {
    let s = Scratch::new();
    data(&s, 3);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    let opts = BuildOptions {
        sketches: vec![],
        ..options()
    };
    let counted = KeySpec::Exact {
        columns: vec!["id".into()],
        counts: true,
        bounds: None,
    };
    b.build(&s.root_str(), std::slice::from_ref(&counted), &opts)
        .unwrap();
    // Drop the coverage of the last row groups (an edited manifest, as a
    // fixture): the plan indexes only them, for a kept target.
    let (m, version) = b.store().get_opt(&manifest_path(ROOT)).unwrap().unwrap();
    let mut m: Value = serde_json::from_slice(&m).unwrap();
    let next = m["rowgroup_space"]["next_id"].as_u64().unwrap();
    let segs = m["segments"].as_array_mut().unwrap();
    let before = segs.len();
    segs.retain(|s| s["rowgroup_range"]["end"].as_u64().unwrap() < next - 3);
    assert!(segs.len() < before);
    m["key_spec_completeness"] = serde_json::json!({});
    b.store()
        .put_if_version(
            &manifest_path(ROOT),
            Bytes::from(serde_json::to_vec_pretty(&m).unwrap()),
            &version,
        )
        .unwrap();
    let p = match b
        .plan_work(
            &s.root_str(),
            std::slice::from_ref(&counted),
            &opts,
            &WorkOptions::default(),
        )
        .unwrap()
    {
        PlanOutcome::Planned(p) => *p,
        PlanOutcome::Nothing(_) => panic!("the gap needs indexing"),
    };
    assert!(
        p.report.specs_rebuilt.is_empty(),
        "{:?}",
        p.report.specs_rebuilt
    );
    run_items_here(&b, &p);
    commit(&b, &p.plan).unwrap();
    match commit(&b, &p.plan) {
        Err(WorkRefusal::Stale {
            committed: true, ..
        }) => {}
        other => panic!("{other:?}"),
    }
    let merged = b
        .compact(&bellwether_build::CompactOptions::default())
        .unwrap();
    assert!(merged.merges > 0, "{merged:?}");
    match commit(&b, &p.plan) {
        Err(WorkRefusal::Stale {
            committed: false,
            what,
            ..
        }) => assert!(what.contains("another commit indexed"), "{what}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn garbage_collection_of_uploaded_segments_refuses_then_a_rerun_repairs() {
    let s = Scratch::new();
    data(&s, 2);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    b.build(&s.root_str(), &[exact(&["id"])], &options())
        .unwrap();
    write_numbered(&s.path("part-2.parquet"), 2000, 60, 10);
    let p = plan(&b, &s.root_str(), ItemSplit::Items(2));
    run_items_here(&b, &p);
    // Uploaded, uncommitted segments are orphans: one run tombstones them,
    // the next (retention zero) claims and deletes them.
    let gc = GcOptions::default();
    gc_with(&**b.store(), ROOT, Duration::ZERO, gc).unwrap();
    let r = gc_with(&**b.store(), ROOT, Duration::ZERO, gc).unwrap();
    assert!(r.deleted > 0, "{r:?}");
    let refused = match commit(&b, &p.plan) {
        Err(WorkRefusal::Segment { item, .. }) => item,
        other => panic!("{other:?}"),
    };
    b.work_item(&p.plan, refused, Some(1)).unwrap().unwrap();
    // Every item whose segments were deleted is named in turn.
    let mut rounds = 0;
    loop {
        match commit(&b, &p.plan) {
            Ok(_) => break,
            Err(WorkRefusal::Segment { item, .. }) => {
                b.work_item(&p.plan, item, Some(1)).unwrap().unwrap();
            }
            Err(r) => panic!("{r:?}"),
        }
        rounds += 1;
        assert!(rounds < 10);
    }
}

#[test]
fn a_gc_claim_on_an_uploaded_segment_is_retried_then_refused() {
    let s = Scratch::new();
    data(&s, 2);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    b.build(&s.root_str(), &[exact(&["id"])], &options())
        .unwrap();
    write_numbered(&s.path("part-2.parquet"), 2000, 60, 10);
    let p = plan(&b, &s.root_str(), ItemSplit::Items(1));
    run_items_here(&b, &p);
    // A live claim on one uploaded segment, as a GC run in progress holds.
    let done = format!("{ROOT}/work/{}/done/0.json", p.run.to_hex());
    let (d, _) = b.store().get_opt(&done).unwrap().unwrap();
    let d: Value = serde_json::from_slice(&d).unwrap();
    let id = d["body"]["outputs"]["segments"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (m, version) = b.store().get_opt(&manifest_path(ROOT)).unwrap().unwrap();
    let mut m: Value = serde_json::from_slice(&m).unwrap();
    m["deleting"] = serde_json::json!([{
        "id": id,
        "claimed_at": bellwether_store::now_rfc3339(),
        "token": 7,
        "lease_ms": 3_600_000,
    }]);
    b.store()
        .put_if_version(
            &manifest_path(ROOT),
            Bytes::from(serde_json::to_vec_pretty(&m).unwrap()),
            &version,
        )
        .unwrap();
    match b
        .commit_work(&p.plan, Some(1), CommitOptions::default(), 1)
        .unwrap()
    {
        Err(WorkRefusal::Claimed { item: 0, segment }) => assert_eq!(segment.to_hex(), id),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_crashed_worker_leaves_orphans_and_a_reusable_plan() {
    let s = Scratch::new();
    data(&s, 2);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    let p = plan(&b, &s.root_str(), ItemSplit::Items(2));
    run_items_here(&b, &p);
    // The worker of item 0 died after uploading, before its done file.
    let done = format!("{ROOT}/work/{}/done/0.json", p.run.to_hex());
    b.store().delete(&done).unwrap();
    match commit(&b, &p.plan) {
        Err(WorkRefusal::NotDone { items, .. }) => assert_eq!(items, vec![0]),
        other => panic!("{other:?}"),
    }
    let again = b.work_item(&p.plan, 0, Some(1)).unwrap().unwrap();
    assert_eq!(again.write, DoneWrite::New);
    commit(&b, &p.plan).unwrap();
}

#[test]
fn a_plan_is_content_addressed_and_checked_before_use() {
    let s = Scratch::new();
    data(&s, 2);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    let p = plan(&b, &s.root_str(), ItemSplit::Items(2));
    assert_eq!(Hash::of(&p.plan), p.run);
    assert_eq!(p.path, plan_path(ROOT, &p.run));
    // Another index store has no such plan.
    let other = tempfile::tempdir().unwrap();
    match builder(other.path()).work_item(&p.plan, 0, None).unwrap() {
        Err(WorkRefusal::NotPublished { run }) => assert_eq!(run, p.run),
        other => panic!("{other:?}"),
    }
    // Every flipped byte is refused (parse, spelling, or an unpublished run).
    for i in (0..p.plan.len()).step_by(7) {
        let mut bad = p.plan.to_vec();
        bad[i] ^= 0x01;
        match b.work_item(&bad, 0, None) {
            Err(_) | Ok(Err(WorkRefusal::NotPublished { .. })) => {}
            Ok(other) => panic!("byte {i}: {other:?}"),
        }
    }
    // An item out of range.
    assert!(b.work_item(&p.plan, 9, None).is_err());
    // A damaged item file.
    let item = format!("{ROOT}/work/{}/items/1.json", p.run.to_hex());
    let (ib, _) = b.store().get_opt(&item).unwrap().unwrap();
    let mut bad = ib.to_vec();
    bad[10] ^= 0x01;
    b.store()
        .put_content_addressed(&item, Bytes::from(bad))
        .unwrap();
    let e = b.work_item(&p.plan, 1, None).unwrap_err().to_string();
    assert!(e.contains("item 1"), "{e}");
}

#[test]
fn nothing_to_do_writes_no_plan() {
    let s = Scratch::new();
    data(&s, 2);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    b.build(&s.root_str(), &specs(), &options()).unwrap();
    let out = b
        .plan_work(&s.root_str(), &specs(), &options(), &WorkOptions::default())
        .unwrap();
    assert!(matches!(out, PlanOutcome::Nothing(_)));
    assert!(!dir.path().join(ROOT).join("work").exists());
}

#[test]
fn the_worker_writes_only_its_segments_and_done_file() {
    // A worker never writes a manifest: the manifest's bytes and the
    // history are untouched by every item.
    let s = Scratch::new();
    data(&s, 2);
    let dir = tempfile::tempdir().unwrap();
    let b = builder(dir.path());
    b.build(&s.root_str(), &[bloom(&["grp"])], &options())
        .unwrap();
    write_numbered(&s.path("part-2.parquet"), 2000, 60, 10);
    let (before, _) = b.store().get_opt(&manifest_path(ROOT)).unwrap().unwrap();
    let history = b.store().list(&format!("{ROOT}/manifests")).unwrap();
    let p = plan(&b, &s.root_str(), ItemSplit::Items(2));
    run_items_here(&b, &p);
    let (after, _) = b.store().get_opt(&manifest_path(ROOT)).unwrap().unwrap();
    assert_eq!(before, after);
    assert_eq!(
        b.store().list(&format!("{ROOT}/manifests")).unwrap(),
        history
    );
}
