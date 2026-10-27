//! A bloom's per-key bounds are checksummed (`docs/format.md`, "Folded
//! layout conformance vectors"): the review's probe of PR #30 rewrote the
//! records of a `bloom:id@0.01@ts` index's `bounds` segment in place,
//! keeping its file name, and a window `locate` then planned no row group
//! with exit 0. Each record is now checksummed in its directory entry, so
//! the same rewrite is refused as a corrupt segment.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{bw, corpus, index_dir, keys, path, run};
use serde_json::Value;

fn u32_at(b: &[u8], at: usize) -> usize {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap()) as usize
}

fn u64_at(b: &[u8], at: usize) -> usize {
    usize::try_from(u64::from_le_bytes(b[at..at + 8].try_into().unwrap())).unwrap()
}

/// Rewrites every bounds pair of every value record of a `keyed` segment
/// to `[-1, -1]`, decoding it as `docs/format.md` lays it out ("Segments,
/// storage version 2") and leaving every checksum as it was. Returns the
/// number of records rewritten.
fn rewrite_bounds(b: &mut [u8]) -> usize {
    assert_eq!(&b[..8], b"BWSEGMNT", "a storage version 2 segment");
    let header_len = u32_at(b, 16);
    let header: Value = serde_json::from_slice(&b[20..20 + header_len]).unwrap();
    assert_eq!(header["storage_version"], 2);
    assert_eq!(header["layout"], "keyed");
    assert_eq!(header["key_spec"]["kind"], "bloom");
    let index = u64_at(b, b.len() - 32);
    let blocks = u32_at(b, index + 17);
    let mut at = index + 21;
    let mut records = Vec::new();
    for _ in 0..blocks {
        at += 4 + u32_at(b, at); // first_key
        at += 4 + u32_at(b, at); // last_key
        at += 8 + 4 + 8; // bloom_off, bloom_len, bloom_checksum
        let body = u64_at(b, at);
        at += 8 + 4 + 8 + 4; // body_off, body_len, body_checksum, n
        let n = u32_at(b, body);
        let key_bytes = u32_at(b, body + 4 + 4 * n);
        let values = body + 4 + 4 * (n + 1) + key_bytes;
        for i in 0..n {
            let e = values + 20 * i;
            records.push((u64_at(b, e), u32_at(b, e + 8)));
        }
    }
    for &(off, len) in &records {
        let bitmap_len = u32_at(b, off);
        let mut p = off + 4 + bitmap_len;
        assert_eq!((off + len - p) % 16, 0, "bitmap | bounds");
        while p < off + len {
            b[p..p + 8].copy_from_slice(&(-1i64).to_le_bytes());
            b[p + 8..p + 16].copy_from_slice(&(-1i64).to_le_bytes());
            p += 16;
        }
    }
    records.len()
}

#[test]
fn bloom_bounds_rewritten_in_place_are_refused_not_planned() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    corpus(&data, 3, 5, 20, 7);
    let k = dir.path().join("k.parquet");
    keys(&k, "id", &[3]);
    run(&["index", path(&data), "--key", "bloom:id@0.01@ts"], 0);
    let window = [
        "--json",
        "locate",
        path(&data),
        "--keys",
        path(&k),
        "--from",
        "0",
        "--to",
        "60000000",
    ];
    let clean: Value = serde_json::from_str(run(&window, 0).trim()).unwrap();
    let planned: usize = clean["evidence"]["record"]["body"]["plan"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["row_groups"].as_array().unwrap().len())
        .sum();
    assert!(planned > 0, "the window holds rows of key 3");

    let idx = index_dir(&data);
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(idx.join("manifest.json")).unwrap()).unwrap();
    let bounds: Vec<&Value> = manifest["segments"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == "bounds")
        .collect();
    assert!(!bounds.is_empty(), "the bloom's bounds segments are listed");
    let mut rewritten = 0;
    for s in bounds {
        assert_eq!(s["key_spec"], "bloom:id@0.01@ts");
        let file = idx
            .join("segments")
            .join(format!("{}.seg", s["id"].as_str().unwrap()));
        let mut b = std::fs::read(&file).unwrap();
        rewritten += rewrite_bounds(&mut b);
        std::fs::write(&file, b).unwrap();
    }
    assert!(rewritten > 0);

    let out = bw().args(window).output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_ne!(out.status.code(), Some(0), "refused, not planned: {stderr}");
    assert!(out.stdout.is_empty(), "no plan is printed");
    assert!(
        stderr.contains("corrupt segment"),
        "the refusal names the corrupt segment: {stderr}"
    );
}
