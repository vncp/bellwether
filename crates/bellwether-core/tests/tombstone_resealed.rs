//! Tombstone segments (B8) a buggy or hostile writer produced:
//!
//! - liveness entries whose bytes are corrupt but whose checksums were
//!   recomputed are refused by the entry rules (live after newest, a flag
//!   contradicting the times, unknown flag bits, a live time without its
//!   flag), never used to prune: the every-byte sweep cannot reach those
//!   rules, since the checksums refuse first (PR #33 review, mutant M5).
//!   A reader holds the rules twice: `Liveness::read` checks each entry,
//!   and `KeyValue::decode` checks the decoded value, entries included,
//!   before it is used. Dropping either one alone leaves this test
//!   passing, since the other refuses the same entries with the same
//!   message (M5 is equivalent here); dropping the one in `Liveness::read`
//!   fails core's `entries_round_trip_and_inconsistent_ones_are_refused`,
//!   and dropping both makes this test fail, accepting the three entries
//!   only those checks refuse (live after newest, and the flag both ways);
//! - two segments holding one key in one row group are refused by merge,
//!   never unioned (`docs/format.md`, "Tombstones"; mutant M14).
#![allow(clippy::unwrap_used, missing_docs, clippy::cast_possible_truncation)]

use bellwether_core::encoding::{encode_tuple, Value};
use bellwether_core::segment::{checksum, PREAMBLE_LEN};
use bellwether_core::tombstone;
use bellwether_core::{merge, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{KeySpec, RowGroupId, Tombstone};

fn key(s: &str) -> Vec<u8> {
    encode_tuple(&[Value::Utf8(s)]).unwrap()
}

fn clean() -> Vec<u8> {
    let spec = KeySpec::Temporal {
        columns: vec!["sym".into()],
        ts_column: "ts".into(),
        counts: false,
        tombstone: Some(Tombstone {
            column: "void".into(),
            value: None,
        }),
    };
    let mut w = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
    w.set_rowgroup_range(RowGroupId(0)..RowGroupId(3)).unwrap();
    let a = key("A");
    let b = key("B");
    w.insert_liveness(&a, RowGroupId(0), (5, 0), true).unwrap();
    w.insert_liveness(&a, RowGroupId(0), (7, 0), false).unwrap();
    w.insert_liveness(&b, RowGroupId(1), (4, 0), false).unwrap();
    w.insert_liveness(&a, RowGroupId(2), (9, 0), true).unwrap();
    w.finish().unwrap().bytes
}

fn find_all(hay: &[u8], needle: &[u8]) -> Vec<usize> {
    (0..=hay.len().saturating_sub(needle.len()))
        .filter(|i| &hay[*i..*i + needle.len()] == needle)
        .collect()
}

fn u64_at(b: &[u8], p: usize) -> u64 {
    u64::from_le_bytes(b[p..p + 8].try_into().unwrap())
}
fn u32_at(b: &[u8], p: usize) -> u32 {
    u32::from_le_bytes(b[p..p + 4].try_into().unwrap())
}

/// Replaces `old` (a value record) with `new` (same length) and reseals
/// the value checksum, the block body checksums and the footer checksum.
fn reseal(seg: &[u8], old: &[u8], new: &[u8]) -> Vec<u8> {
    assert_eq!(old.len(), new.len());
    let mut b = seg.to_vec();
    let n = b.len();
    let index_off = u64_at(&b, n - 32) as usize;
    let index_len = u64_at(&b, n - 24) as usize;
    // every (off u64, len u32, sum u64) in the index whose sum matches its range
    let mut sealed = Vec::new();
    for p in index_off..index_off + index_len - 20 {
        let (o, l) = (u64_at(&b, p) as usize, u32_at(&b, p + 8) as usize);
        if o.checked_add(l).is_some_and(|e| e <= n)
            && l > 0
            && checksum(&[&b[o..o + l]]) == u64_at(&b, p + 12)
        {
            sealed.push((p, o, l));
        }
    }
    let at = find_all(&b, old);
    assert_eq!(at.len(), 1, "record found {} times", at.len());
    let at = at[0];
    b[at..at + new.len()].copy_from_slice(new);
    let mut r = (at as u64).to_le_bytes().to_vec();
    r.extend_from_slice(&(old.len() as u32).to_le_bytes());
    r.extend_from_slice(&checksum(&[old]).to_le_bytes());
    let refs = find_all(&b, &r);
    assert_eq!(refs.len(), 1);
    let s = checksum(&[new]).to_le_bytes();
    b[refs[0] + 12..refs[0] + 20].copy_from_slice(&s);
    for (p, o, l) in sealed {
        let s = checksum(&[&b[o..o + l]]).to_le_bytes();
        b[p + 12..p + 20].copy_from_slice(&s);
    }
    let header_len = u32_at(&b, 16) as usize;
    let sum = checksum(&[
        &b[..PREAMBLE_LEN],
        &b[PREAMBLE_LEN..PREAMBLE_LEN + header_len],
        &b[index_off..index_off + index_len],
    ]);
    b[n - 16..n - 8].copy_from_slice(&sum.to_le_bytes());
    b
}

fn record_a() -> Vec<u8> {
    let mut v = vec![
        0x14, 0, 0, 0, 0x3a, 0x30, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0x10, 0, 0, 0, 0, 0, 2, 0,
    ];
    let e = |v: &mut Vec<u8>, newest: i64, live: i64, flags: u8| {
        v.extend_from_slice(&newest.to_le_bytes());
        v.extend_from_slice(&live.to_le_bytes());
        v.push(flags);
    };
    e(&mut v, 7, 5, 3);
    e(&mut v, 9, 9, 1);
    v
}

fn read(bytes: &[u8]) -> Result<String, String> {
    let r = SegmentReader::open(bytes.to_vec()).map_err(|e| e.to_string())?;
    let got = tombstone::liveness(&[r], &[key("A"), key("B")]).map_err(|e| e.to_string())?;
    Ok(format!("{got:?}"))
}

#[test]
fn resealed_corrupt_liveness_entries_are_refused() {
    let seg = clean();
    let old = record_a();
    assert!(read(&seg).is_ok());
    // the reseal itself is sound: the same record resealed reads the same
    assert_eq!(read(&reseal(&seg, &old, &old)), read(&seg));
    // flip the entry for rg 2 (newest 9 live 9 flags 01) into each corruption
    let base = old.len() - 17;
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    let mut live_after = old.clone();
    live_after[base + 8..base + 16].copy_from_slice(&10i64.to_le_bytes()); // live 10 > newest 9
    cases.push(("live after newest", live_after));
    let mut flag = old.clone();
    flag[base + 16] = 3; // says newest is a tombstone, but live == newest
    cases.push(("flag contradicts times", flag));
    let mut unknown = old.clone();
    unknown[base + 16] = 0x05;
    cases.push(("unknown flag bit", unknown));
    let mut no_flag = old.clone();
    no_flag[base + 16] = 0x02; // live time stored without bit 0
    cases.push(("live time without bit 0", no_flag));
    let mut fake_dead = old.clone();
    // rg 0's entry flags 03 -> 01: claims a live row at newest 7 while live 5 < 7
    fake_dead[24 + 16] = 1;
    cases.push(("flag 0 while live < newest", fake_dead));
    let mut failures = Vec::new();
    for (what, rec) in cases {
        match read(&reseal(&seg, &old, &rec)) {
            Err(e) => eprintln!("{what}: refused: {e}"),
            Ok(v) => failures.push(format!("{what}: ACCEPTED {v}")),
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn merge_refuses_two_segments_holding_one_key_in_one_row_group() {
    let spec = KeySpec::Temporal {
        columns: vec!["sym".into()],
        ts_column: "ts".into(),
        counts: false,
        tombstone: Some(Tombstone {
            column: "void".into(),
            value: None,
        }),
    };
    let seg = |rg: u32, live: bool| {
        let mut w = SegmentWriter::new(spec.clone(), WriterOptions::default()).unwrap();
        w.set_rowgroup_range(RowGroupId(rg)..RowGroupId(rg + 1))
            .unwrap();
        w.insert_liveness(&key("A"), RowGroupId(rg), (5, 0), live)
            .unwrap();
        SegmentReader::open(w.finish().unwrap().bytes).unwrap()
    };
    // disjoint row groups merge
    merge::merge(&[seg(0, true), seg(1, false)], &WriterOptions::default()).unwrap();
    // one key in one row group in both is refused
    let err = merge::merge(&[seg(0, true), seg(0, false)], &WriterOptions::default())
        .unwrap_err()
        .to_string();
    assert!(err.contains("both hold key"), "{err}");
}
