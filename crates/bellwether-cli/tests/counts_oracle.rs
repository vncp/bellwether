//! Per-(key, row group) counts on both bench corpora, the versioned ledger
//! (`bellwether bench generate`) and the time-series `market` corpus, for
//! decimal, timestamp, string and unsigned keys, composite and temporal.
//!
//! Each side is computed independently of bellwether's reader:
//!
//! - the oracle decodes every row group of every data file with arrow and
//!   counts the rows of each key tuple in it, encoded as `docs/format.md`
//!   ("Canonical encoding") says (ADR 014: a tuple whose leading column is
//!   null is not indexed, a trailing null is the `0x00` marker);
//! - the index side reads `manifest.json` and decodes every segment listed
//!   under the spec's identity from its bytes, following only
//!   `docs/format.md` ("Segments, storage version 2"): preamble, header,
//!   footer, index, directory, value records, roaring bitmaps, LEB128
//!   counts. A (key, row group) counted by two segments fails the test.
//!
//! The two maps must be equal, so every count the build wrote is right, no
//! key or row group is missing or extra, and the document describes the
//! bytes well enough to read them.

#![allow(clippy::unwrap_used, missing_docs, clippy::cast_possible_truncation)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{
    DataType, Decimal128Type, Int64Type, TimeUnit, TimestampMicrosecondType, UInt64Type,
};
use bellwether::{spec, Engine, EngineOptions};
use bellwether_bench::corpus::GenParams;
use bellwether_bench::market::{generate_market, MarketParams, MarketRung};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;

/// `(canonical key, global row-group id) -> rows`.
type Counts = BTreeMap<(Vec<u8>, u32), u64>;

const MAGIC: &[u8] = b"BWSEGMNT";

/// Appends the canonical encoding of `col[i]` (encoding version 1).
fn encode(col: &dyn Array, i: usize, out: &mut Vec<u8>) {
    let flip64 = |v: i64| (v.cast_unsigned() ^ (1 << 63)).to_be_bytes();
    match col.data_type() {
        DataType::UInt64 => {
            out.push(0x03);
            out.extend(col.as_primitive::<UInt64Type>().value(i).to_be_bytes());
        }
        DataType::Int64 => {
            out.push(0x02);
            out.extend(flip64(col.as_primitive::<Int64Type>().value(i)));
        }
        DataType::Decimal128(_, _) => {
            let v = col.as_primitive::<Decimal128Type>().value(i);
            out.extend([0x05, 0x10]);
            out.extend((v.cast_unsigned() ^ (1 << 127)).to_be_bytes());
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            out.extend([0x08, 0x04, 0x03]);
            out.extend(flip64(
                col.as_primitive::<TimestampMicrosecondType>().value(i),
            ));
        }
        DataType::Utf8 => {
            out.push(0x06);
            for &b in col.as_string::<i32>().value(i).as_bytes() {
                out.push(b);
                if b == 0 {
                    out.push(0xff);
                }
            }
            out.extend([0x00, 0x00]);
        }
        other => panic!("no canonical encoding in this test for {other}"),
    }
}

/// Every data file of the manifest, with its row-group ids in file order.
fn files(data: &Path, manifest: &Value) -> Vec<(PathBuf, Vec<u32>)> {
    manifest["rowgroup_space"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            let p = PathBuf::from(f["fingerprint"]["path"].as_str().unwrap());
            let p = if p.is_absolute() { p } else { data.join(p) };
            let ids = f["rowgroup_ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as u32)
                .collect();
            (p, ids)
        })
        .collect()
}

fn retired(manifest: &Value) -> BTreeSet<u32> {
    manifest["rowgroup_space"]["tombstones"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|t| t["rowgroup_ids"].as_array().unwrap().clone())
        .map(|v| v.as_u64().unwrap() as u32)
        .collect()
}

/// The oracle: every live row group decoded, the rows of each key tuple.
fn oracle(data: &Path, manifest: &Value, columns: &[&str]) -> Counts {
    let dead = retired(manifest);
    let mut out = Counts::new();
    for (path, ids) in files(data, manifest) {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        assert_eq!(
            builder.metadata().num_row_groups(),
            ids.len(),
            "{}",
            path.display()
        );
        for (index, &id) in ids.iter().enumerate() {
            if dead.contains(&id) {
                continue;
            }
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
                .unwrap()
                .with_row_groups(vec![index])
                .build()
                .unwrap();
            for batch in reader {
                let batch: RecordBatch = batch.unwrap();
                let cols: Vec<_> = columns
                    .iter()
                    .map(|c| batch.column_by_name(c).unwrap().clone())
                    .collect();
                for row in 0..batch.num_rows() {
                    if cols[0].is_null(row) {
                        continue;
                    }
                    let mut key = Vec::new();
                    for c in &cols {
                        if c.is_null(row) {
                            key.push(0x00);
                        } else {
                            encode(c.as_ref(), row, &mut key);
                        }
                    }
                    *out.entry((key, id)).or_default() += 1;
                }
            }
        }
    }
    out
}

struct Bytes<'a>(&'a [u8]);

impl Bytes<'_> {
    fn u32(&self, at: usize) -> usize {
        u32::from_le_bytes(self.0[at..at + 4].try_into().unwrap()) as usize
    }
    fn u64(&self, at: usize) -> usize {
        usize::try_from(u64::from_le_bytes(self.0[at..at + 8].try_into().unwrap())).unwrap()
    }
    fn u16(&self, at: usize) -> u32 {
        u32::from(u16::from_le_bytes(self.0[at..at + 2].try_into().unwrap()))
    }
}

/// A roaring bitmap in the no-run serialized form: cookie `3a 30 00 00`,
/// containers u32, (key u16, cardinality - 1 u16) per container, an offset
/// u32 per container, then each container: sorted u16 values, or a 8 KiB
/// bitset when it holds more than 4096.
fn roaring(bm: &[u8]) -> Vec<u32> {
    let b = Bytes(bm);
    assert_eq!(&bm[..4], &[0x3a, 0x30, 0x00, 0x00], "no-run cookie");
    let containers = b.u32(4);
    let mut out = Vec::new();
    for c in 0..containers {
        let high = b.u16(8 + 4 * c) << 16;
        let card = b.u16(10 + 4 * c) as usize + 1;
        let at = b.u32(8 + 4 * containers + 4 * c);
        if card <= 4096 {
            out.extend((0..card).map(|j| high | b.u16(at + 2 * j)));
        } else {
            for word in 0..1024 {
                let w =
                    u64::from_le_bytes(bm[at + 8 * word..at + 8 * word + 8].try_into().unwrap());
                out.extend(
                    (0..64)
                        .filter(|bit| w >> bit & 1 == 1)
                        .map(|bit| high | (word * 64 + bit) as u32),
                );
            }
        }
    }
    assert!(!out.is_empty(), "an empty keyed bitmap");
    out
}

/// One unsigned LEB128 at `*at`, which it advances.
fn leb128(v: &[u8], at: &mut usize) -> u64 {
    let (mut n, mut shift) = (0u64, 0);
    loop {
        let byte = v[*at];
        *at += 1;
        n |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return n;
        }
        shift += 7;
    }
}

/// Every `((key, row group), count)` a counted keyed segment stores.
fn decode_segment(seg: &[u8]) -> Vec<((Vec<u8>, u32), u64)> {
    let b = Bytes(seg);
    let n = seg.len();
    assert_eq!(&seg[..8], MAGIC, "preamble magic");
    assert_eq!(b.u32(8), 2, "storage version");
    assert_eq!(b.u32(12), 1, "encoding version");
    let header: Value = serde_json::from_slice(&seg[20..20 + b.u32(16)]).unwrap();
    assert_eq!(header["key_spec"]["counts"], true, "{header}");
    let temporal = match header["layout"].as_str().unwrap() {
        "keyed" => false,
        "keyed_temporal" => true,
        other => panic!("layout {other}"),
    };
    assert_eq!(&seg[n - 8..], MAGIC, "footer magic");
    let (index, index_len) = (b.u64(n - 32), b.u64(n - 24));
    let entries = b.u64(index);
    let blocks = b.u32(index + 17);
    let mut p = index + 21;
    let mut out = Vec::new();
    for _ in 0..blocks {
        p += 4 + b.u32(p); // first_key
        p += 4 + b.u32(p); // last_key
        p += 8 + 4 + 8; // bloom_off, bloom_len, bloom_checksum
        let body = b.u64(p);
        p += 8 + 4 + 8; // body_off, body_len, body_checksum
        let keys = b.u32(p);
        p += 4;
        assert_eq!(b.u32(body), keys, "directory n");
        let ends: Vec<usize> = (0..=keys).map(|i| b.u32(body + 4 + 4 * i)).collect();
        assert_eq!(ends[0], 0);
        let key_bytes = body + 4 + 4 * (keys + 1);
        let values = key_bytes + ends[keys];
        for i in 0..keys {
            let key = seg[key_bytes + ends[i]..key_bytes + ends[i + 1]].to_vec();
            let (off, len) = (b.u64(values + 20 * i), b.u32(values + 20 * i + 8));
            let v = &seg[off..off + len];
            let bitmap_len = Bytes(v).u32(0);
            let rgs = roaring(&v[4..4 + bitmap_len]);
            let mut at = 4 + bitmap_len;
            for rg in rgs {
                let count = leb128(v, &mut at);
                assert!(count >= 1);
                out.push(((key.clone(), rg), count));
            }
            if temporal {
                let chain = Bytes(&v[at..]).u32(0);
                at += 4 + 1 + 8 + 12 * chain;
            }
            assert_eq!(at, len, "the value record ends with its counts or chain");
        }
    }
    assert_eq!(p, index + index_len, "index length");
    assert_eq!(
        out.iter()
            .map(|((k, _), _)| k)
            .collect::<BTreeSet<_>>()
            .len(),
        entries
    );
    out
}

/// What the segments listed under `identity` count, over live row groups.
fn stored(location: &Path, manifest: &Value, identity: &str) -> Counts {
    let dead = retired(manifest);
    let mut out = Counts::new();
    let mut segments = 0;
    for s in manifest["segments"].as_array().unwrap() {
        if s["key_spec"] != identity || s.get("kind").is_some() {
            continue;
        }
        segments += 1;
        let id = s["id"].as_str().unwrap();
        let bytes = std::fs::read(location.join("segments").join(format!("{id}.seg"))).unwrap();
        for ((key, rg), count) in decode_segment(&bytes) {
            if dead.contains(&rg) {
                continue;
            }
            assert!(
                out.insert((key, rg), count).is_none(),
                "{identity}: two segments count one key in row group {rg}"
            );
        }
    }
    assert!(segments > 0, "no segment listed under {identity}");
    out
}

/// Indexes `data` under `specs` and checks every spec's counts against the
/// oracle; returns the largest count seen.
fn check(data: &Path, specs: &[(&str, &[&str])]) -> u64 {
    let parsed: Vec<_> = specs.iter().map(|(s, _)| spec::parse(s).unwrap()).collect();
    Engine::open(data.to_str().unwrap(), EngineOptions::default())
        .unwrap()
        .index(&parsed)
        .unwrap();
    let location = data.join(".bellwether");
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(location.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["storage_version"], 2);
    let mut most = 0;
    for ((text, columns), spec) in specs.iter().zip(&parsed) {
        let identity = spec.identity();
        assert_eq!(&identity, text);
        let want = oracle(data, &manifest, columns);
        let got = stored(&location, &manifest, &identity);
        assert!(!want.is_empty(), "{identity}: the oracle found no key");
        if got != want {
            let missing = want.keys().filter(|k| !got.contains_key(*k)).count();
            let extra = got.keys().filter(|k| !want.contains_key(*k)).count();
            let wrong = want
                .iter()
                .filter(|(k, v)| got.get(*k).is_some_and(|g| g != *v))
                .count();
            panic!(
                "{identity}: {missing} (key, row group) pairs missing, {extra} extra, \
                 {wrong} with another count, of {}",
                want.len()
            );
        }
        most = most.max(want.values().copied().max().unwrap());
    }
    most
}

#[test]
fn ledger_corpus_counts_equal_the_decoded_files_per_key_and_row_group() {
    let dir = tempfile::tempdir().unwrap();
    let params = GenParams {
        seed: 7,
        files: 8,
        row_groups_per_file: 3,
        rows_per_row_group: 1500,
        entities: 4000,
        churn: 0.3,
        days: 4,
        payload_bytes: 8,
        late_fraction: 0.05,
    };
    let out = dir.path().join("ledger");
    bellwether_bench::generate::generate(&params, &out).unwrap();
    let most = check(
        &out.join("data"),
        &[
            // Decimal128(18, 2).
            ("exact:amount,counts", &["amount"]),
            // Timestamp(µs, UTC).
            ("exact:version_ts,counts", &["version_ts"]),
            // A string leading a decimal.
            ("exact:status,amount,counts", &["status", "amount"]),
            // Unsigned key, versions by a timestamp: counts over all versions.
            ("temporal:entity_id@version_ts,counts", &["entity_id"]),
        ],
    );
    assert!(most > 1, "some key holds several rows of one row group");
}

#[test]
fn market_corpus_counts_equal_the_decoded_files_per_key_and_row_group() {
    let dir = tempfile::tempdir().unwrap();
    let params = MarketParams {
        symbols: 20,
        days: 3,
        prints_per_symbol_per_day: 100,
        ..MarketParams::rung(MarketRung::Small)
    };
    let out = dir.path().join("market");
    generate_market(&out, &params).unwrap();
    let most = check(
        &out.join("data"),
        &[
            // Timestamp(µs, UTC), shared by a symbol's print and its
            // restatement.
            ("exact:ts,counts", &["ts"]),
            ("exact:symbol,ts,counts", &["symbol", "ts"]),
            ("temporal:symbol@ts,counts", &["symbol"]),
        ],
    );
    assert!(most > 1, "some key holds several rows of one row group");
}
