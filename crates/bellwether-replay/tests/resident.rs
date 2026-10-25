//! Resident memory per key (contract v3 entry 32), measured rather than
//! assumed: the caps in build and replay count accounted key bytes, and this
//! measures what one key really costs in resident memory.
//!
//! - **Open writer:** a [`SegmentWriter`] built for the dataset, as a
//!   transient build opens it, fed `KEYS` distinct `UInt64` keys in row
//!   groups of 65,536 rows through `add_batch`, and kept open.
//! - **Layout map:** the map of distinct cluster keys the layout model holds
//!   (`layout.rs`, `cluster_positions`: a `BTreeMap<Vec<u8>, u64>`), filled
//!   with the same number of 9-byte encoded keys, each charged its bytes
//!   plus [`CLUSTER_KEY_OVERHEAD`].
//!
//! Resident memory is read from the operating system (`/proc/self/statm` on
//! Linux, `ps -o rss=` elsewhere) before and after, with the structure still
//! alive. Ignored by default: the number depends on the build profile and
//! the allocator, so it is measured in release by `scripts/perf.sh`:
//!
//! ```text
//! cargo test --release -p bellwether-replay --test resident -- --ignored --nocapture
//! ```
//!
//! Each case prints one `RESIDENT {json}` line.

#![allow(clippy::unwrap_used, missing_docs, clippy::cast_precision_loss)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::segment::{SegmentWriter, WriterOptions};
use bellwether_replay::CLUSTER_KEY_OVERHEAD;
use bellwether_types::{KeySpec, RowGroupId};

/// Keys per measurement: the transient build's default `max_segment_rows`
/// (2^20), so one open writer holds them all.
const KEYS: u64 = 1 << 20;
const ROWS_PER_ROW_GROUP: u64 = 1 << 16;
/// Accounted bytes of one `UInt64` key: the type tag and 8 big-endian bytes.
const KEY_BYTES: u64 = 9;

/// Resident bytes of this process, now.
fn resident_bytes() -> u64 {
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
        let pages: u64 = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
        return pages * 4096;
    }
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    let kib: u64 = String::from_utf8(out.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    kib * 1024
}

fn report(case: &str, keys: u64, accounted_per_key: u64, before: u64, after: u64) {
    let per_key = after.saturating_sub(before) as f64 / keys as f64;
    println!(
        "RESIDENT {}",
        serde_json::json!({
            "case": case,
            "keys": keys,
            "resident_before_bytes": before,
            "resident_after_bytes": after,
            "resident_bytes_per_key": (per_key * 10.0).round() / 10.0,
            "accounted_bytes_per_key": accounted_per_key,
            "resident_over_accounted": (per_key / accounted_per_key as f64 * 100.0).round() / 100.0,
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        })
    );
}

#[test]
#[ignore = "a measurement; run in release by scripts/perf.sh"]
fn open_writer_resident_bytes_per_key() {
    let schema = Schema::new(vec![Field::new("id", DataType::UInt64, false)]);
    let spec = KeySpec::Exact {
        columns: vec!["id".into()],
    };
    let schema_ref = Arc::new(schema.clone());
    let before = resident_bytes();
    let mut writer = SegmentWriter::for_dataset(spec, &schema, WriterOptions::default()).unwrap();
    for rg in 0..KEYS / ROWS_PER_ROW_GROUP {
        let start = rg * ROWS_PER_ROW_GROUP;
        let ids = UInt64Array::from_iter_values(start..start + ROWS_PER_ROW_GROUP);
        let batch = RecordBatch::try_new(schema_ref.clone(), vec![Arc::new(ids)]).unwrap();
        writer
            .add_batch(RowGroupId(u32::try_from(rg).unwrap()), &batch)
            .unwrap();
    }
    let after = resident_bytes();
    report("open_writer_exact_u64", KEYS, KEY_BYTES, before, after);
    // The writer is still open when resident memory was read.
    drop(writer);
}

#[test]
#[ignore = "a measurement; run in release by scripts/perf.sh"]
fn layout_map_resident_bytes_per_key() {
    let before = resident_bytes();
    let mut map: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
    for k in 0..KEYS {
        let mut key = Vec::with_capacity(1 + 8);
        key.push(0x05);
        key.extend_from_slice(&k.to_be_bytes());
        map.insert(key, 1);
    }
    let after = resident_bytes();
    report(
        "layout_map_9_byte_keys",
        KEYS,
        KEY_BYTES + CLUSTER_KEY_OVERHEAD,
        before,
        after,
    );
    assert_eq!(map.len() as u64, KEYS);
}
