//! Fingerprint drift detection (ADR 013).

#![allow(clippy::unwrap_used, missing_docs)]

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use bellwether_source::{
    drift, fingerprint, fingerprint_with_policy, FingerprintPolicy, LocalFs, ParquetSource,
    TAIL_BYTES,
};
use bellwether_types::{Error, Source};

fn scratch(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "bellwether-source-fingerprint-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

/// Overwrites `path` with `bytes` and restores its previous mtime, so only
/// the content changes.
fn rewrite_keep_mtime(path: &PathBuf, bytes: &[u8]) {
    let mtime = std::fs::metadata(path).unwrap().modified().unwrap();
    let mut f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    f.write_all(bytes).unwrap();
    f.set_modified(mtime).unwrap();
}

#[test]
fn unchanged_file_does_not_drift() {
    let p = scratch("same.bin");
    std::fs::write(&p, b"hello world").unwrap();
    let a = fingerprint(p.to_str().unwrap()).unwrap();
    let b = fingerprint(p.to_str().unwrap()).unwrap();
    assert!(!drift(&a, &b));
    assert_eq!(a.size, 11);
    let etag = a.etag.as_deref().unwrap();
    assert!(etag.ends_with('Z') && etag.len() == 30, "{etag}");
}

#[test]
fn same_size_same_mtime_rewrite_is_caught_by_tail_hash() {
    let p = scratch("tail.bin");
    std::fs::write(&p, b"aaaaaaaaaa").unwrap();
    let before = fingerprint(p.to_str().unwrap()).unwrap();
    rewrite_keep_mtime(&p, b"aaaaaaaaab");
    let after = fingerprint(p.to_str().unwrap()).unwrap();
    assert_eq!(before.size, after.size);
    assert_eq!(before.etag, after.etag);
    assert_ne!(before.tail_blake3, after.tail_blake3);
    assert!(drift(&before, &after));
}

#[test]
fn tail_hash_covers_only_last_64_kib() {
    let p = scratch("big.bin");
    let mut data = vec![0u8; 3 * 64 * 1024];
    std::fs::write(&p, &data).unwrap();
    let before = fingerprint(p.to_str().unwrap()).unwrap();
    let tail_start = data.len() - usize::try_from(TAIL_BYTES).unwrap();
    assert_eq!(
        before.tail_blake3,
        bellwether_types::Hash::of(&data[tail_start..])
    );
    data[tail_start] = 1;
    rewrite_keep_mtime(&p, &data);
    assert!(drift(&before, &fingerprint(p.to_str().unwrap()).unwrap()));
}

#[test]
fn rewritten_parquet_file_drifts_and_stale_reads_are_refused() {
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/source");
    let p = scratch("rewritten.parquet");
    std::fs::copy(fixtures.join("multi_rg.parquet"), &p).unwrap();
    let path = p.to_str().unwrap().to_string();

    let source = ParquetSource::new();
    let old = fingerprint(&path).unwrap();
    let old_rgs = source.row_groups(&old).unwrap();
    assert_eq!(old_rgs.len(), 3);

    // Upstream rewrites the file in place with different contents.
    std::fs::copy(fixtures.join("timestamps.parquet"), &p).unwrap();
    let new = fingerprint(&path).unwrap();
    assert!(drift(&old, &new));

    // A fresh source reading with the stale fingerprint refuses.
    let fresh = ParquetSource::new();
    let err = fresh.row_groups(&old).unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("drift")),
        "{err}"
    );
    // The cached source also refuses to decode with stale offsets.
    let err = source
        .read_columns(&old_rgs[0], &["id".into()])
        .unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("drift")),
        "{err}"
    );
    // The new fingerprint reads the new contents.
    assert_eq!(fresh.row_groups(&new).unwrap().len(), 2);
}

#[test]
fn different_paths_are_not_the_same_file() {
    let a = scratch("a.bin");
    let b = scratch("b.bin");
    std::fs::write(&a, b"x").unwrap();
    std::fs::write(&b, b"x").unwrap();
    let fa = fingerprint(a.to_str().unwrap()).unwrap();
    let fb = fingerprint(b.to_str().unwrap()).unwrap();
    assert!(drift(&fa, &fb));
}

#[test]
fn same_size_same_mtime_rewrite_after_caching_is_refused() {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/source");
    let p = scratch("cached-then-rewritten.parquet");
    std::fs::copy(fixtures.join("multi_rg.parquet"), &p).unwrap();
    let path = p.to_str().unwrap().to_string();

    // Cache the footer and the row groups.
    let source = ParquetSource::new();
    let fp = fingerprint(&path).unwrap();
    let rgs = source.row_groups(&fp).unwrap();
    assert_eq!(
        source
            .read_columns(&rgs[0], &["id".into()])
            .unwrap()
            .num_rows(),
        4
    );

    // Alter one byte inside rg0's `id` column chunk, keeping size and mtime
    // (what `cp -p`, `rsync -t`, or a coarse-mtime filesystem produce).
    let mut bytes = std::fs::read(&p).unwrap();
    let reader = SerializedFileReader::new(std::fs::File::open(&p).unwrap()).unwrap();
    let (start, len) = reader.metadata().row_group(0).column(0).byte_range();
    let at = usize::try_from(start + len / 2).unwrap();
    bytes[at] ^= 0xFF;
    rewrite_keep_mtime(&p, &bytes);
    let after = fingerprint(&path).unwrap();
    assert_eq!((after.size, &after.etag), (fp.size, &fp.etag));

    // The cached source refuses to decode with the stale footer...
    let err = source.read_columns(&rgs[0], &["id".into()]).unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("Tail64K content hash")),
        "{err}"
    );
    // ...and a fresh source refuses to load a footer for the old fingerprint.
    let err = ParquetSource::new().row_groups(&fp).unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("Tail64K content hash")),
        "{err}"
    );
}

/// Writes a ~480 KB Parquet file: 60 000 plain, uncompressed Int64 values in
/// three row groups, so its first data page lies far outside the last 64 KiB.
fn write_large_parquet(path: &PathBuf) {
    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, Encoding};
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int64Array::from((0..60_000_i64).collect::<Vec<_>>())) as ArrayRef,
    )])
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(20_000))
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// The inherent limit of the default policy, and the policy that closes it.
/// The patch is at byte offset 109 of a ~480 KB file, with size and mtime
/// unchanged.
#[test]
fn full_policy_catches_a_patch_outside_the_tail() {
    let p = scratch("patched-head.parquet");
    write_large_parquet(&p);
    let path = p.to_str().unwrap().to_string();
    let tail_before = fingerprint(&path).unwrap();
    let full_before = fingerprint_with_policy(&LocalFs, &path, FingerprintPolicy::Full).unwrap();
    assert!(tail_before.size > 4 * TAIL_BYTES);
    assert_ne!(tail_before.tail_blake3, full_before.tail_blake3);

    let source = ParquetSource::new().with_fingerprint_policy(FingerprintPolicy::Full);
    let rgs = source.row_groups(&full_before).unwrap();
    assert_eq!(rgs.len(), 3);
    assert_eq!(
        source
            .read_columns(&rgs[0], &["id".into()])
            .unwrap()
            .num_rows(),
        20_000
    );

    let mut bytes = std::fs::read(&p).unwrap();
    bytes[109] ^= 0xFF;
    rewrite_keep_mtime(&p, &bytes);

    // Tail64K: the patch is outside the hashed tail, so no drift (ADR 013).
    let tail_after = fingerprint(&path).unwrap();
    assert!(!drift(&tail_before, &tail_after));
    // Full: the patch is drift, and reads are refused.
    let full_after = fingerprint_with_policy(&LocalFs, &path, FingerprintPolicy::Full).unwrap();
    assert!(drift(&full_before, &full_after));
    let err = source.read_columns(&rgs[0], &["id".into()]).unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("Full content hash")),
        "{err}"
    );
}

/// Up to 64 KiB both policies hash the whole file, so they agree; a source
/// under `Full` reads the shared fixtures like the default one.
#[test]
fn policies_agree_on_small_files() {
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/source");
    let root = fixtures.to_str().unwrap();
    let full = ParquetSource::new().with_fingerprint_policy(FingerprintPolicy::Full);
    let found = full.discover(root).unwrap();
    assert_ne!(found.len(), 0);
    for fp in &found {
        assert!(fp.size <= TAIL_BYTES);
        assert_eq!(fp, &fingerprint(&fp.path).unwrap(), "{}", fp.path);
        let rgs = full.row_groups(fp).unwrap();
        let first = rgs[0].stats[0].column.clone();
        let batch = full.read_columns(&rgs[0], &[first]).unwrap();
        assert_eq!(u64::try_from(batch.num_rows()).unwrap(), rgs[0].rows);
    }
}
