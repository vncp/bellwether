//! Byte compatibility with Parquet's embedded split-block bloom filters
//! (ADR 008). A file is written with the `parquet` crate's writer; its raw
//! bitset is read from the file and (1) equals, byte for byte, the filter this
//! crate builds from the same values and sizing, and (2) answers every probe,
//! members and non-members alike, exactly as the `parquet` crate's own reader
//! does.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::needless_for_each,
    clippy::case_sensitive_file_extension_comparisons
)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use bellwether_core::bloom::{num_bytes_for, parquet_plain, PlainHash, Sbbf};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{ReaderProperties, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;

const NDV: u64 = 300;
const FPP: f64 = 0.05;

fn write_file() -> (Bytes, Vec<i64>, Vec<String>, Vec<i32>) {
    let ids: Vec<i64> = (0..NDV as i64).map(|i| i * 7 - 1000).collect();
    let names: Vec<String> = (0..NDV).map(|i| format!("key-{i}\0x")).collect();
    let small: Vec<i32> = (0..NDV as i32).map(|i| i * 3).collect();
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(ids.clone())) as ArrayRef),
        (
            "name",
            Arc::new(StringArray::from(names.clone())) as ArrayRef,
        ),
        (
            "small",
            Arc::new(Int32Array::from(small.clone())) as ArrayRef,
        ),
    ])
    .unwrap();
    let props = WriterProperties::builder()
        .set_bloom_filter_enabled(true)
        .set_bloom_filter_ndv(NDV)
        .set_bloom_filter_fpp(FPP)
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    (Bytes::from(buf), ids, names, small)
}

fn reader(file: &Bytes) -> SerializedFileReader<Bytes> {
    let opts = ReadOptionsBuilder::new()
        .with_reader_properties(
            ReaderProperties::builder()
                .set_read_bloom_filter(true)
                .build(),
        )
        .build();
    SerializedFileReader::new_with_options(file.clone(), opts).unwrap()
}

/// The raw bitset of column `col` in row group 0: the tail of the bloom
/// filter region after its thrift header.
fn raw_bitset(file: &Bytes, col: usize) -> Vec<u8> {
    let r = reader(file);
    let meta = r.metadata().row_group(0).column(col);
    let off = usize::try_from(meta.bloom_filter_offset().unwrap()).unwrap();
    let len = usize::try_from(meta.bloom_filter_length().unwrap()).unwrap();
    let region = &file[off..off + len];
    let n = num_bytes_for(NDV, FPP);
    assert!(
        region.len() > n && region.len() - n <= 20,
        "header is at most 20 bytes"
    );
    region[len - n..].to_vec()
}

#[test]
fn our_filter_is_byte_identical_to_parquets() {
    let (file, ids, names, small) = write_file();

    let mut ours = Sbbf::<PlainHash>::with_ndv_fpp(NDV, FPP).unwrap();
    ids.iter()
        .for_each(|v| ours.insert_hash(parquet_plain::hash_i64(*v)));
    assert_eq!(ours.to_bytes(), raw_bitset(&file, 0), "int64 column");

    let mut ours = Sbbf::<PlainHash>::with_ndv_fpp(NDV, FPP).unwrap();
    names
        .iter()
        .for_each(|v| ours.insert_hash(parquet_plain::hash_bytes(v.as_bytes())));
    assert_eq!(ours.to_bytes(), raw_bitset(&file, 1), "utf8 column");

    let mut ours = Sbbf::<PlainHash>::with_ndv_fpp(NDV, FPP).unwrap();
    small
        .iter()
        .for_each(|v| ours.insert_hash(parquet_plain::hash_i32(*v)));
    assert_eq!(ours.to_bytes(), raw_bitset(&file, 2), "int32 column");
}

#[test]
fn probes_agree_with_parquet_reader_both_ways() {
    let (file, ids, names, _) = write_file();
    let r = reader(&file);
    let rg = r.get_row_group(0).unwrap();

    // Parquet's filter probed by us, and our filter probed by parquet's
    // reader: since the bitsets are identical, every answer must match,
    // including false positives.
    let pq_ids = rg.get_column_bloom_filter(0).unwrap();
    let bits = raw_bitset(&file, 0);
    let ours = Sbbf::<PlainHash>::from_bytes(&bits).unwrap();
    for v in -5000i64..5000 {
        let h = parquet_plain::hash_i64(v);
        let theirs = pq_ids.check(&v);
        assert_eq!(theirs, ours.check_hash(h), "probe {v}");
    }
    for v in &ids {
        assert!(ours.check_hash(parquet_plain::hash_i64(*v)));
    }

    let pq_names = rg.get_column_bloom_filter(1).unwrap();
    let bits = raw_bitset(&file, 1);
    let ours = Sbbf::<PlainHash>::from_bytes(&bits).unwrap();
    for i in 0..2000 {
        let v = format!("key-{i}\0x");
        let theirs = pq_names.check(&v.as_str());
        assert_eq!(
            theirs,
            ours.check_hash(parquet_plain::hash_bytes(v.as_bytes()))
        );
    }
    for v in &names {
        assert!(ours.check_hash(parquet_plain::hash_bytes(v.as_bytes())));
    }
}
