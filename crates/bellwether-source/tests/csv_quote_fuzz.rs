//! Seeded property test for CSV quoting across chunk boundaries.
//!
//! Generates random valid two-column CSV files that mix the inputs which
//! defeated or stressed earlier versions of the quoted-newline guard:
//!
//! - bare quotes inside unquoted fields (inch marks such as `5'10"`), which
//!   defeated a quote-parity count;
//! - `""` escapes inside quoted fields, including next to embedded newlines;
//! - a closing quote followed by another byte (`"ab"cd`), which drops the
//!   field back to unquoted;
//! - quoted fields containing `,`, `\n`, `\r\n`, and a lone `\r`;
//! - `\n`, `\r\n`, and lone `\r` record terminators;
//! - blank lines, empty fields, empty quoted fields, and a missing final
//!   newline.
//!
//! Each file is read through `ChunkedTextSource` at every chunk size from 1 to
//! one past its length. Every read must either fail with an error or return
//! exactly the rows arrow's CSV reader produces for the whole file. A silent
//! difference (a phantom or missing row, or a changed value) fails the test.
//! A single chunk covering the whole file must always succeed.
//!
//! The run is deterministic: the RNG seed is fixed unless `PROPTEST_RNG_SEED`
//! is set, and the case count defaults to [`DEFAULT_CASES`] unless
//! `PROPTEST_CASES` is set. Failing cases are not persisted to disk.

#![allow(clippy::unwrap_used, missing_docs)]

use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_source::{fingerprint, ChunkedTextSource, TextFormat};
use bellwether_types::Source;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, RngSeed};

/// Cases per run when `PROPTEST_CASES` is unset. Sized to keep the test well
/// under 10 s in a debug build.
const DEFAULT_CASES: u32 = 64;

/// RNG seed when `PROPTEST_RNG_SEED` is unset.
const DEFAULT_SEED: u64 = 0x0b31_1e7e_5eed_c5f0;

fn config() -> ProptestConfig {
    // `default()` already applies PROPTEST_* environment overrides; fill in
    // this file's defaults only where the environment says nothing.
    let mut config = ProptestConfig::default();
    if std::env::var_os("PROPTEST_CASES").is_none() {
        config.cases = DEFAULT_CASES;
    }
    if std::env::var_os("PROPTEST_RNG_SEED").is_none() {
        config.rng_seed = RngSeed::Fixed(DEFAULT_SEED);
    }
    config.failure_persistence = None;
    config
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Utf8, true),
    ]))
}

type Rows = Vec<(Option<String>, Option<String>)>;

fn rows_of(batch: &RecordBatch, out: &mut Rows) {
    let k = batch.column(0).as_string::<i32>();
    let v = batch.column(1).as_string::<i32>();
    for i in 0..batch.num_rows() {
        out.push((
            (!k.is_null(i)).then(|| k.value(i).to_string()),
            (!v.is_null(i)).then(|| v.value(i).to_string()),
        ));
    }
}

/// Arrow's parse of the whole file, the reference every chunked read must
/// reproduce.
fn whole_file(text: &str) -> Rows {
    let reader = arrow::csv::ReaderBuilder::new(schema())
        .with_header(true)
        .build(Cursor::new(text.as_bytes()))
        .unwrap();
    let mut out = Rows::new();
    for batch in reader {
        rows_of(&batch.unwrap(), &mut out);
    }
    out
}

/// Every row of the file read through the source at `chunk_size`, or the
/// first error.
fn chunked(path: &str, chunk_size: u64) -> bellwether_types::Result<Rows> {
    let fp = fingerprint(path)?;
    let source = ChunkedTextSource::new(TextFormat::csv())
        .with_schema(schema())
        .with_chunk_size(chunk_size);
    let mut out = Rows::new();
    for rg in source.row_groups(&fp)? {
        rows_of(
            &source.read_columns(&rg, &["k".into(), "v".into()])?,
            &mut out,
        );
    }
    Ok(out)
}

/// The file every case is written to, private to this test process.
fn case_path() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "bellwether-source-csv-quote-fuzz-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("case.csv")
}

/// An unquoted field. It never starts with a quote (that would open a quoted
/// field), but may contain bare quotes, which are literal.
fn unquoted() -> impl Strategy<Value = String> {
    (
        prop::sample::select(vec!["a", "b", "'"]),
        prop::collection::vec(prop::sample::select(vec!["a", "\"", "'", "b"]), 0..4),
    )
        .prop_map(|(first, rest)| format!("{first}{}", rest.concat()))
}

/// A quoted field whose body mixes delimiters, line breaks, and `""`
/// escapes, optionally followed by bytes after the closing quote. Those start
/// with a non-quote byte, since a quote right after the closing quote would
/// be an escape instead.
fn quoted() -> impl Strategy<Value = String> {
    (
        prop::collection::vec(
            prop::sample::select(vec!["a", ",", "\n", "\r\n", "\r", "\"\"", " "]),
            0..6,
        ),
        prop::option::of(prop::collection::vec(
            prop::sample::select(vec!["x", "\""]),
            0..3,
        )),
    )
        .prop_map(|(body, after)| {
            let mut s = format!("\"{}\"", body.concat());
            if let Some(after) = after {
                s.push('x');
                s.push_str(&after.concat());
            }
            s
        })
}

fn field() -> impl Strategy<Value = String> {
    prop_oneof![
        1 => Just(String::new()),
        1 => Just("\"\"".to_string()),
        3 => unquoted(),
        5 => quoted(),
    ]
}

fn terminator() -> impl Strategy<Value = &'static str> {
    prop::sample::select(vec!["\n", "\r\n", "\r"])
}

/// One record: an optional blank line before it, two fields, and the
/// terminator that follows it.
fn record() -> impl Strategy<Value = (bool, String, String, &'static str)> {
    (prop::bool::weighted(0.15), field(), field(), terminator())
}

/// A whole file: a `k,v` header, one to five records, and an optional final
/// terminator.
fn csv_file() -> impl Strategy<Value = String> {
    (
        terminator(),
        prop::collection::vec(record(), 1..6),
        any::<bool>(),
    )
        .prop_map(|(header_end, records, final_terminator)| {
            let mut text = format!("k,v{header_end}");
            let last = records.len() - 1;
            for (i, (blank_before, k, v, end)) in records.into_iter().enumerate() {
                if blank_before {
                    text.push('\n');
                }
                text.push_str(&k);
                text.push(',');
                text.push_str(&v);
                if i < last || final_terminator {
                    text.push_str(end);
                }
            }
            text
        })
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn every_chunk_size_errors_or_matches_the_whole_file_parse(text in csv_file()) {
        let want = whole_file(&text);
        let path = case_path();
        std::fs::write(&path, &text).unwrap();
        let path = path.to_str().unwrap().to_string();
        let len = u64::try_from(text.len()).unwrap();
        for chunk_size in 1..=len + 1 {
            match chunked(&path, chunk_size) {
                // A refusal is always allowed for a split chunk.
                Err(e) => prop_assert!(
                    chunk_size < len,
                    "one chunk over the whole file must read, got {e} for {text:?}"
                ),
                Ok(got) => prop_assert_eq!(
                    &got,
                    &want,
                    "chunk size {} returned other rows than the whole-file parse for {:?}",
                    chunk_size,
                    text
                ),
            }
        }
    }
}
