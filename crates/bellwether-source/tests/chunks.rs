//! Chunk boundaries of `ChunkedTextSource` never split a line.

#![allow(clippy::unwrap_used, clippy::format_push_string, missing_docs)]

use std::path::PathBuf;

use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use bellwether_source::{fingerprint, ChunkedTextSource, TextFormat};
use bellwether_types::{Error, Source};

fn scratch(name: &str, contents: &str) -> String {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("bellwether-source-chunks-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p.to_str().unwrap().to_string()
}

/// Lines of very different lengths so boundaries land everywhere.
fn csv_lines(n: i64) -> String {
    let mut s = String::from("id,payload\n");
    for i in 0..n {
        let pad = "x".repeat(usize::try_from((i * 7) % 23).unwrap());
        s.push_str(&format!("{i},p{pad}\n"));
    }
    s
}

fn check_boundaries(source: &ChunkedTextSource, path: &str, bytes: &[u8]) {
    let fp = fingerprint(path).unwrap();
    let ranges = source.chunk_ranges(&fp).unwrap();
    let size = u64::try_from(bytes.len()).unwrap();
    let mut expect_start = 0;
    for r in &ranges {
        assert_eq!(r.start, expect_start, "chunks are contiguous");
        assert!(r.end > r.start, "chunks are non-empty");
        let end = usize::try_from(r.end).unwrap();
        assert!(
            r.end == size || bytes[end - 1] == b'\n',
            "chunk {r:?} ends mid-line"
        );
        if r.end != size {
            assert!(
                r.end - r.start >= source.chunk_size(),
                "only the last chunk is short"
            );
        }
        expect_start = r.end;
    }
    assert_eq!(expect_start, size, "chunks cover the file");
}

#[test]
fn csv_chunks_never_split_a_line_at_any_chunk_size() {
    let text = csv_lines(40);
    let path = scratch("lines.csv", &text);
    // From the smallest size whose 4x cap fits the longest line (27 bytes).
    for chunk in 9..=u64::try_from(text.len()).unwrap() + 1 {
        let source = ChunkedTextSource::new(TextFormat::csv())
            .with_chunk_size(chunk)
            .with_stats_columns(["id"]);
        check_boundaries(&source, &path, text.as_bytes());
        let fp = fingerprint(&path).unwrap();
        let rgs = source.row_groups(&fp).unwrap();
        assert_eq!(rgs.iter().map(|r| r.rows).sum::<u64>(), 40, "chunk {chunk}");
        let mut ids = Vec::new();
        for rg in &rgs {
            let b = source.read_columns(rg, &["id".into()]).unwrap();
            assert_eq!(u64::try_from(b.num_rows()).unwrap(), rg.rows);
            ids.extend(
                b.column(0)
                    .as_primitive::<Int64Type>()
                    .values()
                    .iter()
                    .copied(),
            );
        }
        assert_eq!(ids, (0..40).collect::<Vec<i64>>(), "chunk {chunk}");
    }
}

#[test]
fn boundary_exactly_on_newline() {
    // "a\n" is 2 bytes; chunk size 2 makes target-1 land on the newline.
    let text = "id\n1\n2\n3\n";
    let path = scratch("exact.csv", text);
    let source = ChunkedTextSource::new(TextFormat::csv()).with_chunk_size(2);
    let fp = fingerprint(&path).unwrap();
    let ranges = source.chunk_ranges(&fp).unwrap();
    assert_eq!(ranges, vec![0..3, 3..5, 5..7, 7..9]);
    check_boundaries(&source, &path, text.as_bytes());
}

#[test]
fn jsonl_without_trailing_newline_and_with_crlf() {
    let text = "{\"id\":1}\r\n{\"id\":2}\r\n{\"id\":3}\r\n{\"id\":4}";
    let path = scratch("crlf.jsonl", text);
    // Lines are 10 bytes; the 4x cap needs chunk >= 3.
    for chunk in 3..=u64::try_from(text.len()).unwrap() {
        let source = ChunkedTextSource::new(TextFormat::JsonLines)
            .with_chunk_size(chunk)
            .with_stats_columns(["id"]);
        check_boundaries(&source, &path, text.as_bytes());
        let fp = fingerprint(&path).unwrap();
        let rgs = source.row_groups(&fp).unwrap();
        assert_eq!(rgs.iter().map(|r| r.rows).sum::<u64>(), 4, "chunk {chunk}");
    }
}

#[test]
fn empty_file_has_no_chunks() {
    let path = scratch("empty.jsonl", "");
    let source = ChunkedTextSource::new(TextFormat::JsonLines);
    let fp = fingerprint(&path).unwrap();
    assert_eq!(source.row_groups(&fp).unwrap().len(), 0);
}

#[test]
fn reading_through_a_different_chunk_size_is_refused() {
    let text = csv_lines(40);
    let path = scratch("resize.csv", &text);
    let fp = fingerprint(&path).unwrap();
    let built = ChunkedTextSource::new(TextFormat::csv()).with_chunk_size(64);
    let rgs = built.row_groups(&fp).unwrap();
    assert!(rgs.len() > 2);
    let other = ChunkedTextSource::new(TextFormat::csv()).with_chunk_size(128);
    for rg in &rgs {
        let err = other.read_columns(rg, &["id".into()]).unwrap_err();
        assert!(matches!(err, Error::Source(_)), "{err}");
    }
    // The same chunk size reads back fine.
    for rg in &rgs {
        let b = built.read_columns(rg, &["id".into()]).unwrap();
        assert_eq!(u64::try_from(b.num_rows()).unwrap(), rg.rows);
    }
    // byte_range reports the recorded chunk.
    assert_eq!(
        built.byte_range(&rgs[1]).unwrap(),
        built.chunk_ranges(&fp).unwrap()[1]
    );
}

#[test]
fn recorded_byte_range_pins_the_chunk_start() {
    // Fixed-width lines: a chunk built at one size can reappear at another
    // size with the same length and row count but a different start. Length
    // and row count alone cannot see that; the recorded byte range can.
    let text = csv_lines(60);
    let path = scratch("pinned.csv", &text);
    let fp = fingerprint(&path).unwrap();
    let built = ChunkedTextSource::new(TextFormat::csv()).with_chunk_size(64);
    let rgs = built.row_groups(&fp).unwrap();
    assert!(
        rgs.iter().all(|rg| rg.byte_range.is_some()),
        "chunked text records byte_range"
    );
    for rg in &rgs {
        let r = built.byte_range(rg).unwrap();
        assert_eq!(rg.byte_range, Some((r.start, r.end)));
    }
    // Forge a row group whose start is shifted by one byte but whose length
    // and row count are unchanged: the range check must refuse it even though
    // the older length and row-count checks would not.
    let mut forged = rgs[1].clone();
    let (s0, e0) = forged.byte_range.unwrap();
    forged.byte_range = Some((s0 + 1, e0 + 1));
    let err = built.read_columns(&forged, &["id".into()]).unwrap_err();
    assert!(matches!(err, Error::Source(_)), "{err}");
    assert!(err.to_string().contains("records"), "{err}");
    // The genuine row group still reads.
    let b = built.read_columns(&rgs[1], &["id".into()]).unwrap();
    assert_eq!(u64::try_from(b.num_rows()).unwrap(), rgs[1].rows);
}

#[test]
fn quoted_newline_split_by_a_boundary_is_an_error() {
    let text = "id,v\nk3,\"x\nk9,y\"\nk4,z\n";
    let path = scratch("quoted.csv", text);
    let fp = fingerprint(&path).unwrap();
    // A boundary right after `"x\n` splits the quoted field.
    let split = ChunkedTextSource::new(TextFormat::csv()).with_chunk_size(4);
    let err = split.row_groups(&fp).unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("quoted")),
        "{err}"
    );
    // One chunk holding the whole field parses it as one record.
    let whole = ChunkedTextSource::new(TextFormat::csv());
    let rgs = whole.row_groups(&fp).unwrap();
    assert_eq!(rgs.iter().map(|r| r.rows).sum::<u64>(), 2);
}

#[test]
fn chunks_are_capped_at_four_times_chunk_size() {
    // CR-only line endings: no `\n` anywhere.
    let text = "id\r1\r2\r3\r4\r5\r6\r7\r8\r9\r10\r11\r12\r";
    let path = scratch("cr-only.csv", text);
    let fp = fingerprint(&path).unwrap();
    let source = ChunkedTextSource::new(TextFormat::csv()).with_chunk_size(4);
    let err = source.row_groups(&fp).unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("no newline")),
        "{err}"
    );
    // A single long line.
    let long = format!("id\n{}\n", "9".repeat(100));
    let path = scratch("long-line.csv", &long);
    let fp = fingerprint(&path).unwrap();
    let source = ChunkedTextSource::new(TextFormat::csv()).with_chunk_size(8);
    assert!(source.row_groups(&fp).is_err());
    // Within the cap (104-byte file, 160-byte cap) it is one chunk, one row.
    let source = ChunkedTextSource::new(TextFormat::csv()).with_chunk_size(40);
    let rgs = source.row_groups(&fp).unwrap();
    assert_eq!((rgs.len(), rgs[0].rows), (1, 1));
}

#[test]
fn csv_header_must_match_supplied_schema_order() {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    let path = scratch("swapped.csv", "b,a\n1,2\n");
    let fp = fingerprint(&path).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let source = ChunkedTextSource::new(TextFormat::csv()).with_schema(Arc::clone(&schema));
    let err = source.row_groups(&fp).unwrap_err();
    assert!(
        matches!(err, Error::Source(ref m) if m.contains("header")),
        "{err}"
    );
    let path = scratch("ordered.csv", "a,b\n1,2\n");
    let fp = fingerprint(&path).unwrap();
    let source = ChunkedTextSource::new(TextFormat::csv()).with_schema(schema);
    assert_eq!(source.row_groups(&fp).unwrap()[0].rows, 1);
}

// ---------------------------------------------------------------------------
// Quoted fields: chunked parsing equals whole-file parsing, or errors.
// ---------------------------------------------------------------------------

fn two_utf8() -> std::sync::Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field, Schema};
    std::sync::Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Utf8, true),
    ]))
}

type Rows = Vec<(Option<String>, Option<String>)>;

/// Every row of every chunk, in order.
fn parse_all(source: &ChunkedTextSource, path: &str) -> bellwether_types::Result<Rows> {
    let fp = fingerprint(path)?;
    let mut out = Vec::new();
    for rg in source.row_groups(&fp)? {
        let b = source.read_columns(&rg, &["k".into(), "v".into()])?;
        let (k, v) = (
            b.column(0).as_string::<i32>(),
            b.column(1).as_string::<i32>(),
        );
        for i in 0..b.num_rows() {
            out.push((
                k.is_valid(i).then(|| k.value(i).to_string()),
                v.is_valid(i).then(|| v.value(i).to_string()),
            ));
        }
    }
    Ok(out)
}

fn csv_source(chunk: u64, has_header: bool) -> ChunkedTextSource {
    ChunkedTextSource::new(TextFormat::Csv {
        delimiter: b',',
        has_header,
    })
    .with_chunk_size(chunk)
    .with_schema(two_utf8())
}

fn is_cap_error(e: &Error) -> bool {
    matches!(e, Error::Source(m) if m.contains("no newline"))
}

fn is_quote_error(e: &Error) -> bool {
    matches!(e, Error::Source(m) if m.contains("quoted"))
}

/// The reviewer's case: inch marks (quotes inside unquoted fields) around a
/// quoted field that contains a newline. A quote-parity check accepted 6 of
/// these 38 chunk sizes and returned a phantom `k9` row.
#[test]
fn inch_marks_and_quoted_newline_never_yield_phantom_rows() {
    let text = "k,v\nk1,5'10\"\nk3,\"x\nk9,y\"\nk5,6'1\"\nk4,c\n";
    assert_eq!(text.len(), 38);
    let path = scratch("inch-marks.csv", text);
    let want = |k: &str, v: &str| (Some(k.to_string()), Some(v.to_string()));
    let whole = vec![
        want("k1", "5'10\""),
        want("k3", "x\nk9,y"),
        want("k5", "6'1\""),
        want("k4", "c"),
    ];
    let mut accepted = 0;
    for chunk in 1..=38 {
        match parse_all(&csv_source(chunk, true), &path) {
            Ok(rows) => {
                assert_eq!(rows, whole, "chunk size {chunk}");
                accepted += 1;
            }
            Err(e) => assert!(is_quote_error(&e) || is_cap_error(&e), "chunk {chunk}: {e}"),
        }
    }
    assert!(accepted > 0);
    // The splits that used to produce the phantom row are now refused.
    for chunk in [14, 15] {
        let err = parse_all(&csv_source(chunk, true), &path).unwrap_err();
        assert!(is_quote_error(&err), "chunk {chunk}: {err}");
    }
}

/// Quotes inside unquoted fields are literal, so they never cause an error.
#[test]
fn inch_marks_alone_are_never_rejected() {
    let text = "k,v\nk1,5'10\"\nk5,6'1\"\nk4,c\"\"\n";
    let path = scratch("inch-marks-only.csv", text);
    for chunk in 1..=u64::try_from(text.len()).unwrap() {
        match parse_all(&csv_source(chunk, true), &path) {
            Ok(rows) => assert_eq!(rows.len(), 3, "chunk {chunk}"),
            Err(e) => assert!(is_cap_error(&e), "chunk {chunk}: {e}"),
        }
    }
}

/// xorshift64*, so the randomized tests are deterministic and dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> usize {
        usize::try_from(self.next() % n).unwrap()
    }
}

/// One well-formed CSV field.
/// - Unquoted fields may hold quotes after the first byte (inch marks).
/// - Quoted fields may hold delimiters, escaped quotes, and (when `newlines` is set) newlines.
/// - A closed quote may be followed by more bytes, which `csv_core` reads as an unquoted continuation.
fn random_field(rng: &mut Rng, newlines: bool) -> String {
    match rng.below(4) {
        0 => {
            let mut s = String::from("a");
            for _ in 0..rng.below(4) {
                s.push(['a', '\'', '"', '5'][rng.below(4)]);
            }
            s
        }
        1 | 2 => {
            let mut s = String::from("\"");
            for _ in 0..rng.below(5) {
                match rng.below(4) {
                    0 => s.push(','),
                    1 => s.push_str("\"\""),
                    2 if newlines => s.push('\n'),
                    _ => s.push('b'),
                }
            }
            s.push('"');
            s
        }
        _ => String::from("\"q\"z\""),
    }
}

/// A two-column CSV file without a header: blank lines here and there,
/// and the final newline sometimes missing.
fn random_csv(rng: &mut Rng, newlines: bool) -> String {
    let mut s = String::new();
    for _ in 0..=rng.below(6) {
        if rng.below(6) == 0 {
            s.push('\n');
        }
        s.push_str(&random_field(rng, newlines));
        s.push(',');
        s.push_str(&random_field(rng, newlines));
        s.push('\n');
    }
    if rng.below(2) == 0 {
        s.pop();
    }
    s
}

/// For well-formed files, the whole-file parse is the oracle. At every chunk
/// size, chunked parsing returns exactly the same rows or refuses. It refuses
/// with a quote error only when a quoted field contains a newline.
#[test]
fn chunked_csv_equals_whole_file_or_refuses() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let (mut accepted, mut refused) = (0, 0);
    for case in 0..120 {
        let newlines = case % 2 == 0;
        let text = random_csv(&mut rng, newlines);
        let path = scratch(&format!("diff-{case}.csv"), &text);
        let whole = parse_all(&csv_source(1 << 20, false), &path)
            .unwrap_or_else(|e| panic!("case {case}: whole-file parse failed: {e}: {text:?}"));
        for chunk in 1..=u64::try_from(text.len()).unwrap() {
            match parse_all(&csv_source(chunk, false), &path) {
                Ok(rows) => {
                    assert_eq!(rows, whole, "case {case}, chunk {chunk}: {text:?}");
                    accepted += 1;
                }
                Err(e) if is_cap_error(&e) => {}
                Err(e) if is_quote_error(&e) => {
                    assert!(
                        newlines,
                        "false quote error, case {case} chunk {chunk}: {text:?}"
                    );
                    refused += 1;
                }
                Err(e) => panic!("case {case}, chunk {chunk}: unexpected {e}: {text:?}"),
            }
        }
    }
    assert!(
        accepted > 0 && refused > 0,
        "{accepted} accepted, {refused} refused"
    );
}

/// For arbitrary byte soup (quotes, delimiters, CR, LF in any order), a
/// chunked parse that succeeds must equal a successful whole-file parse.
#[test]
fn chunked_csv_never_differs_on_malformed_input() {
    let alphabet = ['a', '"', ',', '\n', '\r', '\''];
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let mut accepted = 0;
    for case in 0..120 {
        let len = 4 + rng.below(36);
        let text: String = (0..len).map(|_| alphabet[rng.below(6)]).collect();
        let path = scratch(&format!("soup-{case}.csv"), &text);
        let whole = parse_all(&csv_source(1 << 20, false), &path).ok();
        for chunk in 1..=u64::try_from(text.len()).unwrap() {
            if let Ok(rows) = parse_all(&csv_source(chunk, false), &path) {
                assert_eq!(
                    Some(&rows),
                    whole.as_ref(),
                    "case {case}, chunk {chunk}: {text:?}"
                );
                accepted += 1;
            }
        }
    }
    assert!(accepted > 0);
}
