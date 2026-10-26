//! Text keys (B5): round trip, a property test of every lookup against a
//! brute-force oracle that decodes nothing the index wrote (multi-byte
//! UTF-8, case folding edge cases, nulls), nulls per ADR 014, the Unicode
//! table refusal, and the pinned conformance vectors of `docs/format.md`.
#![allow(clippy::unwrap_used, missing_docs, clippy::cast_possible_truncation)]

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_core::segment::ReadAt;
use bellwether_core::text::{self, Probe, Shape, UnicodeTables};
use bellwether_core::{merge, SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{Error, KeySpec, RowGroupId, TextOp, Tokenizer};
use proptest::prelude::*;
use roaring::RoaringBitmap;
use unicode_segmentation::UnicodeSegmentation;

/// Row groups of an optional string column, by global id.
type Layout = Vec<Vec<Option<String>>>;

fn schema() -> Schema {
    Schema::new(vec![Field::new("body", DataType::Utf8, true)])
}

fn batch(rows: &[Option<String>]) -> RecordBatch {
    let a: ArrayRef = Arc::new(rows.iter().map(Option::as_deref).collect::<StringArray>());
    RecordBatch::try_new(Arc::new(schema()), vec![a]).unwrap()
}

fn spec(tokenizer: Tokenizer, bloom: Option<f64>) -> KeySpec {
    KeySpec::Text {
        column: "body".into(),
        tokenizer,
        bloom,
    }
}

fn specs() -> [KeySpec; 4] {
    [
        spec(Tokenizer::Words, None),
        spec(Tokenizer::Trigram, None),
        spec(Tokenizer::Words, Some(0.01)),
        spec(Tokenizer::Trigram, Some(0.01)),
    ]
}

fn opts() -> WriterOptions {
    WriterOptions {
        block_keys: 3,
        ..WriterOptions::default()
    }
}

/// `layout` indexed under `spec`, cut into segments of at most `per`
/// row groups, each recording the row groups it processed.
fn segments(spec: &KeySpec, layout: &Layout, per: usize) -> Vec<SegmentReader<Vec<u8>>> {
    let mut out = Vec::new();
    for (chunk_no, chunk) in layout.chunks(per.max(1)).enumerate() {
        let first = chunk_no * per.max(1);
        let mut w = SegmentWriter::for_dataset(spec.clone(), &schema(), opts()).unwrap();
        for (i, rows) in chunk.iter().enumerate() {
            w.add_batch(RowGroupId((first + i) as u32), &batch(rows))
                .unwrap();
        }
        w.set_rowgroup_range(RowGroupId(first as u32)..RowGroupId((first + chunk.len()) as u32))
            .unwrap();
        let seg = w.finish().unwrap();
        out.push(SegmentReader::open_for_dataset(seg.bytes, &schema()).unwrap());
    }
    out
}

/// The plan of one lookup through one spec: `None` = the spec does not
/// serve it; every row group when its probe cannot prune.
fn planned<R: ReadAt>(
    spec: &KeySpec,
    readers: &[SegmentReader<R>],
    op: TextOp,
    arg: &str,
    groups: usize,
) -> Option<RoaringBitmap> {
    let shape = Shape::of(std::slice::from_ref(spec), "body", op, arg).ok()?;
    let (_, probe) = shape.serving.first()?;
    Some(
        text::select(readers, probe)
            .unwrap()
            .unwrap_or_else(|| (0..groups as u32).collect()),
    )
}

// -- the oracle: written from the definitions, sharing no code with core --

/// Full case folding (`CaseFolding.txt`, statuses C and F), written out by
/// hand for every character the strategies can produce: the alphabet, its
/// uppercase, and the uppercase of its folds. Any other character is a bug
/// in the test, never a silent guess.
fn oracle_fold(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            'A'..='Z' => out.push(c.to_ascii_lowercase()),
            // LATIN SMALL / CAPITAL LETTER SHARP S: F, 0073 0073.
            '\u{DF}' | '\u{1E9E}' => out.push_str("ss"),
            // GREEK CAPITAL SIGMA, GREEK SMALL FINAL SIGMA: C, 03C3.
            '\u{3A3}' | '\u{3C2}' => out.push('\u{3C3}'),
            // LATIN CAPITAL I WITH DOT ABOVE: F, 0069 0307.
            '\u{130}' => out.push_str("i\u{307}"),
            // KELVIN SIGN: C, 006B.
            '\u{212A}' => out.push('k'),
            // LATIN CAPITAL DZ WITH CARON, and its titlecase: C, 01C6.
            '\u{1C4}' | '\u{1C5}' => out.push('\u{1C6}'),
            // LATIN CAPITAL E WITH ACUTE: C, 00E9.
            '\u{C9}' => out.push('\u{E9}'),
            // GREEK CAPITAL ALPHA: C, 03B1.
            '\u{391}' => out.push('\u{3B1}'),
            'a'..='z' | '0'..='9' | ' ' | '.' | ',' | '\'' | '-' | '\0' => out.push(c),
            // Already folded: σ, dotless ı (only a Turkic T mapping), é,
            // the combining acute and dot above, CJK, ǆ.
            '\u{3C3}' | '\u{3B1}' | '\u{131}' | '\u{E9}' | '\u{301}' | '\u{307}' | '日' | '本'
            | '\u{1C6}' => {
                out.push(c);
            }
            other => panic!(
                "the oracle has no folding for {other:?} (U+{:04X})",
                other as u32
            ),
        }
    }
    out
}

fn bytes_contain(hay: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || hay.windows(needle.len()).any(|w| w == needle)
}

fn oracle_row(op: TextOp, arg: &str, value: &str) -> bool {
    let v = oracle_fold(value);
    let a = oracle_fold(arg);
    match op {
        TextOp::Contains => bytes_contain(v.as_bytes(), a.as_bytes()),
        TextOp::HasToken => v.unicode_words().any(|w| w == a),
        TextOp::HasPrefix => v
            .unicode_words()
            .any(|w| w.as_bytes().starts_with(a.as_bytes())),
    }
}

/// Row groups holding a row that matches: the answer every plan must
/// contain (invariant 1).
fn oracle(layout: &Layout, op: TextOp, arg: &str) -> RoaringBitmap {
    let mut out = RoaringBitmap::new();
    for (rg, rows) in layout.iter().enumerate() {
        if rows.iter().flatten().any(|v| oracle_row(op, arg, v)) {
            out.insert(rg as u32);
        }
    }
    out
}

/// Row groups holding every trigram of the folded argument somewhere: what
/// a trigram dictionary must plan exactly.
fn oracle_trigram_and(layout: &Layout, arg: &str) -> RoaringBitmap {
    let a = oracle_fold(arg);
    let tris: BTreeSet<&[u8]> = a.as_bytes().windows(3).collect();
    let mut out = RoaringBitmap::new();
    for (rg, rows) in layout.iter().enumerate() {
        let folded: Vec<String> = rows.iter().flatten().map(|v| oracle_fold(v)).collect();
        if tris
            .iter()
            .all(|t| folded.iter().any(|f| bytes_contain(f.as_bytes(), t)))
        {
            out.insert(rg as u32);
        }
    }
    out
}

/// Characters chosen for the edges of folding and splitting: ASCII case,
/// German sharp s (both cases), the three sigmas, dotted and dotless i,
/// a precomposed and a decomposed acute, the Kelvin sign, the
/// titlecase digraph, CJK (no case, one word per ideograph), the
/// apostrophe and period that join words, digits, NUL, and a space.
const ALPHABET: [char; 28] = [
    'a', 'b', 'A', 'B', 'ß', 'ẞ', 'Σ', 'σ', 'ς', 'İ', 'ı', 'i', 'I', 'é', 'e', '\u{301}', '日',
    '本', ' ', '.', '\'', '-', '0', '1', 'K', '\u{212A}', 'ǅ', '\0',
];

fn word_string(max: usize) -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(ALPHABET.to_vec()), 0..max)
        .prop_map(|v| v.into_iter().collect())
}

fn layout_strategy() -> impl Strategy<Value = Layout> {
    prop::collection::vec(
        prop::collection::vec(prop::option::weighted(0.85, word_string(14)), 0..6),
        1..9,
    )
}

/// An argument: random, or a slice of a stored value with each character's
/// case flipped at random (so it matches by folding, not by bytes).
fn arg_from(layout: &Layout, pick: usize, start: usize, len: usize, flips: u64, r: &str) -> String {
    let values: Vec<&String> = layout.iter().flatten().flatten().collect();
    if values.is_empty() || pick.is_multiple_of(3) {
        return r.to_string();
    }
    let value = values[pick % values.len()];
    let folded = oracle_fold(value);
    let words: Vec<&str> = folded.unicode_words().collect();
    let v: Vec<char> = if pick % 3 == 1 && !words.is_empty() {
        // A whole word of the value, so has_token finds something.
        words[start % words.len()].chars().collect()
    } else {
        value.chars().collect()
    };
    if v.is_empty() {
        return r.to_string();
    }
    let (s, e) = if pick % 3 == 1 {
        (0, v.len())
    } else {
        let s = start % v.len();
        (s, (s + len % 6).min(v.len()))
    };
    v[s..e]
        .iter()
        .enumerate()
        .flat_map(|(i, c)| {
            if flips >> (i % 64) & 1 == 1 {
                c.to_uppercase().collect::<Vec<_>>()
            } else {
                vec![*c]
            }
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(160))]

    /// Every lookup through every text spec plans a superset of the row
    /// groups holding a match (invariant 1); a words dictionary plans
    /// exactly them, and a trigram dictionary exactly the row groups holding
    /// every trigram of the argument.
    #[test]
    fn lookups_match_a_brute_force_oracle(
        layout in layout_strategy(),
        per in 1usize..4,
        lookups in prop::collection::vec(
            (0usize..3, any::<usize>(), any::<usize>(), any::<usize>(), any::<u64>(), word_string(5)),
            1..6,
        ),
    ) {
        let built: Vec<(KeySpec, Vec<SegmentReader<Vec<u8>>>)> =
            specs().into_iter().map(|s| { let r = segments(&s, &layout, per); (s, r) }).collect();
        for (op_i, pick, start, len, flips, random) in lookups {
            let op = [TextOp::HasToken, TextOp::Contains, TextOp::HasPrefix][op_i];
            let arg = arg_from(&layout, pick, start, len, flips, &random);
            let truth = oracle(&layout, op, &arg);
            for (spec, readers) in &built {
                let Some(plan) = planned(spec, readers, op, &arg, layout.len()) else {
                    // A spec that does not serve the lookup, or an argument
                    // the op refuses, typed.
                    if let Err(e) = Shape::of(std::slice::from_ref(spec), "body", op, &arg) {
                        let typed = matches!(e, Error::InvalidArgument { field: "text", .. });
                        prop_assert!(typed, "untyped refusal: {e}");
                    }
                    continue;
                };
                prop_assert!(
                    truth.is_subset(&plan),
                    "{} missed a match for {}({arg:?}): truth {truth:?}, plan {plan:?}",
                    spec.name(), op.as_str()
                );
                match spec {
                    KeySpec::Text { tokenizer: Tokenizer::Words, bloom: None, .. } => {
                        prop_assert_eq!(&plan, &truth, "words dictionary is exact at row-group grain");
                    }
                    KeySpec::Text { tokenizer: Tokenizer::Trigram, bloom: None, .. }
                        if oracle_fold(&arg).len() >= 3 =>
                    {
                        prop_assert_eq!(&plan, &oracle_trigram_and(&layout, &arg));
                    }
                    _ => {}
                }
            }
        }
    }

    /// The residual's row test is the oracle's.
    #[test]
    fn the_residual_row_test_is_the_definition(
        value in word_string(14),
        arg in word_string(5),
    ) {
        for op in [TextOp::HasToken, TextOp::Contains, TextOp::HasPrefix] {
            prop_assert_eq!(text::matches(op, &arg, &value), oracle_row(op, &arg, &value));
        }
    }

    /// Folding a concatenation folds each part: a substring of a value
    /// folds to a substring of the folded value (why trigram plans are
    /// sound).
    #[test]
    fn folding_is_a_homomorphism(a in word_string(10), b in word_string(10)) {
        prop_assert_eq!(text::fold(&format!("{a}{b}")), format!("{}{}", text::fold(&a), text::fold(&b)));
    }
}

#[test]
fn round_trip_through_merge_and_token_stats() {
    let layout: Layout = vec![
        vec![Some("Hello, World".into()), Some("the quick fox".into())],
        vec![None, Some("Straße STRASSE".into())],
        vec![Some(String::new()), None],
        vec![Some("hello again, ΣΑΣ".into())],
    ];
    for spec in specs() {
        let one = segments(&spec, &layout, 4);
        let two = segments(&spec, &layout, 1);
        let merged = SegmentReader::open(merge(&two, &opts()).unwrap().bytes).unwrap();
        assert_eq!(merged.key_spec(), &spec);
        for (op, arg) in [
            (TextOp::HasToken, "HELLO"),
            (TextOp::Contains, "llo"),
            (TextOp::Contains, "strasse"),
            (TextOp::HasPrefix, "str"),
            (TextOp::Contains, "σασ"),
        ] {
            let a = planned(&spec, &one, op, arg, layout.len());
            let b = planned(&spec, &two, op, arg, layout.len());
            let c = planned(&spec, std::slice::from_ref(&merged), op, arg, layout.len());
            assert_eq!(a, b, "{} {op:?} {arg}", spec.name());
            assert_eq!(a, c, "{} {op:?} {arg} merged", spec.name());
            if let Some(p) = a {
                assert!(oracle(&layout, op, arg).is_subset(&p));
            }
        }
    }
    let words = segments(&spec(Tokenizer::Words, None), &layout, 1);
    let stats = text::token_stats(&words).unwrap();
    // hello world the quick fox strasse again σασ (ß folds to ss)
    assert_eq!(stats.distinct, 8);
    assert_eq!(stats.row_groups_per_token, [1, 2, 2]);
}

/// ADR 014: a null holds no token; a row group of nulls is in no bitmap,
/// and no lookup through a dictionary plans it. The empty string is a
/// value with no token.
#[test]
fn nulls_are_not_indexed() {
    let layout: Layout = vec![
        vec![None, None],
        vec![Some("abc".into())],
        vec![Some(String::new())],
    ];
    for spec in [spec(Tokenizer::Words, None), spec(Tokenizer::Trigram, None)] {
        let readers = segments(&spec, &layout, 3);
        assert_eq!(
            readers[0].rowgroup_range(),
            Some((RowGroupId(0), RowGroupId(2)))
        );
        assert_eq!(readers[0].entries(), 1, "{}", spec.name());
        for (op, arg) in [
            (TextOp::HasToken, "abc"),
            (TextOp::Contains, "abc"),
            (TextOp::HasPrefix, "abc"),
        ] {
            if let Some(p) = planned(&spec, &readers, op, arg, layout.len()) {
                assert_eq!(p, [1u32].into_iter().collect::<RoaringBitmap>());
            }
        }
    }
    // The empty string is a value: `contains("")` matches it.
    assert!(text::matches(TextOp::Contains, "", ""));
}

#[test]
fn a_column_that_is_not_a_string_is_refused() {
    let schema = Schema::new(vec![Field::new("body", DataType::Int64, true)]);
    let e = SegmentWriter::for_dataset(spec(Tokenizer::Words, None), &schema, opts())
        .err()
        .unwrap();
    assert!(e.to_string().contains("needs a string column"), "{e}");
}

/// A segment records the Unicode tables it was built under; a reader on
/// other tables refuses it, typed, with what to do.
#[test]
fn a_segment_built_under_other_unicode_tables_is_refused() {
    let readers = segments(
        &spec(Tokenizer::Words, None),
        &vec![vec![Some("a b".into())]],
        1,
    );
    let mut bytes = readers[0].source().clone();
    let now = UnicodeTables::current();
    let needle = format!("\"case\":\"{}\"", now.case);
    let at = bytes
        .windows(needle.len())
        .position(|w| w == needle.as_bytes())
        .expect("the header records the case table");
    // Same length, another version of the crate: its last character.
    let digit = at + needle.len() - 2;
    bytes[digit] = if bytes[digit] == b'1' { b'2' } else { b'1' };
    match SegmentReader::open(bytes) {
        Err(Error::KeySpec(m)) => {
            assert!(m.contains(text::REBUILD_STEP), "{m}");
            assert!(m.contains(&now.case), "names this binary's tables: {m}");
        }
        other => panic!(
            "expected a typed refusal, got {other:?}",
            other = other.map(|_| ())
        ),
    }
}

/// `recorded_tables` reads the tables from the header alone, whatever they
/// are; a text segment whose header carries no tables is corrupt, never
/// read as current.
#[test]
fn recorded_tables_are_read_and_a_text_header_without_them_is_refused() {
    let readers = segments(
        &spec(Tokenizer::Trigram, None),
        &vec![vec![Some("abcd".into())]],
        1,
    );
    let bytes = readers[0].source().clone();
    assert_eq!(
        text::recorded_tables(&bytes).unwrap(),
        Some(UnicodeTables::current())
    );
    // Rename the field (same length): the header no longer records tables.
    let needle = b"\"text_unicode\"";
    let at = bytes
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("the header records its tables");
    let mut b = bytes.clone();
    b[at + needle.len() - 2] = b'X';
    assert_eq!(text::recorded_tables(&b).unwrap(), None);
    match SegmentReader::open(b) {
        Err(e) => assert!(e.to_string().contains("records its Unicode tables"), "{e}"),
        Ok(_) => panic!("a text segment without its tables opened"),
    }
    assert!(text::recorded_tables(&bytes[..10].to_vec()).is_err());
}

/// The vectors `docs/format.md` ("Text key conformance vectors") pins.
#[test]
fn conformance_vectors() {
    let hex = |b: &[u8]| {
        b.iter()
            .map(|x| format!("{x:02x}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    assert_eq!(text::fold("ẞİΣ\u{212A}ς"), "ssi\u{307}σkσ");
    assert_eq!(
        hex(&text::word_key("grösse")),
        "06 67 72 c3 b6 73 73 65 00 00"
    );
    assert_eq!(
        hex(&text::trigram_key("aö".as_bytes())),
        "07 61 c3 b6 00 00"
    );
    assert_eq!(hex(&text::word_prefix_probe("st")), "06 73 74");
    let words: Vec<&str> = text::words("hello, wörld! can't 3.5 日本").collect();
    assert_eq!(words, ["hello", "wörld", "can't", "3.5", "日", "本"]);
    let layout: Layout = vec![
        vec![Some("Hello, World".into())],
        vec![None, Some("Straße".into())],
    ];
    let digest = |s: &KeySpec| {
        let r = segments(s, &layout, 2);
        bellwether_types::Hash::of(r[0].source()).to_hex()
    };
    // The segment header records the Unicode tables (the pinned crates,
    // never the toolchain), so the digests hold for these tables only: a
    // build on other crate versions refuses segments built here, and these
    // vectors (and docs/format.md) move with it.
    assert_eq!(
        UnicodeTables::current(),
        UnicodeTables {
            case: "unicase 2.10.0".into(),
            words: "unicode-segmentation 1.13.3".into()
        },
        "the Unicode tables changed: text keys built before are refused; update the vectors \
         here and in docs/format.md"
    );
    assert_eq!(
        digest(&spec(Tokenizer::Words, None)),
        "9ec49da81fbc14f0851824f2cf1c9e0d5b4073518f07e8e7fe7506bb7c73e7e1"
    );
    assert_eq!(
        digest(&spec(Tokenizer::Trigram, None)),
        "61059108b1be52592ddf632085b14b055dc4640b736ff3a2c3e5896d82872e57"
    );
}

#[test]
fn probes_follow_the_planning_rule() {
    let w = spec(Tokenizer::Words, None);
    let wb = spec(Tokenizer::Words, Some(0.01));
    let t = spec(Tokenizer::Trigram, None);
    assert_eq!(text::probe(&w, "body", TextOp::Contains, "abc"), None);
    assert_eq!(text::probe(&wb, "body", TextOp::HasPrefix, "abc"), None);
    assert_eq!(text::probe(&w, "other", TextOp::HasToken, "abc"), None);
    assert_eq!(
        text::probe(&t, "body", TextOp::Contains, "ab"),
        Some(Probe::Unpruned)
    );
    assert!(
        matches!(text::probe(&t, "body", TextOp::Contains, "abcd"), Some(Probe::All(k)) if k.len() == 2)
    );
    let shape = Shape::of(&[w.clone(), t.clone()], "body", TextOp::HasToken, "Hello").unwrap();
    assert_eq!(shape.folded, "hello");
    assert!(shape.exact() && shape.prunes());
    let shape = Shape::of(&[t], "body", TextOp::Contains, "ab").unwrap();
    assert!(!shape.prunes() && !shape.exact());
}
