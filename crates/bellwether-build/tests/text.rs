//! Text keys (B5) through the builder and the planner, checked against the
//! decoded files (invariant 1): every row group holding a matching row is
//! planned, the residual applied to the planned rows returns exactly the
//! matching rows, a words dictionary plans only row groups holding a match,
//! nulls hold no token (ADR 014), and incremental builds, compaction, and a
//! changed file keep all of it. A text key never serves a key lookup.
#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{BuildOptions, Lookup};
use bellwether_core::encoding;
use bellwether_types::{KeySpec, Predicate, TextOp, Tokenizer};
use common::*;
use proptest::prelude::*;

fn text_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]))
}

fn write(path: &Path, rows: &[(i64, Option<&str>)], per_rg: usize) {
    let ids: ArrayRef = Arc::new(rows.iter().map(|r| r.0).collect::<Int64Array>());
    let bodies: ArrayRef = Arc::new(rows.iter().map(|r| r.1).collect::<StringArray>());
    let b = RecordBatch::try_new(text_schema(), vec![ids, bodies]).unwrap();
    write_parquet(path, &b, per_rg);
}

fn text(tokenizer: Tokenizer, bloom: Option<f64>) -> KeySpec {
    KeySpec::Text {
        column: "body".into(),
        tokenizer,
        bloom,
    }
}

fn lookup(op: TextOp, value: &str) -> Lookup {
    Lookup::Text {
        column: "body".into(),
        op,
        values: vec![value.into()],
    }
}

/// Every row of every file under `root`, decoded.
fn all_rows(root: &Path) -> Vec<Row> {
    let mut files: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    files.sort();
    files
        .iter()
        .flat_map(|f| parquet_rows(f, &text_schema()))
        .collect()
}

fn matching(rows: &[Row], op: TextOp, value: &str) -> Vec<Row> {
    rows.iter()
        .filter(|r| {
            r.get("body")
                .map(|v| decode_utf8(v))
                .is_some_and(|v| oracle(op, value, &v))
        })
        .cloned()
        .collect()
}

/// Full case folding (`CaseFolding.txt`, C and F) written out for the
/// characters these documents and lookups hold; any other character with
/// case is a bug in the test, never a silent guess.
fn fold(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            'A'..='Z' => out.push(c.to_ascii_lowercase()),
            // SHARP S: F, 0073 0073.
            '\u{DF}' => out.push_str("ss"),
            // CAPITAL SIGMA, FINAL SIGMA: C, 03C3.
            '\u{3A3}' | '\u{3C2}' => out.push('\u{3C3}'),
            // CAPITAL ALPHA: C, 03B1.
            '\u{391}' => out.push('\u{3B1}'),
            // CAPITAL I WITH DOT ABOVE: F, 0069 0307.
            '\u{130}' => out.push_str("i\u{307}"),
            c if c.to_lowercase().eq(std::iter::once(c)) => out.push(c),
            other => panic!("the oracle has no folding for {other:?}"),
        }
    }
    out
}

/// The match by definition, independent of core: per-code-point full case
/// folding, then a byte search or the UAX #29 words.
fn oracle(op: TextOp, arg: &str, value: &str) -> bool {
    use unicode_segmentation::UnicodeSegmentation;
    let (v, a) = (fold(value), fold(arg));
    match op {
        TextOp::Contains => {
            a.is_empty() || v.as_bytes().windows(a.len()).any(|w| w == a.as_bytes())
        }
        TextOp::HasToken => v.unicode_words().any(|w| w == a),
        TextOp::HasPrefix => v.unicode_words().any(|w| w.starts_with(&a)),
    }
}

fn row_ids(rows: &[Row]) -> BTreeSet<(String, u32, usize)> {
    rows.iter().map(|r| (r.path.clone(), r.rg, r.row)).collect()
}

fn rgs_of(rows: &[Row]) -> BTreeSet<(String, u32)> {
    rows.iter().map(|r| (r.path.clone(), r.rg)).collect()
}

/// Checks one lookup against the decoded rows under `specs`.
fn check(
    store: &Arc<bellwether_store::ObjectStoreBackend>,
    root: &Path,
    specs: &[KeySpec],
    op: TextOp,
    value: &str,
) {
    let p = planner(store);
    let plan = match p.plan(specs, &lookup(op, value)) {
        Ok(plan) => plan,
        Err(e) => {
            // has_token or has_prefix of something that is not one word: typed.
            assert!(
                matches!(
                    e,
                    bellwether_types::Error::InvalidArgument { field: "text", .. }
                ),
                "{op:?} {value:?}: {e}"
            );
            return;
        }
    };
    let rows = all_rows(root);
    let truth = matching(&rows, op, value);
    let planned = plan_rgs(&plan);
    assert!(
        rgs_of(&truth).is_subset(&planned),
        "{op:?}({value:?}) under {specs:?} missed row groups {:?}",
        rgs_of(&truth).difference(&planned).collect::<Vec<_>>()
    );
    let residual = plan.residual.clone().unwrap();
    assert_eq!(
        residual,
        Predicate::Text {
            column: "body".into(),
            operator: op,
            values: vec![value.into()]
        }
    );
    let read = apply_residual(&residual, planned_rows(&plan, &rows));
    assert_eq!(row_ids(&read), row_ids(&truth), "{op:?}({value:?})");
    let words_dictionary = specs.contains(&text(Tokenizer::Words, None));
    if words_dictionary && op != TextOp::Contains {
        assert_eq!(
            planned,
            rgs_of(&truth),
            "a words dictionary plans only matches"
        );
    }
}

const DOCS: [(i64, Option<&str>); 12] = [
    (1, Some("Quarterly GUIDANCE raised; Straße office")),
    (2, None),
    (3, Some("guidance withdrawn")),
    (4, Some("")),
    (5, Some("ΣΑΣ restatement of revenue")),
    (6, None),
    (7, Some("dividend ἀρχή 日本 listing")),
    (8, Some("can't restate")),
    (9, Some("İstanbul office")),
    (10, Some("no match here")),
    (11, Some("STRASSE")),
    (12, Some("quarterly")),
];

const LOOKUPS: [(TextOp, &str); 16] = [
    (TextOp::HasToken, "guidance"),
    (TextOp::HasToken, "Quarterly"),
    (TextOp::HasToken, "straße"),
    (TextOp::HasToken, "can't"),
    (TextOp::HasToken, "two words"),
    (TextOp::Contains, "ance"),
    (TextOp::Contains, "STRA"),
    (TextOp::Contains, "σασ"),
    (TextOp::Contains, "i\u{307}st"),
    (TextOp::Contains, "ab"),
    (TextOp::Contains, ""),
    (TextOp::Contains, "zzzz"),
    (TextOp::HasPrefix, "restat"),
    (TextOp::HasPrefix, "日"),
    (TextOp::HasPrefix, "q"),
    (TextOp::HasPrefix, "zz"),
];

fn build(store: &Arc<bellwether_store::ObjectStoreBackend>, root: &Path, specs: &[KeySpec]) {
    builder(store)
        .build(root.to_str().unwrap(), specs, &parquet_opts(&text_schema()))
        .unwrap();
}

/// Invariant 1 against the decoded files, for each layout alone and both
/// dictionaries together; incremental builds, a changed file, and
/// compaction keep it.
#[test]
fn text_lookups_plan_every_matching_row_group() {
    for specs in [
        vec![text(Tokenizer::Words, None), text(Tokenizer::Trigram, None)],
        vec![text(Tokenizer::Words, Some(0.05))],
        vec![text(Tokenizer::Trigram, Some(0.05))],
    ] {
        let s = Scratch::new();
        let store = memory_store();
        write(&s.path("a.parquet"), &DOCS[..6], 2);
        write(&s.path("b.parquet"), &DOCS[6..], 3);
        build(&store, s.root(), &specs);
        for (op, v) in LOOKUPS {
            check(&store, s.root(), &specs, op, v);
            for one in &specs {
                check(&store, s.root(), std::slice::from_ref(one), op, v);
            }
        }
        // A new file, then a changed one: incremental builds.
        write(&s.path("c.parquet"), &[(20, Some("late guidance"))], 1);
        build(&store, s.root(), &[]);
        write(&s.path("a.parquet"), &[(1, Some("rewritten quarterly"))], 1);
        build(&store, s.root(), &[]);
        builder(&store)
            .compact(&bellwether_build::CompactOptions::default())
            .unwrap();
        for (op, v) in LOOKUPS {
            check(&store, s.root(), &specs, op, v);
        }
    }
}

/// A text key serves text lookups only; a key lookup on its column is not
/// served by it.
#[test]
fn a_text_key_never_serves_a_key_lookup() {
    let s = Scratch::new();
    let store = memory_store();
    write(&s.path("a.parquet"), &DOCS, 4);
    build(&store, s.root(), &[text(Tokenizer::Words, None)]);
    let p = planner(&store);
    let mut k = Vec::new();
    encoding::encode_value(&encoding::Value::Utf8("guidance withdrawn"), &mut k).unwrap();
    let e = p
        .plan(
            &[text(Tokenizer::Words, None)],
            &Lookup::Keys {
                columns: vec!["body".into()],
                keys: vec![k],
            },
        )
        .unwrap_err();
    assert!(e.to_string().contains("serves a lookup"), "{e}");
    // And a text lookup on another column, or a column that is not a
    // string, is refused.
    let e = p
        .plan(
            &[],
            &Lookup::Text {
                column: "id".into(),
                op: TextOp::Contains,
                values: vec!["1".into()],
            },
        )
        .unwrap_err();
    assert!(e.to_string().contains("needs a string column"), "{e}");
}

/// Declaring a text key on a column that is not a string is refused before
/// anything is read.
#[test]
fn a_text_key_on_a_number_column_is_refused_by_the_build() {
    let s = Scratch::new();
    let store = memory_store();
    write(&s.path("a.parquet"), &DOCS, 4);
    let e = builder(&store)
        .build(
            s.root_str().as_str(),
            &[KeySpec::Text {
                column: "id".into(),
                tokenizer: Tokenizer::Words,
                bloom: None,
            }],
            &BuildOptions {
                schema: Some(text_schema()),
                ..parquet_opts(&text_schema())
            },
        )
        .unwrap_err();
    assert!(e.to_string().contains("needs a string column"), "{e}");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Random documents over a case- and script-mixed alphabet, random row
    /// groups and files, random lookups drawn from the documents' own
    /// slices: invariant 1 and the residual's exactness hold.
    #[test]
    fn random_documents_hold_invariant_1(
        docs in prop::collection::vec(
            prop::option::weighted(0.85, prop::collection::vec(
                prop::sample::select(vec!['a', 'B', 'ß', 'Σ', 'ς', 'İ', 'é', ' ', '.', 'x', 'X']), 0..10)
                .prop_map(|v| v.into_iter().collect::<String>())),
            1..30),
        per in 1usize..5,
        picks in prop::collection::vec((0usize..3, any::<usize>(), 0usize..5), 1..6),
    ) {
        let s = Scratch::new();
        let store = memory_store();
        let rows: Vec<(i64, Option<&str>)> =
            docs.iter().enumerate().map(|(i, d)| (i as i64, d.as_deref())).collect();
        let half = rows.len() / 2;
        write(&s.path("a.parquet"), &rows[..half], per);
        if half < rows.len() {
            write(&s.path("b.parquet"), &rows[half..], per);
        }
        let specs = [text(Tokenizer::Words, None), text(Tokenizer::Trigram, None)];
        build(&store, s.root(), &specs);
        for (op_i, at, len) in picks {
            let op = [TextOp::HasToken, TextOp::Contains, TextOp::HasPrefix][op_i];
            let source: Vec<char> = docs.iter().flatten().flat_map(|d| d.chars()).collect();
            let arg: String = if source.is_empty() {
                "a".into()
            } else {
                let st = at % source.len();
                source[st..(st + len).min(source.len())].iter().collect()
            };
            check(&store, s.root(), &specs, op, &arg);
        }
    }
}

/// Rewrites every committed segment of `spec` as if a binary on other
/// Unicode tables had built it: the header's case-table version changes
/// (same length), the segment is stored under the hash of its new bytes,
/// and the manifest points at it. Returns the aged segments' ids.
fn age_tables(store: &Arc<bellwether_store::ObjectStoreBackend>, spec: &KeySpec) -> Vec<String> {
    use bellwether_types::Store;
    let path = "idx/manifest.json";
    let (bytes, version) = store.get(path).unwrap();
    let mut m: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let now = bellwether_core::text::UnicodeTables::current();
    let needle = format!("\"case\":\"{}\"", now.case);
    let mut old = Vec::new();
    for seg in m["segments"].as_array_mut().unwrap() {
        if seg["key_spec"] != spec.name() {
            continue;
        }
        let id = seg["id"].as_str().unwrap().to_string();
        let (b, _) = store.get(&format!("idx/segments/{id}.seg")).unwrap();
        let mut b = b.to_vec();
        let at = b
            .windows(needle.len())
            .position(|w| w == needle.as_bytes())
            .unwrap();
        let digit = at + needle.len() - 2;
        b[digit] = if b[digit] == b'1' { b'2' } else { b'1' };
        let new_id = bellwether_types::Hash::of(&b).to_hex();
        store
            .put_if_absent(&format!("idx/segments/{new_id}.seg"), b.into())
            .unwrap();
        let _ = id;
        seg["id"] = serde_json::Value::String(new_id.clone());
        old.push(new_id);
    }
    assert_ne!(old.len(), 0);
    store
        .put_if_version(path, serde_json::to_vec(&m).unwrap().into(), &version)
        .unwrap();
    old
}

/// After a deliberate upgrade of a Unicode table crate, the text key's
/// segments record other tables: lookups refuse with the rebuild step, and a
/// plain `index` (no new spec, no new file) rebuilds exactly that key under
/// this binary's tables, keeping the other key's segments; the lookups then
/// plan as before. A build that also indexes a new file never mixes tables.
#[test]
#[allow(clippy::many_single_char_names)]
fn a_text_key_built_under_other_tables_is_rebuilt_by_index() {
    use bellwether_core::text;
    use bellwether_types::Store;
    let s = Scratch::new();
    let store = memory_store();
    write(&s.path("a.parquet"), &DOCS[..6], 2);
    write(&s.path("b.parquet"), &DOCS[6..], 3);
    let words = text(Tokenizer::Words, None);
    let specs = [words.clone(), exact(&["id"])];
    build(&store, s.root(), &specs);
    let before = planner(&store)
        .plan(&specs, &lookup(TextOp::HasToken, "guidance"))
        .unwrap();
    let exact_ids = |m: &bellwether_store::Manifest| -> Vec<String> {
        m.key_spec_segments("exact:id")
            .map(|s| s.id.to_hex())
            .collect()
    };
    let exact_before = exact_ids(planner(&store).manifest());
    let old = age_tables(&store, &words);
    let e = planner(&store)
        .plan(&specs, &lookup(TextOp::HasToken, "guidance"))
        .unwrap_err();
    assert!(e.to_string().contains(text::REBUILD_STEP), "{e}");
    // A new file lands too: the rebuild covers it, under one table version.
    write(&s.path("c.parquet"), &[(30, Some("late guidance"))], 1);
    let r = builder(&store)
        .build(
            s.root().to_str().unwrap(),
            &[],
            &parquet_opts(&text_schema()),
        )
        .unwrap();
    assert_eq!(r.specs_rebuilt, [words.name()]);
    // The report says why: the tables the old segments recorded, and this
    // binary's.
    let now = text::UnicodeTables::current();
    assert_eq!(r.text_tables_replaced.len(), 1);
    let t = &r.text_tables_replaced[0];
    assert_eq!(t.spec, words.name());
    assert_eq!(t.current, now);
    assert_ne!(t.recorded.case, now.case);
    assert_eq!(t.recorded.words, now.words);
    let p = planner(&store);
    let m = p.manifest();
    let mut seen = 0;
    for seg in m.key_spec_segments(&words.name()) {
        assert!(!old.contains(&seg.id.to_hex()), "an aged segment survived");
        let b = store
            .get(&format!("idx/segments/{}.seg", seg.id.to_hex()))
            .unwrap()
            .0;
        assert_eq!(text::recorded_tables(&b).unwrap(), Some(now.clone()));
        seen += 1;
    }
    assert!(seen > 0);
    // The other key keeps its segments (and gains one for the new file).
    let exact_after = exact_ids(m);
    assert!(
        exact_before.iter().all(|id| exact_after.contains(id)),
        "the other key is kept: {exact_before:?} -> {exact_after:?}"
    );
    assert_eq!(exact_after.len(), exact_before.len() + 1);
    let after = p
        .plan(&specs, &lookup(TextOp::HasToken, "guidance"))
        .unwrap();
    assert!(after.row_groups_selected > before.row_groups_selected);
    check(&store, s.root(), &specs, TextOp::HasToken, "guidance");
    // Nothing left to rebuild.
    let again = builder(&store)
        .build(
            s.root().to_str().unwrap(),
            &[],
            &parquet_opts(&text_schema()),
        )
        .unwrap();
    assert_eq!(again.specs_rebuilt, Vec::<String>::new());
    assert_eq!(again.text_tables_replaced, []);
}
