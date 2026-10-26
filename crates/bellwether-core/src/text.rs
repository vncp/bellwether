//! Text keys (B5): the tokenizers, the token encodings a text segment
//! stores, the residual's row test, and the planning rule both planners
//! follow (`docs/format.md`, "Text keys").
//!
//! A text key (`text:<column>,tokenizer=words|trigram`) indexes one string
//! column. A value is **case-folded** first: each code point is replaced by
//! its Unicode full case folding (`CaseFolding.txt`, statuses C and F), one
//! code point at a time, with no context. Folding is therefore a
//! homomorphism over concatenation (`fold(a + b) = fold(a) + fold(b)`),
//! which is what makes a substring of a value fold to a substring of the
//! folded value, and so what makes the trigram plan sound. `ß` folds to
//! `ss`, a final sigma `ς` to `σ`, the Kelvin sign to `k`, and `İ` to `i̇`
//! (two code points). The residual folds the same way, so plan and
//! residual always agree. Text is never Unicode-normalized: a precomposed
//! `é` (NFC) and `e` + U+0301 (NFD) are different text, so `contains` in
//! one form does not find the other, and `has_prefix("e")` matches the NFD
//! word `école` but not the NFC one.
//!
//! - **words**: the words of the folded value at Unicode word boundaries
//!   ([UAX #29](https://unicode.org/reports/tr29/), the
//!   `unicode-segmentation` crate's `unicode_words`), each stored as the
//!   canonical `Utf8` encoding of the word (ADR 016).
//! - **trigram**: every 3-byte window of the folded value's UTF-8 bytes,
//!   each stored as the canonical `Binary` encoding of the 3 bytes. A
//!   window may cut a multi-byte character; the bytes are what matter. A
//!   folded value shorter than 3 bytes has no trigram.
//!
//! A null value has no tokens and matches nothing (ADR 014).
//!
//! # Unicode tables
//!
//! Both tables come from pinned crates that carry their own data, never
//! from the standard library, so a toolchain upgrade changes no token: the
//! case folding from `unicase` ([`CASE_TABLES`]) and the word boundaries
//! from `unicode-segmentation` ([`WORD_TABLES`]), each pinned to one
//! version in the workspace manifest. Every text segment's header records
//! both ([`UnicodeTables`]); a segment built under other tables is refused,
//! never read, because the same value could fold or split differently.
//! That refusal happens only after a deliberate upgrade of either crate,
//! and its next step is `bellwether index`, which rebuilds exactly the text
//! keys built under other tables ([`recorded_tables`]).
//!
//! # Lookups
//!
//! [`TextOp::HasToken`] (`t` is one word after folding), [`TextOp::Contains`]
//! (a substring), and [`TextOp::HasPrefix`] (some word starts with `p`).
//! [`probe`] says how a text spec serves one: a words dictionary answers
//! `has_token` and `has_prefix` exactly at row-group grain (a planned row
//! group holds a matching row); a words bloom filter answers `has_token`
//! with candidates; a trigram spec answers all three with candidates, the
//! row groups holding every trigram of the folded argument (an argument
//! shorter than 3 bytes after folding cannot prune). With trigram
//! candidates the residual is mandatory: a planned row group may hold no
//! matching row at all.

use std::collections::BTreeSet;

use arrow::array::Array;
use arrow::datatypes::DataType;
use bellwether_types::{Error, KeySpec, Result, TextOp, Tokenizer};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use unicode_segmentation::UnicodeSegmentation;

use crate::encoding::{self, tag, Value};
use crate::segment::{ReadAt, SegmentReader};

/// Bytes in a trigram.
pub const TRIGRAM: usize = 3;

/// The next step a refusal of a text segment built under other tables
/// names. The facade replaces `the data folder` with the data root.
pub const REBUILD_STEP: &str = "run `bellwether index` on the data folder: it rebuilds exactly \
                                this key with this binary's tables and keeps the ledger and \
                                every other key";

/// The case-folding tables: the pinned crate and version that carry them
/// (the workspace manifest pins `unicase` to exactly this version).
pub const CASE_TABLES: &str = "unicase 2.10.0";

/// The word-boundary tables: the pinned crate and version that carry them
/// (the workspace manifest pins `unicode-segmentation` to exactly this
/// version).
pub const WORD_TABLES: &str = "unicode-segmentation 1.13.3";

/// `value` case-folded (module docs): each code point replaced by its
/// Unicode full case folding, without context.
#[must_use]
pub fn fold(value: &str) -> String {
    unicase::UniCase::unicode(value).to_folded_case()
}

/// The words of an already folded value, in order, repeats included.
pub fn words(folded: &str) -> impl Iterator<Item = &str> {
    folded.unicode_words()
}

/// The 3-byte windows of an already folded value, in order, repeats
/// included; none when it is shorter than [`TRIGRAM`] bytes.
pub fn trigrams(folded: &str) -> impl Iterator<Item = &[u8]> {
    folded.as_bytes().windows(TRIGRAM)
}

/// The stored key of a word: its canonical `Utf8` encoding.
#[must_use]
pub fn word_key(word: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(word.len() + 3);
    // Infallible: a string encodes under its natural type.
    let _ = encoding::encode_value(&Value::Utf8(word), &mut out);
    out
}

/// The stored key of a trigram: the canonical `Binary` encoding of its
/// bytes.
#[must_use]
pub fn trigram_key(trigram: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(trigram.len() + 3);
    let _ = encoding::encode_value(&Value::Binary(trigram), &mut out);
    out
}

/// The byte prefix every stored word key starting with `prefix` shares: the
/// word key of `prefix` without its two-byte terminator (the escaping is a
/// per-byte code, so it preserves prefixes).
#[must_use]
pub fn word_prefix_probe(prefix: &str) -> Vec<u8> {
    let mut k = word_key(prefix);
    k.truncate(k.len() - 2);
    k
}

/// The string a canonical `Utf8` encoding holds: a text run's argument as
/// its key set stores it. Refuses any other tag, a broken escape, a missing
/// terminator, trailing bytes, or bytes that are not UTF-8.
pub fn arg_of_key(key: &[u8]) -> Result<String> {
    let bad = |why: &str| {
        Error::KeySpec(format!(
            "a text lookup's argument is a string; this key is not one ({why})"
        ))
    };
    let Some((&tag::UTF8, rest)) = key.split_first() else {
        return Err(bad("its type tag is not Utf8"));
    };
    let mut out = Vec::with_capacity(rest.len());
    let mut i = 0;
    loop {
        match (rest.get(i), rest.get(i + 1)) {
            (Some(0x00), Some(0x00)) if i + 2 == rest.len() => break,
            (Some(0x00), Some(0xFF)) => {
                out.push(0);
                i += 2;
            }
            (Some(0x00) | None, _) => return Err(bad("its terminator is missing")),
            (Some(&b), _) => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| bad("its bytes are not UTF-8"))
}

/// Adds the distinct stored keys of one value to `out`.
pub fn keys_of(tokenizer: Tokenizer, value: &str, out: &mut BTreeSet<Vec<u8>>) {
    let folded = fold(value);
    match tokenizer {
        Tokenizer::Words => {
            for w in words(&folded) {
                out.insert(word_key(w));
            }
        }
        Tokenizer::Trigram => {
            for t in trigrams(&folded) {
                out.insert(trigram_key(t));
            }
        }
    }
}

/// The tables folding and word splitting use, as a text segment records
/// them: each the pinned crate and version that carries it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnicodeTables {
    /// The case-folding tables ([`CASE_TABLES`]).
    pub case: String,
    /// The word-boundary tables ([`WORD_TABLES`]).
    pub words: String,
}

impl UnicodeTables {
    /// The tables this binary folds and splits with.
    #[must_use]
    pub fn current() -> Self {
        Self {
            case: CASE_TABLES.to_string(),
            words: WORD_TABLES.to_string(),
        }
    }
}

/// The Unicode tables the text segment in `src` records, read from its
/// preamble and header only (two small reads), without the check that
/// refuses other tables: what `index` reads to find a text key built under
/// other tables, which it then rebuilds. `None` for a segment that is not a
/// text key's. A preamble or header that does not parse is refused.
pub fn recorded_tables<R: ReadAt>(src: &R) -> Result<Option<UnicodeTables>> {
    use crate::segment::{MAGIC, PREAMBLE_LEN};
    #[derive(Deserialize)]
    struct Recorded {
        #[serde(default)]
        text_unicode: Option<UnicodeTables>,
    }
    let corrupt = |what: &str| Error::Other(format!("corrupt segment: {what}"));
    let pre = src.read_at(0, PREAMBLE_LEN)?;
    if pre.len() != PREAMBLE_LEN || pre[..8] != MAGIC {
        return Err(corrupt("bad magic"));
    }
    let header_len = u32::from_le_bytes([pre[16], pre[17], pre[18], pre[19]]) as usize;
    let header = src.read_at(PREAMBLE_LEN as u64, header_len)?;
    let r: Recorded = serde_json::from_slice(&header).map_err(|_| corrupt("header"))?;
    Ok(r.text_unicode)
}

/// Whether `dt` can carry a text key: a string type, or a dictionary of
/// one.
#[must_use]
pub fn is_text_type(dt: &DataType) -> bool {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, v) => is_text_type(v),
        _ => false,
    }
}

/// Refuses a text key on a column that is not a string column.
pub fn check_column_type(column: &str, dt: &DataType) -> Result<()> {
    if is_text_type(dt) {
        Ok(())
    } else {
        Err(Error::KeySpec(format!(
            "a text key needs a string column, and {column} is {dt}; index it with exact:{column} \
             or another kind"
        )))
    }
}

/// The string at `row` of `array`, `None` for a null.
pub fn value_at(array: &dyn Array, row: usize) -> Result<Option<&str>> {
    match encoding::value_at(array, row)? {
        None => Ok(None),
        Some(Value::Utf8(s)) => Ok(Some(s)),
        Some(_) => Err(Error::KeySpec(format!(
            "a text key reads strings, and the column holds {}",
            array.data_type()
        ))),
    }
}

/// What indexing one row group's `array` under `tokenizer` can add to an
/// open segment, as an upper bound: its distinct keys, and their bytes
/// (8 per key for a bloom layout, which holds one hash per key).
pub fn measure(tokenizer: Tokenizer, bloom: bool, array: &dyn Array) -> Result<(u64, u64)> {
    let mut keys = BTreeSet::new();
    for row in 0..array.len() {
        if let Some(v) = value_at(array, row)? {
            keys_of(tokenizer, v, &mut keys);
        }
    }
    let bytes = if bloom {
        8 * keys.len() as u64
    } else {
        keys.iter().map(|k| k.len() as u64).sum()
    };
    Ok((keys.len() as u64, bytes))
}

/// The residual's row test: whether `value` matches `arg` under `op`, both
/// folded. A row whose value is null never matches.
#[must_use]
pub fn matches(op: TextOp, arg: &str, value: &str) -> bool {
    matches_any(op, &[fold(arg)], value)
}

/// Whether `value`, folded once, matches at least one of `folded_args`
/// (each already [`fold`]ed) under `op`: the residual of a run with several
/// values.
#[must_use]
pub fn matches_any(op: TextOp, folded_args: &[String], value: &str) -> bool {
    let v = fold(value);
    match op {
        TextOp::Contains => folded_args.iter().any(|a| v.contains(a.as_str())),
        TextOp::HasToken => words(&v).any(|w| folded_args.iter().any(|a| w == a)),
        TextOp::HasPrefix => {
            words(&v).any(|w| folded_args.iter().any(|a| w.starts_with(a.as_str())))
        }
    }
}

/// `arg` folded, after the checks its op needs: `has_token` takes exactly
/// one word (its folded form must be a single word, whole), `has_prefix` the
/// start of one word (one word by Unicode word boundaries, no white space).
/// `contains` takes any string; the empty string matches
/// every non-null value.
pub fn check_arg(op: TextOp, arg: &str) -> Result<String> {
    let folded = fold(arg);
    match op {
        TextOp::HasToken => {
            let ws: Vec<&str> = words(&folded).collect();
            if ws.as_slice() != [folded.as_str()] {
                return Err(Error::InvalidArgument {
                    field: "text",
                    reason: format!(
                        "has_token takes one word, and {arg:?} folds to {folded:?}, which is {} \
                         by Unicode word boundaries; pass one word, or search the text with \
                         contains",
                        match ws.as_slice() {
                            [] => "no word".to_string(),
                            [one] => format!("the word {one:?} and other characters"),
                            many => format!("{} words", many.len()),
                        }
                    ),
                });
            }
        }
        TextOp::HasPrefix if folded.is_empty() => {
            return Err(Error::InvalidArgument {
                field: "text",
                reason: "has_prefix needs a non-empty prefix; pass at least one character".into(),
            });
        }
        TextOp::HasPrefix => {
            // A word never holds white space, and a prefix spanning a word
            // boundary, or holding no word character, starts no word: it
            // would match nothing, silently.
            let n = words(&folded).count();
            if n != 1 || folded.chars().any(char::is_whitespace) {
                return Err(Error::InvalidArgument {
                    field: "text",
                    reason: format!(
                        "has_prefix takes the start of one word, and {arg:?} folds to {folded:?}, \
                         which {} by Unicode word boundaries, so no word starts with it; pass the \
                         start of one word, or search the text with contains",
                        match n {
                            0 => "holds no word character".to_string(),
                            1 => "contains white space".to_string(),
                            many => format!("spans {many} words"),
                        }
                    ),
                });
            }
        }
        TextOp::Contains => {}
    }
    Ok(folded)
}

/// How one text spec answers one lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Probe {
    /// Row groups holding this stored key.
    Key(Vec<u8>),
    /// Row groups holding a stored key with this byte prefix.
    Prefix(Vec<u8>),
    /// Row groups holding every one of these stored keys (sorted,
    /// distinct, non-empty).
    All(Vec<Vec<u8>>),
    /// Every row group: the argument is too short to prune.
    Unpruned,
}

/// How `spec` serves `op` over the folded argument `folded`, or `None`
/// when it does not (another column, a words spec asked `contains`, a
/// words bloom spec asked `has_prefix`, or not a text spec).
#[must_use]
pub fn probe(spec: &KeySpec, column: &str, op: TextOp, folded: &str) -> Option<Probe> {
    let KeySpec::Text {
        column: c,
        tokenizer,
        bloom,
    } = spec
    else {
        return None;
    };
    if c != column {
        return None;
    }
    match (tokenizer, op) {
        (Tokenizer::Words, TextOp::HasToken) => Some(Probe::Key(word_key(folded))),
        (Tokenizer::Words, TextOp::HasPrefix) if bloom.is_none() => {
            Some(Probe::Prefix(word_prefix_probe(folded)))
        }
        (Tokenizer::Words, _) => None,
        (Tokenizer::Trigram, _) => {
            let keys: BTreeSet<Vec<u8>> = trigrams(folded).map(trigram_key).collect();
            if keys.is_empty() {
                Some(Probe::Unpruned)
            } else {
                Some(Probe::All(keys.into_iter().collect()))
            }
        }
    }
}

/// Whether `spec` answers `op` on `column` at all (with pruning or not):
/// [`probe`] serves it, whatever the argument.
#[must_use]
pub fn serves(spec: &KeySpec, column: &str, op: TextOp) -> bool {
    probe(spec, column, op, "").is_some()
}

/// Whether a plan through `spec` holds only row groups with a matching
/// row: a words dictionary (`has_token`, `has_prefix`). Every other
/// serving spec plans candidates.
#[must_use]
pub fn exact_row_groups(spec: &KeySpec) -> bool {
    matches!(
        spec,
        KeySpec::Text {
            tokenizer: Tokenizer::Words,
            bloom: None,
            ..
        }
    )
}

/// The plan shape of one text lookup over `specs` (every complete spec):
/// the serving specs with their probes, in `specs` order. Both planners
/// follow it, so a transient spec plans what a committed one does.
#[derive(Clone, Debug, PartialEq)]
pub struct Shape {
    /// The folded argument.
    pub folded: String,
    /// Each serving spec and how it serves.
    pub serving: Vec<(KeySpec, Probe)>,
}

impl Shape {
    /// The shape of `op(column, arg)` over `specs`.
    pub fn of(specs: &[KeySpec], column: &str, op: TextOp, arg: &str) -> Result<Self> {
        let folded = check_arg(op, arg)?;
        let serving = specs
            .iter()
            .filter_map(|s| probe(s, column, op, &folded).map(|p| (s.clone(), p)))
            .collect();
        Ok(Self { folded, serving })
    }

    /// Whether some serving spec prunes (a probe other than
    /// [`Probe::Unpruned`]).
    #[must_use]
    pub fn prunes(&self) -> bool {
        self.serving.iter().any(|(_, p)| *p != Probe::Unpruned)
    }

    /// Whether every planned row group holds a matching row: some pruning
    /// serving spec is a words dictionary. The intersection of an exact set
    /// with supersets is still exact.
    #[must_use]
    pub fn exact(&self) -> bool {
        self.serving
            .iter()
            .any(|(s, p)| *p != Probe::Unpruned && exact_row_groups(s))
    }
}

/// What a text spec's segments hold, for pricing: exact counts over the
/// union of the segments' dictionaries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenStats {
    /// Distinct tokens (words or trigrams).
    pub distinct: u64,
    /// Row groups one token is in: nearest-rank p50, p95, and max over
    /// the distinct tokens (zeros when there is none).
    pub row_groups_per_token: [u64; 3],
}

/// [`TokenStats`] of one keyed text spec's segments, by a k-way merge of
/// their sorted dictionaries (one block per segment at a time, ADR 018;
/// the histogram holds one counter per distinct row-group count, so at most
/// one per row group).
pub fn token_stats<R: ReadAt>(readers: &[SegmentReader<R>]) -> Result<TokenStats> {
    let mut histogram: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
    let mut distinct = 0u64;
    crate::merge::visit_merged_keys(readers, |_, bitmap| {
        distinct += 1;
        *histogram.entry(bitmap.len()).or_default() += 1;
        Ok(())
    })?;
    let rank = |q: f64| -> u64 {
        if distinct == 0 {
            return 0;
        }
        // Nearest rank: the smallest count whose cumulative share reaches q.
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let target = ((q * distinct as f64).ceil() as u64).clamp(1, distinct);
        let mut seen = 0u64;
        for (count, n) in &histogram {
            seen += n;
            if seen >= target {
                return *count;
            }
        }
        0
    };
    Ok(TokenStats {
        distinct,
        row_groups_per_token: [rank(0.5), rank(0.95), rank(1.0)],
    })
}

/// The row groups of one segment that `probe` selects. For [`Probe::All`]
/// the keys' bitmaps are intersected within the segment: a segment that
/// processed a row group holds every key of it, so the union over segments
/// of these per-segment sets is the set of row groups holding every key.
/// [`Probe::Unpruned`] selects no set a segment holds (the caller plans
/// every live row group, [`select`]) and is refused.
pub fn select_one<R: ReadAt>(reader: &SegmentReader<R>, probe: &Probe) -> Result<RoaringBitmap> {
    Ok(match probe {
        Probe::Unpruned => {
            return Err(Error::Other(
                "an argument too short to prune selects every live row group, which the planner \
                 takes from the manifest, not from a segment"
                    .into(),
            ))
        }
        Probe::Key(k) => reader.lookup(&[k])?,
        Probe::Prefix(p) => reader.lookup_prefix(p)?,
        Probe::All(keys) => {
            let mut acc: Option<RoaringBitmap> = None;
            for k in keys {
                let b = reader.lookup(&[k])?;
                let next = match acc {
                    Some(a) => a & b,
                    None => b,
                };
                let done = next.is_empty();
                acc = Some(next);
                if done {
                    break;
                }
            }
            acc.unwrap_or_default()
        }
    })
}

/// The row groups `probe` selects across `readers` (one spec's segments,
/// [`select_one`] each, unioned), or `None` for [`Probe::Unpruned`]: the
/// caller plans every live row group.
pub fn select<R: ReadAt>(
    readers: &[SegmentReader<R>],
    probe: &Probe,
) -> Result<Option<RoaringBitmap>> {
    if *probe == Probe::Unpruned {
        return Ok(None);
    }
    let mut out = RoaringBitmap::new();
    for r in readers {
        out |= select_one(r, probe)?;
    }
    Ok(Some(out))
}

/// The tag every word key starts with (`Utf8`), and every trigram key
/// (`Binary`), so a reader can tell a stored key's tokenizer.
#[must_use]
pub fn key_tag(tokenizer: Tokenizer) -> u8 {
    match tokenizer {
        Tokenizer::Words => tag::UTF8,
        Tokenizer::Trigram => tag::BINARY,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn fold_is_per_code_point_full_case_folding() {
        assert_eq!(fold("HeLLo"), "hello");
        assert_eq!(fold("ΣΑΣ"), "σασ"); // no final-sigma context
        assert_eq!(fold("ς"), "σ");
        assert_eq!(fold("İ"), "i\u{307}");
        assert_eq!(fold("Straße"), "strasse");
        assert_eq!(fold("\u{212A}elvin"), "kelvin");
        let (a, b) = ("Ab\u{130}ß", "ΣX");
        assert_eq!(fold(&format!("{a}{b}")), format!("{}{}", fold(a), fold(b)));
    }

    /// The table versions a segment records are the versions the lock file
    /// builds with: an upgrade of either crate must change the recorded
    /// string too, so old segments are refused, never silently misread.
    #[test]
    fn recorded_tables_match_the_locked_crates() {
        let lock = include_str!("../../../Cargo.lock");
        for recorded in [CASE_TABLES, WORD_TABLES] {
            let (name, version) = recorded.split_once(' ').unwrap();
            let entry = format!("name = \"{name}\"\nversion = \"{version}\"\n");
            assert!(
                lock.contains(&entry),
                "{recorded} is not the locked version"
            );
            assert_eq!(
                lock.matches(&format!("name = \"{name}\"\n")).count(),
                1,
                "{name} is locked at more than one version"
            );
        }
    }

    #[test]
    fn tokenizers_split_the_folded_value() {
        let w: Vec<&str> = words("hello, wörld! can't 3.5").collect();
        assert_eq!(w, ["hello", "wörld", "can't", "3.5"]);
        let t: Vec<&[u8]> = trigrams("abcd").collect();
        assert_eq!(t, [b"abc".as_slice(), b"bcd".as_slice()]);
        assert_eq!(trigrams("ab").count(), 0);
        assert_eq!(word_key("a"), [0x06, 0x61, 0x00, 0x00]);
        assert_eq!(
            trigram_key(b"a\0b"),
            [0x07, 0x61, 0x00, 0xff, 0x62, 0x00, 0x00]
        );
        assert_eq!(word_prefix_probe("ab"), [0x06, 0x61, 0x62]);
    }

    #[test]
    fn arguments_decode_from_their_keys() {
        for v in ["", "a", "a\0b", "straße"] {
            assert_eq!(arg_of_key(&word_key(v)).unwrap(), v);
        }
        assert!(arg_of_key(&trigram_key(b"abc")).is_err());
        assert!(arg_of_key(&[0x06, 0x61]).is_err());
        assert!(arg_of_key(&[0x06, 0x61, 0x00, 0x00, 0x01]).is_err());
        assert!(arg_of_key(&[0x06, 0xff, 0x00, 0x00]).is_err());
    }

    #[test]
    fn args_are_checked() {
        assert_eq!(check_arg(TextOp::HasToken, "Hello").unwrap(), "hello");
        assert!(check_arg(TextOp::HasToken, "two words").is_err());
        assert!(check_arg(TextOp::HasToken, "").is_err());
        assert!(check_arg(TextOp::HasToken, "x!").is_err());
        assert!(check_arg(TextOp::HasPrefix, "").is_err());
        assert!(check_arg(TextOp::HasPrefix, "foo b").is_err());
        assert!(check_arg(TextOp::HasPrefix, "foo ").is_err());
        assert!(check_arg(TextOp::HasPrefix, "!").is_err());
        assert_eq!(check_arg(TextOp::HasPrefix, "Can'").unwrap(), "can'");
        assert_eq!(check_arg(TextOp::HasPrefix, "3.").unwrap(), "3.");
        assert_eq!(check_arg(TextOp::Contains, "").unwrap(), "");
    }

    #[test]
    fn matches_folds_both_sides() {
        assert!(matches(TextOp::Contains, "WORLD", "hello world"));
        assert!(matches(TextOp::HasToken, "Hello", "HELLO, world"));
        assert!(!matches(TextOp::HasToken, "hell", "hello"));
        assert!(matches(TextOp::HasPrefix, "hel", "say HELLO"));
        assert!(matches(TextOp::Contains, "", "x"));
    }
}
