//! The short key-spec syntax used on command lines and in messages.
//!
//! | Spec | Short form |
//! |---|---|
//! | `Exact { columns: [a, b] }` | `exact:a,b` |
//! | `Bloom { columns: [a], fpr: 0.01 }` | `bloom:a@0.01` |
//! | `Range { column: ts }` | `range:ts` |
//! | `Temporal { columns: [id], ts_column: ts }` | `temporal:id@ts` |
//! | `Exact { columns: [a, b], counts: true }` | `exact:a,b,counts` |
//! | `Temporal { columns: [id], ts_column: ts, counts: true }` | `temporal:id@ts,counts` |
//! | `Expr { name: sym, expr: json_get(payload,'$.symbol'), over: Exact }` | `expr:sym=json_get(payload,'$.symbol'):exact` |
//! | `Expr { .., over: Exact { counts: true } }` | `expr:sym=json_get(payload,'$.symbol'):exact,counts` |
//! | `Expr { .., over: Bloom { fpr: 0.01 } }` | `expr:sym=json_get(payload,'$.symbol'):bloom@0.01` |
//! | `List { column: tags }` | `list:tags` |
//! | `Exact { columns: [symbol], bounds: Some(ts) }` | `exact:symbol@ts` (with counts `exact:symbol@ts,counts`) |
//! | `Bloom { columns: [symbol], fpr: 0.01, bounds: Some(ts) }` | `bloom:symbol@0.01@ts` |
//! | `Bitemporal { columns: [id], valid_column: valid, known_column: known }` | `temporal2:id@valid,known` |
//! | `Join { dataset: orders, columns: [order_id], right: payables, right_columns: [order_id] }` | `join:orders.order_id=payables.order_id` |
//! | `Join { .., counts: true }` | `join:orders.order_id=payables.order_id,counts` |
//!
//! A trailing `,counts` asks the spec to store, per key and per row group,
//! the number of rows (storage version 2, ADR 028); it is the last element
//! after the columns (exact) or the timestamp column (temporal). A spec of
//! one column named `counts` is `exact:counts`; with counts,
//! `exact:counts,counts`.
//! | `Text { column: body, tokenizer: words, bloom: None }` | `text:body,tokenizer=words` |
//! | `Text { column: body, tokenizer: trigram, bloom: Some(0.01) }` | `text:body,tokenizer=trigram,bloom=0.01` |
//!
//! An `expr` key's expression is in the closed language of
//! `bellwether_core::expr`; it is stored normalized, so `label` gives the
//! normalized spelling, and the kind after the last `:` outside quotes is
//! `exact`, `exact,counts`, or `bloom@<fpr>`.
//!
//! A spec may also be given as its JSON form (`{"kind":"exact",...}`).
//! [`label`] and [`parse`] are inverses. Specs are validated by core's
//! rules (non-empty, distinct columns, `fpr` strictly between 0 and 1).
//!
//! `exact:a@ts` and `bloom:a@0.01@ts` keep per-key time bounds on `ts`,
//! and `temporal2:id@valid,known` keeps bitemporal versions: key specs
//! like any other (storage version 2, M5), which `propose`, `fix` and
//! `why` handle as they handle every key spec.
//!
//! `index --key` also takes `sketch:a,b,a+b[,p=12][,minhash=64]`
//! ([`parse_sketch`]): an approximate per-file distinct-count sketch
//! (`bellwether_types::SketchSpec`) that no planner reads. It is not a key
//! spec, so `parse` refuses it and names `index --key`.

use bellwether_core::segment::validate_spec;
use bellwether_types::{Error, KeySpec, Result, SketchSpec, Tokenizer, SKETCH_DEFAULT_PRECISION};

/// Parses the short form or the JSON form.
pub fn parse(s: &str) -> Result<KeySpec> {
    let s = s.trim();
    let spec = if s.starts_with('{') {
        serde_json::from_str::<KeySpec>(s).map_err(|e| {
            Error::KeySpec(format!(
                "{s:?} is not a key spec ({e}); write it as exact:col, temporal:col@ts, \
                 bloom:col@0.01, range:col, text:col,tokenizer=words, expr:name=<expression>:exact, or
                 its JSON form"
            ))
        })?
    } else if let Some(rest) = s.strip_prefix("expr:") {
        parse_expr(s, rest)?
    } else {
        parse_short(s)?
    };
    check(&spec)?;
    Ok(spec)
}

/// Core's spec rules plus the facade's: no column listed twice (a lookup
/// lists each column once, so such a spec could never serve one).
pub fn check(spec: &KeySpec) -> Result<()> {
    validate_spec(spec)?;
    let cols = spec.columns();
    for (i, c) in cols.iter().enumerate() {
        if cols[..i].contains(c) {
            return Err(Error::KeySpec(format!(
                "key spec {} lists column {c:?} twice; list each column once",
                label(spec)
            )));
        }
    }
    Ok(())
}

fn bad(s: &str, why: &str) -> Error {
    Error::KeySpec(format!(
        "{s:?} is not a key spec ({why}); write it as exact:a,b, exact:a@ts (per-key time \
         bounds), temporal:a@ts, temporal2:a@valid,known, bloom:a@0.01, range:ts, \
         text:body,tokenizer=words (or tokenizer=trigram), expr:name=<expression>:exact, or \
         list:tags"
    ))
}

/// `expr:<name>=<expression>:<kind>`, the kind after the last `:` that is
/// outside quotes (an expression's strings and quoted columns may hold
/// `:`), `exact` or `bloom@<fpr>`.
fn parse_expr(s: &str, rest: &str) -> Result<KeySpec> {
    let bad_expr = |why: &str| {
        Error::KeySpec(format!(
            "{s:?} is not an expr key ({why}); write it as \
             expr:<name>=<expression>:exact or expr:<name>=<expression>:bloom@0.01, for \
             example expr:sym=json_get(payload,'$.symbol'):exact"
        ))
    };
    let (name, tail) = rest
        .split_once('=')
        .ok_or_else(|| bad_expr("no '=' after the name"))?;
    let mut quote: Option<char> = None;
    let mut split = None;
    for (i, c) in tail.char_indices() {
        match (quote, c) {
            (None, '\'' | '"') => quote = Some(c),
            (Some(q), c) if c == q => quote = None,
            (None, ':') => split = Some(i),
            _ => {}
        }
    }
    let i = split.ok_or_else(|| bad_expr("no ':<kind>' after the expression"))?;
    let (text, kind) = (tail[..i].trim(), tail[i + 1..].trim());
    let over = match kind.split_once('@') {
        None if kind == "exact" => bellwether_core::expr::Over::Exact,
        None if kind.replace(' ', "") == "exact,counts" => bellwether_core::expr::Over::Counts,
        Some(("bloom", fpr)) => bellwether_core::expr::Over::Bloom(
            fpr.trim()
                .parse()
                .map_err(|_| bad_expr("the false-positive rate after '@' is not a number"))?,
        ),
        None if kind == "bloom" => {
            return Err(bad_expr(
                "a bloom expr key needs a false-positive rate, :bloom@0.01",
            ))
        }
        _ => {
            return Err(bad_expr(&format!(
                "the kind after the expression is exact, exact,counts, or bloom@<fpr>, not \
                 {kind:?}"
            )))
        }
    };
    bellwether_core::expr::spec(name.trim(), text, over)
}

fn columns(s: &str, list: &str) -> Result<Vec<String>> {
    let cols: Vec<String> = list.split(',').map(|c| c.trim().to_string()).collect();
    if cols.iter().any(String::is_empty) {
        return Err(bad(s, "a column name is empty"));
    }
    Ok(cols)
}

/// `list` without a trailing `,counts` element, and whether it had one.
/// The element counts only after at least one other: `counts` alone is a
/// column name.
fn strip_counts(list: &str) -> (&str, bool) {
    match list.trim_end().rsplit_once(',') {
        Some((head, "counts")) if !head.trim().is_empty() => (head, true),
        _ => (list, false),
    }
}

#[allow(clippy::too_many_lines)]
fn parse_short(s: &str) -> Result<KeySpec> {
    let (kind, rest) = s
        .split_once(':')
        .ok_or_else(|| bad(s, "no kind before ':'"))?;
    let (cols, extra) = match rest.split_once('@') {
        Some((c, e)) => (c, Some(e.trim())),
        None => (rest, None),
    };
    match (kind.trim(), extra) {
        ("exact", None) => {
            let (cols, counts) = strip_counts(cols);
            Ok(KeySpec::Exact {
                columns: columns(s, cols)?,
                counts,
                bounds: None,
            })
        }
        ("bloom", Some(rest)) => {
            // bloom:a@0.01, or bloom:a@0.01@ts with per-key bounds on ts
            let (fpr, bounds) = match rest.split_once('@') {
                Some((fpr, ts)) => (fpr, Some(one_column(s, ts, "bounds")?)),
                None => (rest, None),
            };
            let fpr: f64 = fpr
                .trim()
                .parse()
                .map_err(|_| bad(s, "the false-positive rate after '@' is not a number"))?;
            Ok(KeySpec::Bloom {
                columns: columns(s, cols)?,
                fpr,
                bounds,
            })
        }
        ("bloom", None) => Err(bad(
            s,
            "a bloom spec needs a false-positive rate, bloom:a@0.01",
        )),
        ("range", None) => {
            let c = columns(s, cols)?;
            match c.as_slice() {
                [one] => Ok(KeySpec::Range {
                    column: one.clone(),
                }),
                _ => Err(bad(s, "a range spec has exactly one column")),
            }
        }
        ("temporal", Some(ts)) if !ts.is_empty() => {
            let (ts, counts) = strip_counts(ts);
            match columns(s, ts)?.as_slice() {
                [one] => Ok(KeySpec::Temporal {
                    columns: columns(s, cols)?,
                    ts_column: one.clone(),
                    counts,
                }),
                _ => Err(bad(
                    s,
                    "a temporal spec has one timestamp column, then optionally counts: \
                     temporal:id@ts or temporal:id@ts,counts",
                )),
            }
        }
        ("temporal", _) => Err(bad(
            s,
            "a temporal spec needs a timestamp column, temporal:id@ts",
        )),
        ("exact", Some(ts)) => {
            // exact:a@ts: per-key time bounds on ts; the counts option
            // follows the time column.
            let (ts, counts) = strip_counts(ts);
            Ok(KeySpec::Exact {
                columns: columns(s, cols)?,
                counts,
                bounds: Some(one_column(s, ts, "bounds")?),
            })
        }
        ("temporal2", Some(times)) => match columns(s, times)?.as_slice() {
            [valid, known] => Ok(KeySpec::Bitemporal {
                columns: columns(s, cols)?,
                valid_column: valid.clone(),
                known_column: known.clone(),
            }),
            _ => Err(bad(
                s,
                "a bitemporal key names its valid and known time columns, \
                 temporal2:id@valid,known",
            )),
        },
        ("temporal2", None) => Err(bad(
            s,
            "a bitemporal key names its valid and known time columns, temporal2:id@valid,known",
        )),
        ("range", Some(_)) => Err(bad(s, "a range spec takes no '@'")),
        ("list", None) => match columns(s, cols)?.as_slice() {
            [one] => Ok(KeySpec::List {
                column: one.clone(),
            }),
            _ => Err(bad(
                s,
                "a list key indexes the elements of exactly one list column, list:tags",
            )),
        },
        ("list", Some(_)) => Err(bad(s, "a list key takes no '@'; write list:<column>")),
        ("sketch", _) => Err(bad(
            s,
            "a sketch is an approximate distinct-count sidecar, not a key spec; declare it with \
             `index --key`",
        )),
        ("text", None) => parse_text(s, cols),
        ("text", Some(_)) => Err(bad(s, "a text key takes no '@'")),
        ("join", None) => parse_join(s, cols),
        ("join", Some(_)) => Err(bad(
            s,
            "a join key takes no '@'; write join:<left>.<columns>=<right>.<columns>",
        )),
        (other, _) => Err(bad(s, &format!("unknown kind {other:?}"))),
    }
}

/// The one time column after an `@` (`what` names its role in a refusal).
fn one_column(s: &str, list: &str, what: &str) -> Result<String> {
    match columns(s, list)?.as_slice() {
        [one] => Ok(one.clone()),
        _ => Err(bad(s, &format!("the {what} time column is one column"))),
    }
}

/// `join:<left>.<cols>=<right>.<cols>[,counts]`: each side a dataset (a
/// folder name, which holds no `.`) then its columns after the first `.`.
fn parse_join(s: &str, rest: &str) -> Result<KeySpec> {
    let (left, right) = rest.split_once('=').ok_or_else(|| {
        bad(
            s,
            "a join key pairs two sides with '=', join:orders.order_id=payables.order_id",
        )
    })?;
    let (right_list, counts) = strip_counts(right);
    let (dataset, columns) = join_side(s, left)?;
    let (right, right_columns) = join_side(s, right_list)?;
    Ok(KeySpec::Join {
        dataset,
        columns,
        right,
        right_columns,
        counts,
    })
}

/// One side of a join key: `<dataset>.<col>[,<col>...]`.
fn join_side(s: &str, side: &str) -> Result<(String, Vec<String>)> {
    let (dataset, cols) = side.trim().split_once('.').ok_or_else(|| {
        bad(
            s,
            "each side of a join key is <dataset>.<columns>, join:orders.order_id=payables.order_id",
        )
    })?;
    Ok((dataset.trim().to_string(), columns(s, cols)?))
}

/// The right side a join lookup names (`--join`): `<right dataset>.<col>[,<col>...]`,
/// the folder beside the data root and its join columns, pairwise with the
/// key file's (or the left side's) columns.
pub fn parse_join_side(s: &str) -> Result<(String, Vec<String>)> {
    let s = s.trim();
    let bad_side = |why: &str| Error::InvalidArgument {
        field: "join",
        reason: format!(
            "{s:?} does not name a right dataset and its columns ({why}); write \
             <right dataset>.<column>[,<column>...], for example payables.order_id, or the \
             whole key, join:orders.order_id=payables.order_id"
        ),
    };
    let (dataset, cols) = s.split_once('.').ok_or_else(|| bad_side("no '.'"))?;
    let columns: Vec<String> = cols.split(',').map(|c| c.trim().to_string()).collect();
    if dataset.trim().is_empty() || columns.iter().any(String::is_empty) {
        return Err(bad_side("a name is empty"));
    }
    Ok((dataset.trim().to_string(), columns))
}

/// `text:<column>,tokenizer=words|trigram[,bloom=<fpr>]`: the column, then
/// each parameter once, in any order.
fn parse_text(s: &str, rest: &str) -> Result<KeySpec> {
    let mut parts = rest.split(',').map(str::trim);
    let column = parts.next().unwrap_or_default();
    if column.is_empty() || column.contains('=') {
        return Err(bad(
            s,
            "a text key names its column first, text:body,tokenizer=words",
        ));
    }
    let (mut tokenizer, mut bloom) = (None, None);
    for p in parts {
        match p.split_once('=') {
            Some(("tokenizer", t)) if tokenizer.is_none() => {
                tokenizer = Some(match t.trim() {
                    "words" => Tokenizer::Words,
                    "trigram" => Tokenizer::Trigram,
                    other => {
                        let why = format!("unknown tokenizer {other:?}; use words or trigram");
                        return Err(bad(s, &why));
                    }
                });
            }
            Some(("bloom", f)) if bloom.is_none() => {
                bloom = Some(f.trim().parse::<f64>().map_err(|_| {
                    bad(
                        s,
                        "the bloom false-positive rate is not a number, bloom=0.01",
                    )
                })?);
            }
            Some(("tokenizer" | "bloom", _)) => {
                return Err(bad(s, &format!("{p:?} is given twice")));
            }
            _ => {
                return Err(bad(
                    s,
                    &format!(
                        "{p:?} is not a text key parameter; a text key takes tokenizer= and bloom="
                    ),
                ))
            }
        }
    }
    let tokenizer = tokenizer.ok_or_else(|| {
        bad(
            s,
            "a text key names its tokenizer, text:body,tokenizer=words (word tokens: has_token, \
             has_prefix) or text:body,tokenizer=trigram (substrings: contains)",
        )
    })?;
    Ok(KeySpec::Text {
        column: column.to_string(),
        tokenizer,
        bloom,
    })
}

/// The short form of `spec`: its identity ([`KeySpec::identity`],
/// `docs/format.md`), which [`parse`] reads back to an equal spec.
#[must_use]
pub fn label(spec: &KeySpec) -> String {
    spec.identity()
}

/// What one `index --key` argument declares: a key spec ([`parse`]), or a
/// sketch (`sketch:...`, [`parse_sketch`]), an approximate structure no
/// planner reads and so not a key spec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Declaration {
    /// The key spec, if the argument declares one.
    pub key: Option<KeySpec>,
    /// The sketch, if the argument declares one (`sketch:...`); never with
    /// a key.
    pub sketch: Option<SketchSpec>,
}

/// Parses one `index --key` argument (see [`Declaration`]): everything
/// [`parse`] reads, and `sketch:...`.
pub fn parse_declaration(s: &str) -> Result<Declaration> {
    let t = s.trim();
    if t.starts_with("sketch:") {
        return Ok(Declaration {
            key: None,
            sketch: Some(parse_sketch(t)?),
        });
    }
    Ok(Declaration {
        key: Some(parse(t)?),
        sketch: None,
    })
}

/// The `index --key` argument that declares `declared`;
/// [`parse_declaration`] reads it back to an equal declaration.
#[must_use]
pub fn declaration_label(declared: &Declaration) -> String {
    match (&declared.sketch, &declared.key) {
        (Some(sketch), _) => sketch.label(),
        (None, Some(key)) => label(key),
        (None, None) => String::new(),
    }
}

/// Parses `sketch:<item>[,<item>...][,p=<precision>][,minhash=<k>]`: each
/// item is a column or a `+`-joined tuple of columns; `p` defaults to
/// [`SKETCH_DEFAULT_PRECISION`]; without `minhash` no signature is kept.
/// [`SketchSpec::label`] is its inverse.
pub fn parse_sketch(s: &str) -> Result<SketchSpec> {
    let t = s.trim();
    let bad = |why: &str| {
        Error::KeySpec(format!(
            "{t:?} is not a sketch ({why}); write it as sketch:a,b,a+b with optional ,p=14 and \
             ,minhash=64"
        ))
    };
    let rest = t
        .strip_prefix("sketch:")
        .ok_or_else(|| bad("it does not start with sketch:"))?;
    let mut items = Vec::new();
    let mut precision = None;
    let mut minhash = None;
    for part in rest.split(',').map(str::trim) {
        if let Some((k, v)) = part.split_once('=') {
            let dup = match k.trim() {
                "p" => precision
                    .replace(
                        v.trim()
                            .parse::<u8>()
                            .map_err(|_| bad("p is not an integer"))?,
                    )
                    .is_some(),
                "minhash" => minhash
                    .replace(
                        v.trim()
                            .parse::<u16>()
                            .map_err(|_| bad("minhash is not an integer"))?,
                    )
                    .is_some(),
                other => return Err(bad(&format!("unknown parameter {other:?}"))),
            };
            if dup {
                return Err(bad("a parameter is given twice"));
            }
            continue;
        }
        if precision.is_some() || minhash.is_some() {
            return Err(bad("parameters come after every item"));
        }
        if part.is_empty() {
            return Err(bad("an item is empty"));
        }
        items.push(
            part.split('+')
                .map(|c| c.trim().to_string())
                .collect::<Vec<_>>(),
        );
    }
    let spec = SketchSpec {
        items,
        precision: precision.unwrap_or(SKETCH_DEFAULT_PRECISION),
        minhash,
    };
    bellwether_core::sketch::validate(&spec)?;
    Ok(spec)
}

/// `s` as one POSIX shell word, for a command a message names: unchanged
/// when every character is one a shell never interprets, else in single
/// quotes, each `'` written `'\''`.
#[must_use]
pub fn shell_quote(s: &str) -> String {
    bellwether_core::expr::shell_quote(s)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn short_forms_round_trip() {
        for s in [
            "exact:a",
            "exact:a,b",
            "bloom:a,b@0.01",
            "range:ts",
            "temporal:id@ts",
            "temporal:id,v@ts",
            "exact:a,counts",
            "exact:a,b,counts",
            "exact:counts",
            "exact:counts,counts",
            "temporal:id@ts,counts",
            "temporal:id@counts",
            "text:body,tokenizer=words",
            "text:body,tokenizer=trigram",
            "text:body,tokenizer=trigram,bloom=0.01",
            "exact:symbol@ts",
            "exact:a,b@ts,counts",
            "bloom:a,b@0.01@ts",
            "temporal2:id@valid,known",
            "join:orders.order_id=payables.order_id",
            "join:trades.symbol,minute=quotes.sym,minute,counts",
            "join:orders.counts=payables.order_id",
        ] {
            let spec = parse(s).unwrap();
            assert_eq!(label(&spec), s);
            assert_eq!(parse(&label(&spec)).unwrap(), spec);
        }
        let json = r#"{"kind":"exact","columns":["order_id"]}"#;
        assert_eq!(label(&parse(json).unwrap()), "exact:order_id");
        assert_eq!(
            parse("exact:a,b,counts").unwrap(),
            KeySpec::Exact {
                columns: vec!["a".into(), "b".into()],
                counts: true,
                bounds: None,
            }
        );
        assert_eq!(
            parse("temporal:id@ts,counts").unwrap(),
            KeySpec::Temporal {
                columns: vec!["id".into()],
                ts_column: "ts".into(),
                counts: true,
            }
        );
        assert!(parse("temporal:id@ts,known").is_err());
        let d = parse_declaration("exact:a@ts,counts").unwrap();
        assert_eq!(d.key.as_ref().map(KeySpec::counts), Some(true));
        assert_eq!(declaration_label(&d), "exact:a@ts,counts");
        assert_eq!(parse_declaration(&declaration_label(&d)).unwrap(), d);
    }

    #[test]
    fn malformed_specs_are_refused() {
        for s in [
            "",
            "exact",
            "exact:",
            "exact:a,,b",
            "bloom:a",
            "bloom:a@x",
            "bloom:a@1.5",
            "range:a,b",
            "temporal:a",
            "temporal:a@",
            "hash:a",
            "exact:a,a",
            "{\"kind\":\"zorder\"}",
            "text:body",
            "text:body,tokenizer=ngram",
            "text:body,tokenizer=words,tokenizer=trigram",
            "text:,tokenizer=words",
            "text:a,b,tokenizer=words",
            "text:body,tokenizer=words,bloom=2",
            "text:body,tokenizer=words,bloom=x",
            "text:body@ts,tokenizer=words",
        ] {
            assert!(parse(s).is_err(), "{s}");
        }
    }

    /// Per-key bounds and bitemporal versions are key specs (M5).
    #[test]
    fn bounds_and_bitemporal_versions_are_key_specs() {
        assert_eq!(
            parse("exact:symbol@ts").unwrap(),
            KeySpec::Exact {
                columns: vec!["symbol".into()],
                counts: false,
                bounds: Some("ts".into()),
            }
        );
        assert_eq!(
            parse("bloom:a,b@0.01@ts").unwrap(),
            KeySpec::Bloom {
                columns: vec!["a".into(), "b".into()],
                fpr: 0.01,
                bounds: Some("ts".into()),
            }
        );
        assert_eq!(
            parse("temporal2:id@valid,known").unwrap(),
            KeySpec::Bitemporal {
                columns: vec!["id".into()],
                valid_column: "valid".into(),
                known_column: "known".into(),
            }
        );
        let d = parse_declaration("temporal2:id@valid,known").unwrap();
        assert_eq!(d.key.unwrap().name(), "temporal2:id");
        for bad in [
            "temporal2:id",
            "temporal2:id@valid",
            "temporal2:id@valid,known,x",
            "temporal2:id@id,known",
            "temporal2:id@valid,valid",
            "exact:a@",
            "exact:a@a",
            "exact:a@t1,t2",
            "bloom:a@0.01@",
            "bloom:a@0.01@a",
            "range:ts@x",
        ] {
            assert!(parse_declaration(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn declaration_labels_read_back_to_equal_declarations() {
        for s in [
            "exact:symbol@ts",
            "bloom:a,b@0.01@ts",
            "temporal2:id@valid,known",
            "temporal:id@ts",
            "exact:a,b",
            "range:ts",
            "sketch:id",
            "sketch:batch,id,symbol+ts,p=12,minhash=64",
            "sketch:a,minhash=8",
        ] {
            let d = parse_declaration(s).unwrap();
            assert_eq!(declaration_label(&d), s);
            assert_eq!(parse_declaration(&declaration_label(&d)).unwrap(), d);
        }
    }

    #[test]
    fn sketches_parse_and_malformed_ones_name_the_problem() {
        let d = parse_declaration("sketch:batch, id ,symbol+ts,p=12,minhash=64").unwrap();
        assert_eq!(d.key, None);
        let s = d.sketch.unwrap();
        assert_eq!(
            s.items,
            vec![
                vec!["batch".to_string()],
                vec!["id".to_string()],
                vec!["symbol".to_string(), "ts".to_string()]
            ]
        );
        assert_eq!((s.precision, s.minhash), (12, Some(64)));
        assert_eq!(s.name(), "sketch:batch,id,symbol+ts");
        let d = parse_declaration("sketch:id").unwrap().sketch.unwrap();
        assert_eq!((d.precision, d.minhash), (14, None));
        for (bad, why) in [
            ("sketch:", "empty"),
            ("sketch:a,,b", "empty"),
            ("sketch:a,p=x", "integer"),
            ("sketch:a,p=3", "precision"),
            ("sketch:a,q=3", "unknown parameter"),
            ("sketch:a,p=12,b", "after every item"),
            ("sketch:a,p=12,p=13", "twice"),
            ("sketch:a,a", "twice"),
            ("sketch:a+a", "twice"),
            ("sketch:a,minhash=0", "minhash"),
        ] {
            let e = parse_declaration(bad).unwrap_err().to_string();
            assert!(e.contains(why), "{bad}: {e}");
        }
        // A sketch is not a key spec.
        let e = parse("sketch:a").unwrap_err().to_string();
        assert!(e.contains("index --key"), "{e}");
    }

    #[test]
    fn shell_quote_makes_one_word() {
        assert_eq!(shell_quote("/a/b-c_d.parquet"), "/a/b-c_d.parquet");
        assert_eq!(shell_quote("temporal2:id@v,k"), "temporal2:id@v,k");
        assert_eq!(shell_quote("/tmp/my lake"), "'/tmp/my lake'");
        assert_eq!(shell_quote("{day:date}.parquet"), "'{day:date}.parquet'");
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn join_keys_parse_both_sides_and_refuse_what_cannot_be_one() {
        assert_eq!(
            parse("join:trades.symbol,minute=quotes.sym,minute,counts").unwrap(),
            KeySpec::Join {
                dataset: "trades".into(),
                columns: vec!["symbol".into(), "minute".into()],
                right: "quotes".into(),
                right_columns: vec!["sym".into(), "minute".into()],
                counts: true,
            }
        );
        for (bad, why) in [
            ("join:orders.id", "pairs two sides"),
            ("join:orders=payables.id", "<dataset>.<columns>"),
            (
                "join:orders.id=payables.a,b",
                "1 left column(s) with 2 right",
            ),
            ("join:or ders.id=payables.id", "not a folder name"),
            ("join:orders.id,id=payables.a,b", "twice"),
            ("join:orders.id=payables.id@ts", "takes no '@'"),
        ] {
            let e = parse(bad).map(|s| label(&s)).unwrap_err().to_string();
            assert!(e.contains(why), "{bad}: {e}");
        }
        assert_eq!(
            parse_join_side("payables.order_id").unwrap(),
            ("payables".into(), vec!["order_id".into()])
        );
        assert_eq!(
            parse_join_side(" quotes.sym,minute ").unwrap(),
            ("quotes".into(), vec!["sym".into(), "minute".into()])
        );
        for bad in ["payables", ".id", "payables.", "payables.a,"] {
            assert!(parse_join_side(bad).is_err(), "{bad}");
        }
    }
}
