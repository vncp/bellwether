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
//!
//! A trailing `,counts` asks the spec to store, per key and per row group,
//! the number of rows (storage version 2, ADR 028); it is the last element
//! after the columns (exact) or the timestamp column (temporal). A spec of
//! one column named `counts` is `exact:counts`; with counts,
//! `exact:counts,counts`.
//!
//! A spec may also be given as its JSON form (`{"kind":"exact",...}`).
//! [`label`] and [`parse`] are inverses. Specs are validated by core's
//! rules (non-empty, distinct columns, `fpr` strictly between 0 and 1).
//!
//! `index --key` also takes declarations that add a sidecar
//! ([`parse_declaration`], WP21): `exact:a@ts` and `bloom:a@0.01@ts`
//! declare the key spec plus per-key time bounds on `ts`
//! (`bounds:a@ts`); `temporal2:id@valid,known` declares a bitemporal
//! sidecar alone. A key spec itself never carries the bound, so `parse`
//! refuses `exact:a@ts` and names `index --key`.
//!
//! `index --key` also takes `sketch:a,b,a+b[,p=12][,minhash=64]`
//! ([`parse_sketch`]): an approximate per-file distinct-count sketch
//! (`bellwether_types::SketchSpec`) that no planner reads. It is not a key
//! spec, so `parse` refuses it and names `index --key`.

use bellwether_core::segment::validate_spec;
use bellwether_core::sidecar::{SidecarDef, SidecarKind};
use bellwether_types::{Error, KeySpec, Result, SketchSpec, SKETCH_DEFAULT_PRECISION};

/// Parses the short form or the JSON form.
pub fn parse(s: &str) -> Result<KeySpec> {
    let s = s.trim();
    let spec = if s.starts_with('{') {
        serde_json::from_str::<KeySpec>(s).map_err(|e| {
            Error::KeySpec(format!(
                "{s:?} is not a key spec ({e}); write it as exact:col, temporal:col@ts, \
                 bloom:col@0.01, range:col, or its JSON form"
            ))
        })?
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
        "{s:?} is not a key spec ({why}); write it as exact:a,b, temporal:a@ts, bloom:a@0.01, \
         or range:ts"
    ))
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
        ("bloom", Some(fpr)) => {
            let fpr: f64 = fpr
                .parse()
                .map_err(|_| bad(s, "the false-positive rate after '@' is not a number"))?;
            Ok(KeySpec::Bloom {
                columns: columns(s, cols)?,
                fpr,
                bounds: None,
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
        ("exact", Some(_)) => Err(bad(
            s,
            "exact:a@ts declares per-key time bounds beside the key, which only `index --key` \
             accepts; a key spec itself has no '@'",
        )),
        ("range", Some(_)) => Err(bad(s, "a range spec takes no '@'")),
        ("sketch", _) => Err(bad(
            s,
            "a sketch is an approximate distinct-count sidecar, not a key spec; declare it with \
             `index --key`",
        )),
        (other, _) => Err(bad(s, &format!("unknown kind {other:?}"))),
    }
}

/// The short form of `spec`: its identity ([`KeySpec::identity`],
/// `docs/format.md`), which [`parse`] reads back to an equal spec.
#[must_use]
pub fn label(spec: &KeySpec) -> String {
    spec.identity()
}

/// What one `index --key` argument declares: a key spec, a sidecar that
/// rides beside one (per-key time bounds, `exact:symbol@ts` or
/// `bloom:symbol@0.01@ts`), or a sidecar alone (`temporal2:id@valid,known`,
/// bitemporal versions).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Declaration {
    /// The key spec, if the argument declares one.
    pub key: Option<KeySpec>,
    /// The sidecar, if the argument declares one.
    pub sidecar: Option<SidecarDef>,
    /// The sketch, if the argument declares one (`sketch:...`); never with a
    /// key or a sidecar.
    pub sketch: Option<SketchSpec>,
}

/// Parses one `index --key` argument (see [`Declaration`]): everything
/// [`parse`] reads, plus `exact:<cols>@<ts>`, `bloom:<cols>@<fpr>@<ts>`, and
/// `temporal2:<cols>@<valid>,<known>`.
pub fn parse_declaration(s: &str) -> Result<Declaration> {
    let t = s.trim();
    let sidecar = |kind: SidecarKind, cols: &str, times: &str| -> Result<SidecarDef> {
        let times = columns(t, times)?;
        let def = SidecarDef {
            kind,
            columns: columns(t, cols)?,
            time_columns: times,
        };
        def.validate().map_err(|e| {
            Error::KeySpec(format!("{t:?} is not a valid declaration: {}", message(&e)))
        })?;
        Ok(def)
    };
    if let Some(rest) = t.strip_prefix("temporal2:") {
        let (cols, times) = rest.split_once('@').ok_or_else(|| {
            bad(
                t,
                "a bitemporal declaration names its valid and known time columns, \
                 temporal2:id@valid,known",
            )
        })?;
        return Ok(Declaration {
            key: None,
            sidecar: Some(sidecar(SidecarKind::Bitemporal, cols, times)?),
            sketch: None,
        });
    }
    if t.starts_with("sketch:") {
        return Ok(Declaration {
            key: None,
            sidecar: None,
            sketch: Some(parse_sketch(t)?),
        });
    }
    let bounded = match t.split_once(':') {
        // exact:a@ts,counts: the counts option belongs to the key spec.
        Some(("exact", rest)) => rest.split_once('@').map(|(cols, ts)| {
            let (ts, counts) = strip_counts(ts);
            let key = if counts {
                format!("exact:{cols},counts")
            } else {
                format!("exact:{cols}")
            };
            (key, cols.to_string(), ts.to_string())
        }),
        Some(("bloom", rest)) => {
            let parts: Vec<&str> = rest.split('@').collect();
            match parts.as_slice() {
                [cols, fpr, ts] => Some((
                    format!("bloom:{cols}@{fpr}"),
                    (*cols).to_string(),
                    (*ts).to_string(),
                )),
                _ => None,
            }
        }
        _ => None,
    };
    match bounded {
        Some((key, cols, ts)) => {
            let key = parse(&key)?;
            let def = sidecar(SidecarKind::Bounds, &cols, &ts)?;
            Ok(Declaration {
                key: Some(key),
                sidecar: Some(def),
                sketch: None,
            })
        }
        None => Ok(Declaration {
            key: Some(parse(t)?),
            sidecar: None,
            sketch: None,
        }),
    }
}

/// The short form of a sidecar: `bounds:symbol@ts` or
/// `temporal2:id@valid,known` (its name in the manifest).
#[must_use]
pub fn sidecar_label(def: &SidecarDef) -> String {
    def.name()
}

/// The `index --key` argument that declares `declared`;
/// [`parse_declaration`] reads it back to an equal declaration.
#[must_use]
pub fn declaration_label(declared: &Declaration) -> String {
    if let Some(sketch) = &declared.sketch {
        return sketch.label();
    }
    match (&declared.key, &declared.sidecar) {
        (Some(key), Some(def)) if def.kind == SidecarKind::Bounds => {
            // exact:a@ts,counts: the counts option follows the time column.
            let mut bare = key.clone();
            if let KeySpec::Exact { counts, .. } = &mut bare {
                *counts = false;
            }
            let counts = if key.counts() { ",counts" } else { "" };
            format!("{}@{}{counts}", label(&bare), def.time_columns.join(","))
        }
        (_, Some(def)) => def.name(),
        (Some(key), None) => label(key),
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
    let plain = !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "_@%+=:,./-".contains(c));
    if plain {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', r"'\''"))
    }
}

fn message(e: &Error) -> String {
    match e {
        Error::KeySpec(m) | Error::Other(m) => m.clone(),
        other => other.to_string(),
    }
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
            "exact:a@ts",
            "bloom:a",
            "bloom:a@x",
            "bloom:a@1.5",
            "range:a,b",
            "temporal:a",
            "temporal:a@",
            "hash:a",
            "exact:a,a",
            "{\"kind\":\"zorder\"}",
        ] {
            assert!(parse(s).is_err(), "{s}");
        }
    }

    #[test]
    fn declarations_add_sidecars_beside_or_instead_of_a_key() {
        let d = parse_declaration("exact:symbol@ts").unwrap();
        assert_eq!(d.key, Some(parse("exact:symbol").unwrap()));
        let s = d.sidecar.unwrap();
        assert_eq!(
            (s.kind, sidecar_label(&s)),
            (SidecarKind::Bounds, "bounds:symbol@ts".into())
        );
        let d = parse_declaration("bloom:a,b@0.01@ts").unwrap();
        assert_eq!(d.key, Some(parse("bloom:a,b@0.01").unwrap()));
        assert_eq!(d.sidecar.unwrap().time_columns, vec!["ts".to_string()]);
        let d = parse_declaration("temporal2:id@valid,known").unwrap();
        assert_eq!(d.key, None);
        let s = d.sidecar.unwrap();
        assert_eq!(s.kind, SidecarKind::Bitemporal);
        assert_eq!(
            s.time_columns,
            vec!["valid".to_string(), "known".to_string()]
        );
        let d = parse_declaration("temporal:id@ts").unwrap();
        assert_eq!((d.key.is_some(), d.sidecar), (true, None));
        for bad in [
            "temporal2:id",
            "temporal2:id@valid",
            "temporal2:id@valid,known,x",
            "temporal2:id@id,known",
            "exact:a@",
            "exact:a@a",
            "exact:a@t1,t2",
            "bloom:a@0.01@",
            "range:ts@x",
        ] {
            assert!(parse_declaration(bad).is_err(), "{bad}");
        }
        // A key spec alone never carries the bound.
        assert!(parse("exact:symbol@ts").is_err());
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
        assert_eq!((d.key, d.sidecar), (None, None));
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
}
