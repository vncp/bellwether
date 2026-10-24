//! The short key-spec syntax used on command lines and in messages.
//!
//! | Spec | Short form |
//! |---|---|
//! | `Exact { columns: [a, b] }` | `exact:a,b` |
//! | `Bloom { columns: [a], fpr: 0.01 }` | `bloom:a@0.01` |
//! | `Range { column: ts }` | `range:ts` |
//! | `Temporal { columns: [id], ts_column: ts }` | `temporal:id@ts` |
//!
//! A spec may also be given as its JSON form (`{"kind":"exact",...}`).
//! [`label`] and [`parse`] are inverses. Specs are validated by core's
//! rules (non-empty, distinct columns, `fpr` strictly between 0 and 1).

use bellwether_core::segment::validate_spec;
use bellwether_types::{Error, KeySpec, Result};

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

fn parse_short(s: &str) -> Result<KeySpec> {
    let (kind, rest) = s
        .split_once(':')
        .ok_or_else(|| bad(s, "no kind before ':'"))?;
    let (cols, extra) = match rest.split_once('@') {
        Some((c, e)) => (c, Some(e.trim())),
        None => (rest, None),
    };
    match (kind.trim(), extra) {
        ("exact", None) => Ok(KeySpec::Exact {
            columns: columns(s, cols)?,
        }),
        ("bloom", Some(fpr)) => {
            let fpr: f64 = fpr
                .parse()
                .map_err(|_| bad(s, "the false-positive rate after '@' is not a number"))?;
            Ok(KeySpec::Bloom {
                columns: columns(s, cols)?,
                fpr,
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
        ("temporal", Some(ts)) if !ts.is_empty() => Ok(KeySpec::Temporal {
            columns: columns(s, cols)?,
            ts_column: ts.to_string(),
        }),
        ("temporal", _) => Err(bad(
            s,
            "a temporal spec needs a timestamp column, temporal:id@ts",
        )),
        ("exact" | "range", Some(_)) => Err(bad(s, "only bloom and temporal take '@'")),
        (other, _) => Err(bad(s, &format!("unknown kind {other:?}"))),
    }
}

/// The short form of `spec`; [`parse`] reads it back to an equal spec.
#[must_use]
pub fn label(spec: &KeySpec) -> String {
    match spec {
        KeySpec::Exact { columns } => format!("exact:{}", columns.join(",")),
        KeySpec::Bloom { columns, fpr } => format!("bloom:{}@{fpr}", columns.join(",")),
        KeySpec::Range { column } => format!("range:{column}"),
        KeySpec::Temporal { columns, ts_column } => {
            format!("temporal:{}@{ts_column}", columns.join(","))
        }
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
        ] {
            let spec = parse(s).unwrap();
            assert_eq!(label(&spec), s);
            assert_eq!(parse(&label(&spec)).unwrap(), spec);
        }
        let json = r#"{"kind":"exact","columns":["order_id"]}"#;
        assert_eq!(label(&parse(json).unwrap()), "exact:order_id");
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
}
