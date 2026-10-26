//! The `expr` language: normalization (two spellings of one expression
//! normalize equal; a near miss does not), refusals with a next step, and
//! every function against an independent oracle (property tests).
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampMicrosecondType, UInt64Type};
use bellwether_core::expr::{self, json_get, normalize, parse_path, Expr, Over, Typed};
use bellwether_types::{Error, KeySpec};
use proptest::prelude::*;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("payload", DataType::Utf8, true),
        Field::new("Payload", DataType::Utf8, true),
        Field::new("symbol", DataType::Utf8, true),
        Field::new("exchange", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("odd name", DataType::Utf8, true),
    ])
}

#[test]
fn differently_spelled_equal_expressions_normalize_equal() {
    let groups: &[&[&str]] = &[
        &[
            "json_get(payload,'$.symbol')",
            "JSON_GET( payload , '$.symbol' )",
            "json_get(payload, '$[\"symbol\"]')",
            "json_get(\"payload\", '$[''symbol'']')",
            "Json_Get(\n\tpayload,'$.symbol')",
        ],
        &[
            "date_trunc(ts,day)",
            "date_trunc(ts, DAY)",
            "DATE_TRUNC(ts, 'day')",
            "date_trunc('day', ts)",
        ],
        &["cast(n,utf8)", "CAST(n, 'UTF8')", "cast( \"n\" ,Utf8 )"],
        &["hash(symbol,exchange)", "HASH(symbol , exchange)"],
        &[
            "substr(lower(symbol),1,3)",
            "SUBSTR( LOWER(symbol) , 1 , 3 )",
        ],
        &[
            "json_get(payload,'$.a[\"b c\"][0]')",
            "json_get(payload,'$[''a''][\"b c\"][0]')",
        ],
        &["lower(\"odd name\")", "LOWER( \"odd name\" )"],
    ];
    for g in groups {
        let first = normalize(g[0]).unwrap();
        for s in &g[1..] {
            assert_eq!(normalize(s).unwrap(), first, "{s} vs {}", g[0]);
        }
        // normalized text is a fixed point
        assert_eq!(normalize(&first).unwrap(), first);
    }
    assert_eq!(
        normalize("JSON_GET( payload , '$[\"symbol\"]' )").unwrap(),
        "json_get(payload,'$.symbol')"
    );
    assert_eq!(
        normalize("date_trunc('day', ts)").unwrap(),
        "date_trunc(ts,day)"
    );
}

#[test]
fn near_misses_normalize_differently() {
    let base = normalize("json_get(payload,'$.symbol')").unwrap();
    for near in [
        "json_get(payload,'$.Symbol')",
        "json_get(Payload,'$.symbol')",
        "json_get(payload,'$.symbol[0]')",
        "json_get(payload,'$.symbol.x')",
        "lower(json_get(payload,'$.symbol'))",
        "json_get(payload,'$[\"symbol \"]')",
        "json_get(symbol,'$.symbol')",
    ] {
        assert_ne!(normalize(near).unwrap(), base, "{near}");
    }
    assert_ne!(
        normalize("date_trunc(ts,hour)").unwrap(),
        normalize("date_trunc(ts,day)").unwrap()
    );
}

#[test]
fn refusals_name_the_language_and_what_to_write() {
    let cases: &[(&str, &str)] = &[
        (
            "upper(symbol)",
            "json_get, lower, substr, cast, date_trunc, and hash",
        ),
        ("json_get(payload, symbol)", "string literal"),
        ("json_get(payload,'symbol')", "must start with $"),
        ("json_get(payload,'$.a b')", "a step starts with"),
        ("json_get(payload,'$[01]')", "leading zeros"),
        ("substr(symbol,0,2)", "1-based"),
        ("substr(symbol,1,-1)", "cannot be negative"),
        ("substr(symbol,n,2)", "integer literal"),
        ("cast(n,float64)", "int64, utf8, timestamp_s"),
        (
            "date_trunc(ts,week)",
            "second, minute, hour, day, month, year",
        ),
        ("hash()", "at least 1"),
        ("lower(symbol", "expected ',' or ')'"),
        ("lower(symbol) x", "unexpected text after"),
        ("lower('abc", "not closed"),
        ("lower(1.5)", "whole numbers"),
        ("lower(symbol;)", "unexpected ';'"),
        ("date_trunc('day','day')", "not a string literal"),
        ("lower(99999999999999999999)", "does not fit in i64"),
    ];
    for (text, want) in cases {
        let e = Expr::parse(text).unwrap_err().to_string();
        assert!(e.contains(want), "{text}: {e}");
    }
    let deep = format!("{}symbol{}", "lower(".repeat(40), ")".repeat(40));
    assert!(Expr::parse(&deep)
        .unwrap_err()
        .to_string()
        .contains("nest deeper"));
    let long = format!("lower('{}')", "x".repeat(5000));
    assert!(Expr::parse(&long)
        .unwrap_err()
        .to_string()
        .contains("longer than"));
}

#[test]
fn type_checks_refuse_with_the_types_involved() {
    let s = schema();
    let cases: &[(&str, &str)] = &[
        ("lower(n)", "lower takes a string, not Int64"),
        ("json_get(ts,'$.a')", "json_get takes a string"),
        (
            "date_trunc(symbol,day)",
            "date_trunc takes a timestamp or a date, not Utf8",
        ),
        ("cast(ts,utf8)", "a string or an integer"),
        ("cast(symbol,timestamp_us)", "an integer"),
        (
            "lower(nope)",
            "column \"nope\" is not in the dataset schema",
        ),
    ];
    for (text, want) in cases {
        let e = Typed::parse(text, &s).unwrap_err().to_string();
        assert!(e.contains(want), "{text}: {e}");
    }
    assert_eq!(
        Typed::parse("date_trunc(ts,day)", &s).unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        Typed::parse("hash(symbol,n,ts)", &s).unwrap().data_type(),
        &DataType::UInt64
    );
}

#[test]
fn spec_rules() {
    let s = expr::spec("sym", "JSON_GET(payload, '$.symbol')", Over::Exact).unwrap();
    let KeySpec::Expr {
        name,
        expr,
        over,
        conformance,
    } = &s
    else {
        panic!()
    };
    assert_eq!(name, "sym");
    assert_eq!(expr, "json_get(payload,'$.symbol')");
    assert_eq!(
        **over,
        KeySpec::Exact {
            columns: vec!["sym".into()],
            counts: false,
            bounds: None,
        }
    );
    assert_eq!(conformance.len(), 9, "json_get's vectors only");
    assert_eq!(s.name(), "expr:sym=json_get(payload,'$.symbol'):exact");
    assert_eq!(s.columns(), ["sym".to_string()]);
    assert_eq!(s.physical(), over.as_ref());

    let b = expr::spec("d", "date_trunc('day', ts)", Over::Bloom(0.01)).unwrap();
    assert_eq!(b.name(), "expr:d=date_trunc(ts,day):bloom");
    // date_trunc over a cast: both functions' vectors
    let c = expr::spec("d", "date_trunc(cast(n,timestamp_us),day)", Over::Exact).unwrap();
    let KeySpec::Expr { conformance, .. } = &c else {
        panic!()
    };
    assert_eq!(conformance.len(), 5 + 5);

    for (name, text, want) in [
        ("1x", "lower(symbol)", "identifier"),
        ("a b", "lower(symbol)", "identifier"),
        ("k", "symbol", "calls no function"),
        ("k", "lower('A')", "reads no column"),
    ] {
        let e = expr::spec(name, text, Over::Exact).unwrap_err().to_string();
        assert!(e.contains(want), "{name}={text}: {e}");
    }
    // stored un-normalized, or over a wrong inner spec
    let KeySpec::Expr {
        name,
        conformance,
        over,
        ..
    } = s.clone()
    else {
        panic!()
    };
    let raw = KeySpec::Expr {
        name: name.clone(),
        expr: "JSON_GET(payload,'$.symbol')".into(),
        over: over.clone(),
        conformance: conformance.clone(),
    };
    assert!(expr::check_spec(&raw)
        .unwrap_err()
        .to_string()
        .contains("normalized"));
    let wrong = KeySpec::Expr {
        name: name.clone(),
        expr: "json_get(payload,'$.symbol')".into(),
        over: Box::new(KeySpec::Exact {
            columns: vec!["other".into()],
            counts: false,
            bounds: None,
        }),
        conformance: conformance.clone(),
    };
    assert!(expr::check_spec(&wrong)
        .unwrap_err()
        .to_string()
        .contains("virtual column sym"));
    // a tampered conformance vector is refused, naming both values
    let mut bad = conformance;
    bad[0].value = Some("0653594d 320000".replace(' ', ""));
    let tampered = KeySpec::Expr {
        name,
        expr: "json_get(payload,'$.symbol')".into(),
        over,
        conformance: bad,
    };
    let e = expr::check_spec(&tampered).unwrap_err().to_string();
    assert!(
        e.contains("this binary computes 0653594d310000") && e.contains("rebuild the index"),
        "{e}"
    );
    // a name that is already a dataset column
    let dup = expr::spec("symbol", "lower(symbol)", Over::Exact).unwrap();
    let e = expr::typed_spec(&dup, &schema()).unwrap_err().to_string();
    assert!(
        e.contains("already a dataset column") && e.contains("symbol_key"),
        "{e}"
    );
}

fn utf8(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

#[test]
fn eval_derives_one_batch_and_nulls_stay_null() {
    let s = schema();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ])),
        vec![
            utf8(&[
                Some(r#"{"symbol":"SYM1"}"#),
                None,
                Some(r#"{"symbol":null}"#),
                Some("not json"),
                Some(r#"{"symbol":"a\u0000b"}"#),
            ]),
            Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(3),
                Some(-4),
                Some(5),
            ])),
        ],
    )
    .unwrap();
    let t = Typed::parse("json_get(payload,'$.symbol')", &s).unwrap();
    let out = t.eval(&batch).unwrap();
    let out = out.as_string::<i32>();
    assert_eq!(out.value(0), "SYM1");
    assert!(out.is_null(1) && out.is_null(2) && out.is_null(3));
    assert_eq!(out.value(4), "a\0b");
    let h = Typed::parse("hash(payload,n)", &s)
        .unwrap()
        .eval(&batch)
        .unwrap();
    let h = h.as_primitive::<UInt64Type>();
    assert!(!h.is_null(0) && h.is_null(1));
    // a missing input column is named
    let e = Typed::parse("lower(symbol)", &s)
        .unwrap()
        .eval(&batch)
        .unwrap_err()
        .to_string();
    assert!(e.contains("\"symbol\""), "{e}");
}

// --- oracles -----------------------------------------------------------------

/// What the generator put at `$.symbol`, and so what `json_get` must return.
#[derive(Clone, Debug)]
enum Planted {
    Missing,
    Null,
    Str(String),
    Num(&'static str),
    Bool(bool),
    Container,
    Dup(String, String),
}

fn planted() -> impl Strategy<Value = (Planted, bool, u8)> {
    let s = "[a-zA-Z0-9 \"\\\\/é\u{1F600}\u{0}\n]{0,6}";
    (
        prop_oneof![
            Just(Planted::Missing),
            Just(Planted::Null),
            s.prop_map(Planted::Str),
            prop::sample::select(vec![
                "0",
                "-0",
                "1.50",
                "2e10",
                "-1E-3",
                "123456789012345678901234"
            ])
            .prop_map(Planted::Num),
            any::<bool>().prop_map(Planted::Bool),
            Just(Planted::Container),
            (s, s).prop_map(|(a, b)| Planted::Dup(a, b)),
        ],
        any::<bool>(),
        0u8..4,
    )
}

/// The document, and the expected value: `invalid` breaks the document.
fn doc(p: &Planted, invalid: bool, pad: u8) -> (String, Option<String>) {
    let q = |s: &str| serde_json::to_string(s).unwrap();
    let padding = match pad {
        0 => String::new(),
        1 => r#""pad":[1,{"symbol":"nested"}],"#.to_string(),
        2 => r#" "x" : "symbol" , "#.to_string(),
        _ => r#""sym":"no","#.to_string(),
    };
    let (member, want) = match p {
        Planted::Missing => (String::new(), None),
        Planted::Null => (r#""symbol":null"#.into(), None),
        Planted::Str(s) => (format!(r#""symbol":{}"#, q(s)), Some(s.clone())),
        Planted::Num(n) => (format!(r#""symbol":{n}"#), Some((*n).to_string())),
        Planted::Bool(b) => (format!(r#""symbol":{b}"#), Some(b.to_string())),
        Planted::Container => (r#""symbol":{"a":"b"}"#.into(), None),
        Planted::Dup(a, b) => (
            format!(r#""symbol":{},"symbol":{}"#, q(a), q(b)),
            Some(a.clone()),
        ),
    };
    let body = if member.is_empty() {
        padding.trim_end_matches([',', ' ']).to_string()
    } else {
        format!("{padding}{member}")
    };
    let d = format!("{{{body}}}");
    if invalid {
        (format!("{d},"), None)
    } else {
        (d, want)
    }
}

proptest! {
    #[test]
    fn json_get_equals_the_planted_value((p, invalid, pad) in planted()) {
        let (d, want) = doc(&p, invalid, pad);
        let path = parse_path("$.symbol").unwrap();
        prop_assert_eq!(json_get(&d, &path), want.clone(), "{}", d);
        // serde_json agrees on validity and on strings
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&d);
        prop_assert_eq!(parsed.is_ok(), !invalid, "{}", d);
        if let (Ok(v), Planted::Str(s)) = (&parsed, &p) {
            prop_assert_eq!(v.get("symbol").and_then(|x| x.as_str()), Some(s.as_str()));
        }
    }

    #[test]
    fn json_get_never_panics_on_arbitrary_text(s in "\\PC{0,40}", path in "\\$(\\.[a-z]{1,3}|\\[[0-9]\\]){0,3}") {
        let p = parse_path(&path).unwrap();
        let _ = json_get(&s, &p);
        // and agrees with serde_json about whether the text is a document
        if json_get(&s, &[]).is_some() {
            prop_assert!(serde_json::from_str::<serde_json::Value>(&s).is_ok(), "{}", s);
        }
    }

    #[test]
    fn lower_and_substr_equal_their_oracles(s in "\\PC{0,12}", start in 1i64..15, len in 0i64..15) {
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![utf8(&[Some(&s)])]).unwrap();
        let l = Typed::parse("lower(s)", &schema).unwrap().eval(&batch).unwrap();
        // independent of `str::to_lowercase`: per-character default
        // lowercase; the final-sigma rule is pinned by the vectors, so
        // both sides spell every sigma the same before comparing
        let oracle: String = s.chars().flat_map(char::to_lowercase).collect::<String>().replace('ς', "σ");
        prop_assert_eq!(l.as_string::<i32>().value(0).replace('ς', "σ"), oracle);
        let text = format!("substr(s,{start},{len})");
        let u = Typed::parse(&text, &schema).unwrap().eval(&batch).unwrap();
        let chars: Vec<char> = s.chars().collect();
        let from = usize::try_from(start - 1).unwrap().min(chars.len());
        let to = (from + usize::try_from(len).unwrap()).min(chars.len());
        let oracle: String = chars[from..to].iter().collect();
        prop_assert_eq!(u.as_string::<i32>().value(0), oracle.as_str());
    }

    #[test]
    fn cast_to_int64_equals_its_oracle(s in "-?[0-9]{0,20}|[ +]?[0-9a]{1,4}") {
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![utf8(&[Some(&s)])]).unwrap();
        let c = Typed::parse("cast(s,int64)", &schema).unwrap().eval(&batch).unwrap();
        let c = c.as_primitive::<arrow::datatypes::Int64Type>();
        let digits = s.strip_prefix('-').unwrap_or(&s);
        let oracle = (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse::<i64>().ok())
            .flatten();
        prop_assert_eq!((!c.is_null(0)).then(|| c.value(0)), oracle);
    }

    #[test]
    fn date_trunc_equals_a_calendar_oracle(day in -80_000i64..80_000, intra in 0i64..86_400_000_000) {
        let t = day * 86_400_000_000 + intra;
        let schema = Schema::new(vec![Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![Some(t)]))],
        ).unwrap();
        let (y, m, _) = calendar(day);
        let first_of_month = days_of(y, m, 1);
        let first_of_year = days_of(y, 1, 1);
        for (unit, want) in [
            ("second", t - t.rem_euclid(1_000_000)),
            ("minute", t - t.rem_euclid(60_000_000)),
            ("hour", t - t.rem_euclid(3_600_000_000)),
            ("day", day * 86_400_000_000),
            ("month", first_of_month * 86_400_000_000),
            ("year", first_of_year * 86_400_000_000),
        ] {
            let out = Typed::parse(&format!("date_trunc(ts,{unit})"), &schema).unwrap().eval(&batch).unwrap();
            prop_assert_eq!(out.as_primitive::<TimestampMicrosecondType>().value(0), want, "{}", unit);
        }
    }

    #[test]
    fn hash_equals_xxh64_over_hand_encoded_strings(a in "[a-z\u{0}]{0,5}", b in "[A-Z]{0,5}") {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![utf8(&[Some(&a)]), utf8(&[Some(&b)])]).unwrap();
        let h = Typed::parse("hash(a,b)", &schema).unwrap().eval(&batch).unwrap();
        let enc = |s: &str| {
            let mut v = vec![0x06u8];
            for byte in s.bytes() {
                v.push(byte);
                if byte == 0 { v.push(0xFF); }
            }
            v.extend_from_slice(&[0, 0]);
            v
        };
        let mut bytes = enc(&a);
        bytes.extend(enc(&b));
        prop_assert_eq!(h.as_primitive::<UInt64Type>().value(0), xxhash_rust::xxh64::xxh64(&bytes, 0));
    }
}

/// Proleptic Gregorian date of days since 1970-01-01, by stepping months
/// (independent of the implementation's era arithmetic).
fn calendar(day: i64) -> (i64, i64, i64) {
    let (mut y, mut m, mut d) = (1970i64, 1i64, 1i64);
    let mut rest = day;
    let leap = |y: i64| (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let len = |y: i64, m: i64| match m {
        2 if leap(y) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    while rest > 0 {
        let left = len(y, m) - d + 1;
        if rest >= left {
            rest -= left;
            d = 1;
            m += 1;
            if m == 13 {
                m = 1;
                y += 1;
            }
        } else {
            d += rest;
            rest = 0;
        }
    }
    while rest < 0 {
        if -rest >= d {
            rest += d;
            m -= 1;
            if m == 0 {
                m = 12;
                y -= 1;
            }
            d = len(y, m);
        } else {
            d += rest;
            rest = 0;
        }
    }
    (y, m, d)
}

fn days_of(y: i64, m: i64, d: i64) -> i64 {
    // search near an estimate with the stepping calendar
    let mut guess = (y - 1970) * 365 + (y - 1970) / 4 + (m - 1) * 30;
    loop {
        let (gy, gm, gd) = calendar(guess);
        match (gy, gm, gd).cmp(&(y, m, d)) {
            std::cmp::Ordering::Equal => return guess,
            std::cmp::Ordering::Less => guess += 1,
            std::cmp::Ordering::Greater => guess -= 1,
        }
    }
}

#[test]
fn verify_vectors_refuses_a_closed_vector_that_reads_a_column() {
    let v = vec![bellwether_types::ExprVector {
        expr: "lower(symbol)".into(),
        value: None,
    }];
    let e = expr::verify_vectors("expr:k=lower(symbol):exact", &v).unwrap_err();
    assert!(matches!(e, Error::KeySpec(_)), "{e}");
}

/// A document nested past `MAX_JSON_DEPTH` is not a document (null), and a
/// 100,000-deep one returns null without exhausting the stack.
#[test]
fn json_nesting_past_the_depth_limit_is_null_and_never_overflows() {
    use bellwether_core::expr::{json_get, parse_path, MAX_JSON_DEPTH};
    let at = |depth: usize| {
        format!(
            "{}{{\"symbol\":\"S\"}}{}",
            "[".repeat(depth),
            "]".repeat(depth)
        )
    };
    let path = |depth: usize| parse_path(&format!("${}.symbol", "[0]".repeat(depth))).unwrap();
    let ok = MAX_JSON_DEPTH - 2;
    assert_eq!(json_get(&at(ok), &path(ok)).as_deref(), Some("S"));
    let deep = MAX_JSON_DEPTH + 1;
    assert_eq!(json_get(&at(deep), &path(deep)), None);
    let root = parse_path("$").unwrap();
    let huge = format!("{}1{}", "[".repeat(100_000), "]".repeat(100_000));
    assert_eq!(json_get(&huge, &root), None);
    let huge_obj = format!("{}1{}", "{\"a\":".repeat(100_000), "}".repeat(100_000));
    assert_eq!(json_get(&huge_obj, &parse_path("$.a").unwrap()), None);
}

/// A spec missing any vector this binary pins for a function it calls is
/// refused: none at all, or only another function's.
#[test]
fn a_spec_missing_pinned_vectors_is_refused() {
    let full = expr::spec("sym", "lower(json_get(payload,'$.symbol'))", Over::Exact).unwrap();
    let KeySpec::Expr {
        name,
        expr: text,
        over,
        conformance,
    } = full.clone()
    else {
        unreachable!()
    };
    let with = |c: Vec<bellwether_types::ExprVector>| KeySpec::Expr {
        name: name.clone(),
        expr: text.clone(),
        over: over.clone(),
        conformance: c,
    };
    let lower_only: Vec<_> = conformance
        .iter()
        .filter(|v| v.expr.starts_with("lower"))
        .cloned()
        .collect();
    let one_short: Vec<_> = conformance[1..].to_vec();
    for c in [Vec::new(), lower_only, one_short] {
        let e = expr::check_spec(&with(c)).unwrap_err().to_string();
        assert!(e.contains("does not record the conformance vector"), "{e}");
    }
    expr::check_spec(&full).unwrap();
}
