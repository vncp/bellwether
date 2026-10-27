//! Expression keys (`expr`): a closed, deterministic expression language
//! whose value is indexed as a virtual column.
//!
//! An `expr` key spec ([`KeySpec::Expr`]) names a virtual column, an
//! expression over dataset columns, and the ordinary exact or bloom segment
//! that indexes the expression's value. There is no user code: the language
//! has six functions, string and integer literals, and column references.
//!
//! | Function | Input | Value |
//! |---|---|---|
//! | `json_get(s, '$.a.b[0]')` | a string column or expression | the JSON value at the path, as `Utf8`: a string's text, a number's literal token as written, `true` or `false`; null for a JSON null, an object, an array, a missing path, or a document that is not valid JSON (RFC 8259). A key that repeats takes its first occurrence. |
//! | `lower(s)` | string | the Unicode lowercase mapping of the string, as Rust's `str::to_lowercase` computes it: each character's default lowercase (`À` gives `à`, `İ` gives `i` followed by U+0307), with the final-sigma rule (`ΣΑΣ` gives `σας`) |
//! | `substr(s, start, len)` | string, `start >= 1`, `len >= 0` | `len` characters (Unicode scalar values) from the 1-based `start`; empty when `start` is past the end |
//! | `cast(x, int64)` | string, integer, timestamp, date | a string of an optional `-` and ASCII digits that fits `i64` (else null); an integer that fits `i64`; a timestamp's raw value in its unit; a date's days (`Date32`) or milliseconds (`Date64`) |
//! | `cast(x, utf8)` | string, integer | the string itself, or the integer in decimal |
//! | `cast(x, timestamp_s\|ms\|us\|ns)` | integer | the integer as a UTC instant in that unit (an unsigned value above `i64::MAX` is null) |
//! | `date_trunc(t, unit)` | timestamp, date | the instant floored in UTC to `second`, `minute`, `hour`, `day`, `month`, or `year`, in the input's own type (a date takes `day`, `month`, or `year`) |
//! | `hash(x, ...)` | one or more values of any type with a canonical encoding | `UInt64`: xxhash64 (seed 0) over the concatenated canonical encodings (ADR 016) of the arguments, each at its own type; null if any argument is null |
//!
//! A null input gives a null value, and a null value is never indexed
//! (ADR 014). `lower` follows the Unicode tables of the Rust standard
//! library that built the binary; the conformance vectors pin its ASCII,
//! Latin-1, multi-character, and final-sigma behaviour, so a reader whose
//! tables differ on any of them refuses the key rather than miss rows
//! (invariant 1). A character whose mapping a later Unicode version adds
//! is outside what the vectors can see. A time zone
//! declared on a timestamp column does not move truncation, which is in
//! UTC.
//!
//! # Normalized form
//!
//! Two spellings of one expression normalize to the same text, and a lookup
//! matches an `expr` key only when its expression normalizes to the key's
//! (an expression that differs in any way, by one character of a path or one
//! function, is a different expression and is refused, never guessed):
//! function names and keywords lowercase; no whitespace; columns bare when
//! they are an identifier (`[A-Za-z_][A-Za-z0-9_]*`), else double-quoted;
//! strings single-quoted with `'` doubled; integers in decimal; JSON paths
//! `$`, then `.key` for an identifier key, `["key"]` for any other (with `"`
//! and `\` escaped by `\`), and `[n]` for an array index. `date_trunc` also
//! reads the `date_trunc('day', ts)` order and normalizes it to
//! `date_trunc(ts,day)`.
//!
//! # Conformance vectors
//!
//! Every function has pinned vectors ([`VECTORS`]): closed expressions over
//! literals and the canonical encoding of their values. An `expr` key spec
//! carries the vectors of every function its expression calls
//! ([`vectors_for`]), so the manifest that declares it tells any reader what
//! the builder computed. [`verify_vectors`] recomputes them, and every path
//! that builds, plans with, or prices an `expr` key refuses one whose
//! vectors this binary does not reproduce.
//!
//! # Memory (ADR 018)
//!
//! [`Typed::eval`] derives one batch's values at a time, one row at a time:
//! the derived column of one row group, never more. JSON is scanned without
//! building a document tree, and nesting deeper than [`MAX_JSON_DEPTH`]
//! counts as invalid JSON (null), so no input can exhaust the stack.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Builder, Date64Builder, Int64Builder, PrimitiveBuilder, RecordBatch,
    StringBuilder, UInt64Builder,
};
use arrow::datatypes::{
    DataType, Field, Schema, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use bellwether_types::{Error, ExprVector, KeySpec, Result};
use xxhash_rust::xxh64::xxh64;

use crate::encoding::{self, Value};

/// Nesting depth past which a JSON document counts as invalid (its
/// `json_get` value is null).
pub const MAX_JSON_DEPTH: usize = 128;

/// Longest expression text accepted, in bytes.
pub const MAX_EXPR_LEN: usize = 4096;

/// The functions of the language, in the order their vectors are listed.
pub const FUNCTIONS: [&str; 6] = ["json_get", "lower", "substr", "cast", "date_trunc", "hash"];

fn err(text: &str, why: impl fmt::Display) -> Error {
    Error::KeySpec(format!("expression {text:?} is not valid: {why}"))
}

// ---------------------------------------------------------------------------
// Syntax
// ---------------------------------------------------------------------------

/// A JSON path step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// An object member by key.
    Key(String),
    /// An array element by 0-based index.
    Index(usize),
}

/// A `cast` target type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CastType {
    /// `int64`.
    Int64,
    /// `utf8`.
    Utf8,
    /// `timestamp_s`, `timestamp_ms`, `timestamp_us`, `timestamp_ns`.
    Timestamp(TimeUnit),
}

impl CastType {
    fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "int64" => Self::Int64,
            "utf8" => Self::Utf8,
            "timestamp_s" => Self::Timestamp(TimeUnit::Second),
            "timestamp_ms" => Self::Timestamp(TimeUnit::Millisecond),
            "timestamp_us" => Self::Timestamp(TimeUnit::Microsecond),
            "timestamp_ns" => Self::Timestamp(TimeUnit::Nanosecond),
            _ => return None,
        })
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Int64 => "int64",
            Self::Utf8 => "utf8",
            Self::Timestamp(TimeUnit::Second) => "timestamp_s",
            Self::Timestamp(TimeUnit::Millisecond) => "timestamp_ms",
            Self::Timestamp(TimeUnit::Microsecond) => "timestamp_us",
            Self::Timestamp(TimeUnit::Nanosecond) => "timestamp_ns",
        }
    }
}

/// A `date_trunc` unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TruncUnit {
    /// `second`.
    Second,
    /// `minute`.
    Minute,
    /// `hour`.
    Hour,
    /// `day`.
    Day,
    /// `month`.
    Month,
    /// `year`.
    Year,
}

impl TruncUnit {
    fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "second" => Self::Second,
            "minute" => Self::Minute,
            "hour" => Self::Hour,
            "day" => Self::Day,
            "month" => Self::Month,
            "year" => Self::Year,
            _ => return None,
        })
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
            Self::Year => "year",
        }
    }

    /// Seconds in a fixed-width unit; `None` for month and year.
    fn seconds(self) -> Option<i64> {
        match self {
            Self::Second => Some(1),
            Self::Minute => Some(60),
            Self::Hour => Some(3600),
            Self::Day => Some(86_400),
            Self::Month | Self::Year => None,
        }
    }
}

/// A parsed expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expr {
    /// A dataset column.
    Column(String),
    /// A string literal.
    Str(String),
    /// An integer literal.
    Int(i64),
    /// `json_get(input, path)`.
    JsonGet {
        /// The JSON text.
        input: Box<Expr>,
        /// The path, `$` then steps.
        path: Vec<Step>,
    },
    /// `lower(input)`.
    Lower(Box<Expr>),
    /// `substr(input, start, len)`.
    Substr {
        /// The string.
        input: Box<Expr>,
        /// 1-based first character.
        start: i64,
        /// Characters taken.
        len: i64,
    },
    /// `cast(input, to)`.
    Cast {
        /// The value.
        input: Box<Expr>,
        /// The target type.
        to: CastType,
    },
    /// `date_trunc(input, unit)`.
    DateTrunc {
        /// The timestamp or date.
        input: Box<Expr>,
        /// The unit.
        unit: TruncUnit,
    },
    /// `hash(args...)`.
    Hash(Vec<Expr>),
}

fn is_ident(s: &str) -> bool {
    let mut c = s.chars();
    c.next()
        .is_some_and(|f| f.is_ascii_alphabetic() || f == '_')
        && c.all(|x| x.is_ascii_alphanumeric() || x == '_')
}

fn write_str_literal(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
    write!(f, "'{}'", s.replace('\'', "''"))
}

/// The normalized text of a JSON path.
#[must_use]
pub fn path_text(path: &[Step]) -> String {
    let mut out = String::from("$");
    for s in path {
        match s {
            Step::Key(k) if is_ident(k) => {
                out.push('.');
                out.push_str(k);
            }
            Step::Key(k) => {
                out.push_str("[\"");
                for ch in k.chars() {
                    if ch == '"' || ch == '\\' {
                        out.push('\\');
                    }
                    out.push(ch);
                }
                out.push_str("\"]");
            }
            Step::Index(i) => {
                out.push('[');
                out.push_str(&i.to_string());
                out.push(']');
            }
        }
    }
    out
}

impl fmt::Display for Expr {
    /// The normalized form (module docs).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column(c) if is_ident(c) => f.write_str(c),
            Self::Column(c) => write!(f, "\"{}\"", c.replace('"', "\"\"")),
            Self::Str(s) => write_str_literal(f, s),
            Self::Int(i) => write!(f, "{i}"),
            Self::JsonGet { input, path } => {
                write!(f, "json_get({input},")?;
                write_str_literal(f, &path_text(path))?;
                f.write_str(")")
            }
            Self::Lower(input) => write!(f, "lower({input})"),
            Self::Substr { input, start, len } => write!(f, "substr({input},{start},{len})"),
            Self::Cast { input, to } => write!(f, "cast({input},{})", to.as_str()),
            Self::DateTrunc { input, unit } => write!(f, "date_trunc({input},{})", unit.as_str()),
            Self::Hash(args) => {
                f.write_str("hash(")?;
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{a}")?;
                }
                f.write_str(")")
            }
        }
    }
}

impl Expr {
    /// Parses expression text (module docs). Never reads data.
    pub fn parse(text: &str) -> Result<Self> {
        if text.len() > MAX_EXPR_LEN {
            let head: String = text.chars().take(64).collect();
            return Err(err(&head, format!("longer than {MAX_EXPR_LEN} bytes")));
        }
        let tokens = lex(text)?;
        let mut p = Parser {
            text,
            tokens: &tokens,
            pos: 0,
            depth: 0,
        };
        let e = p.expr()?;
        if p.pos != tokens.len() {
            return Err(err(text, "unexpected text after the expression"));
        }
        Ok(e)
    }

    /// The dataset columns the expression reads, sorted.
    #[must_use]
    pub fn columns(&self) -> Vec<String> {
        let mut out = BTreeSet::new();
        self.walk(&mut |e| {
            if let Self::Column(c) = e {
                out.insert(c.clone());
            }
        });
        out.into_iter().collect()
    }

    /// The functions the expression calls, in [`FUNCTIONS`] order.
    #[must_use]
    pub fn functions(&self) -> Vec<&'static str> {
        let mut used = BTreeSet::new();
        self.walk(&mut |e| {
            let f = match e {
                Self::JsonGet { .. } => 0,
                Self::Lower(_) => 1,
                Self::Substr { .. } => 2,
                Self::Cast { .. } => 3,
                Self::DateTrunc { .. } => 4,
                Self::Hash(_) => 5,
                _ => return,
            };
            used.insert(f);
        });
        used.into_iter().map(|i| FUNCTIONS[i]).collect()
    }

    /// Whether the expression calls a function (a bare column or literal is
    /// not an expression key).
    #[must_use]
    pub fn is_call(&self) -> bool {
        !matches!(self, Self::Column(_) | Self::Str(_) | Self::Int(_))
    }

    fn walk(&self, f: &mut impl FnMut(&Self)) {
        f(self);
        match self {
            Self::Column(_) | Self::Str(_) | Self::Int(_) => {}
            Self::JsonGet { input, .. }
            | Self::Lower(input)
            | Self::Substr { input, .. }
            | Self::Cast { input, .. }
            | Self::DateTrunc { input, .. } => input.walk(f),
            Self::Hash(args) => {
                for a in args {
                    a.walk(f);
                }
            }
        }
    }
}

/// The normalized text of `text` (module docs).
pub fn normalize(text: &str) -> Result<String> {
    Ok(Expr::parse(text)?.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Quoted(String),
    Str(String),
    Int(i64),
    LParen,
    RParen,
    Comma,
}

// One pass over the characters, one arm per token kind.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
fn lex(text: &str) -> Result<Vec<Tok>> {
    let mut out = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            ',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            '\'' | '"' => {
                let q = c;
                let mut s = String::new();
                i += 1;
                loop {
                    match chars.get(i) {
                        None => {
                            return Err(err(
                                text,
                                format!("a {q}-quoted text is not closed; close it with {q}"),
                            ))
                        }
                        Some(&x) if x == q => {
                            if chars.get(i + 1) == Some(&q) {
                                s.push(q);
                                i += 2;
                            } else {
                                i += 1;
                                break;
                            }
                        }
                        Some(&x) => {
                            s.push(x);
                            i += 1;
                        }
                    }
                }
                out.push(if q == '\'' {
                    Tok::Str(s)
                } else {
                    Tok::Quoted(s)
                });
            }
            '-' | '0'..='9' => {
                let start = i;
                if c == '-' {
                    i += 1;
                }
                let digits = i;
                while chars.get(i).is_some_and(char::is_ascii_digit) {
                    i += 1;
                }
                let lit: String = chars[start..i].iter().collect();
                if i == digits {
                    return Err(err(text, "'-' must start an integer"));
                }
                if chars
                    .get(i)
                    .is_some_and(|x| x.is_ascii_alphanumeric() || *x == '_' || *x == '.')
                {
                    return Err(err(
                        text,
                        format!("{lit:?} is followed by {:?}; integers are whole numbers, and a column name starts with a letter or _", chars[i]),
                    ));
                }
                let v: i64 = lit
                    .parse()
                    .map_err(|_| err(text, format!("integer {lit} does not fit in i64")))?;
                out.push(Tok::Int(v));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while chars
                    .get(i)
                    .is_some_and(|x| x.is_ascii_alphanumeric() || *x == '_')
                {
                    i += 1;
                }
                out.push(Tok::Ident(chars[start..i].iter().collect()));
            }
            other => {
                return Err(err(
                    text,
                    format!(
                        "unexpected {other:?}; an expression is a function call such as \
                         json_get(payload,'$.symbol'), a column, a 'string', or an integer"
                    ),
                ))
            }
        }
    }
    Ok(out)
}

/// Deepest call nesting the parser accepts.
const MAX_CALL_DEPTH: usize = 32;

struct Parser<'a> {
    text: &'a str,
    tokens: &'a [Tok],
    pos: usize,
    depth: usize,
}

impl Parser<'_> {
    fn next(&mut self) -> Option<&Tok> {
        let t = self.tokens.get(self.pos);
        self.pos += 1;
        t
    }

    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos)
    }

    fn expr(&mut self) -> Result<Expr> {
        match self.next().cloned() {
            Some(Tok::Str(s)) => Ok(Expr::Str(s)),
            Some(Tok::Int(i)) => Ok(Expr::Int(i)),
            Some(Tok::Quoted(c)) => {
                if c.is_empty() {
                    return Err(err(self.text, "a quoted column name is empty"));
                }
                Ok(Expr::Column(c))
            }
            Some(Tok::Ident(name)) => {
                if self.peek() == Some(&Tok::LParen) {
                    self.pos += 1;
                    self.depth += 1;
                    if self.depth > MAX_CALL_DEPTH {
                        return Err(err(
                            self.text,
                            format!("calls nest deeper than {MAX_CALL_DEPTH}"),
                        ));
                    }
                    let e = self.call(&name)?;
                    self.depth -= 1;
                    Ok(e)
                } else {
                    Ok(Expr::Column(name))
                }
            }
            _ => Err(err(
                self.text,
                "expected a function call, a column, a 'string', or an integer",
            )),
        }
    }

    /// Arguments of a call until `)`, each an expression; keyword positions
    /// are read by the caller from `Expr::Column`.
    fn args(&mut self) -> Result<Vec<Expr>> {
        let mut args = Vec::new();
        if self.peek() == Some(&Tok::RParen) {
            self.pos += 1;
            return Ok(args);
        }
        loop {
            args.push(self.expr()?);
            match self.next() {
                Some(Tok::Comma) => {}
                Some(Tok::RParen) => return Ok(args),
                _ => return Err(err(self.text, "expected ',' or ')' after an argument")),
            }
        }
    }

    // One arm per function of the closed language.
    #[allow(clippy::too_many_lines)]
    fn call(&mut self, name: &str) -> Result<Expr> {
        let lname = name.to_ascii_lowercase();
        let text = self.text;
        let arity = |args: &[Expr], want: &str, ok: bool| -> Result<()> {
            if ok {
                Ok(())
            } else {
                Err(err(
                    text,
                    format!("{lname} takes {want}, not {} arguments", args.len()),
                ))
            }
        };
        let int_lit = |e: &Expr, what: &str| -> Result<i64> {
            match e {
                Expr::Int(i) => Ok(*i),
                _ => Err(err(
                    text,
                    format!("{what} of {lname} must be an integer literal"),
                )),
            }
        };
        let keyword = |e: &Expr| -> Option<String> {
            match e {
                Expr::Column(c) | Expr::Str(c) => Some(c.clone()),
                _ => None,
            }
        };
        match lname.as_str() {
            "json_get" => {
                let args = self.args()?;
                arity(
                    &args,
                    "2 (a string and a path such as '$.symbol')",
                    args.len() == 2,
                )?;
                let mut it = args.into_iter();
                let input = it
                    .next()
                    .ok_or_else(|| err(text, "json_get needs an input"))?;
                let Some(Expr::Str(p)) = it.next() else {
                    return Err(err(
                        text,
                        "the path of json_get must be a string literal such as '$.symbol'",
                    ));
                };
                let path = parse_path(&p).map_err(|why| err(text, why))?;
                Ok(Expr::JsonGet {
                    input: Box::new(input),
                    path,
                })
            }
            "lower" => {
                let args = self.args()?;
                arity(&args, "1", args.len() == 1)?;
                let input = args
                    .into_iter()
                    .next()
                    .ok_or_else(|| err(text, "lower needs an input"))?;
                Ok(Expr::Lower(Box::new(input)))
            }
            "substr" => {
                let args = self.args()?;
                arity(
                    &args,
                    "3 (a string, a 1-based start, a length)",
                    args.len() == 3,
                )?;
                let start = int_lit(&args[1], "the start")?;
                let len = int_lit(&args[2], "the length")?;
                if start < 1 {
                    return Err(err(text, "the start of substr is 1-based, so at least 1"));
                }
                if len < 0 {
                    return Err(err(text, "the length of substr cannot be negative"));
                }
                let input = args
                    .into_iter()
                    .next()
                    .ok_or_else(|| err(text, "substr needs an input"))?;
                Ok(Expr::Substr {
                    input: Box::new(input),
                    start,
                    len,
                })
            }
            "cast" => {
                let args = self.args()?;
                arity(
                    &args,
                    "2 (a value and int64, utf8, or timestamp_s|ms|us|ns)",
                    args.len() == 2,
                )?;
                let to = keyword(&args[1])
                    .and_then(|k| CastType::parse(&k))
                    .ok_or_else(|| {
                        err(
                            text,
                            "the target of cast is one of int64, utf8, timestamp_s, \
                             timestamp_ms, timestamp_us, timestamp_ns",
                        )
                    })?;
                let input = args
                    .into_iter()
                    .next()
                    .ok_or_else(|| err(text, "cast needs an input"))?;
                Ok(Expr::Cast {
                    input: Box::new(input),
                    to,
                })
            }
            "date_trunc" => {
                let args = self.args()?;
                arity(&args, "2 (a timestamp and a unit)", args.len() == 2)?;
                let mut it = args.into_iter();
                let (Some(a), Some(b)) = (it.next(), it.next()) else {
                    return Err(err(text, "date_trunc takes 2 arguments"));
                };
                // date_trunc(ts, day), date_trunc(ts, 'day'), or the SQL
                // order date_trunc('day', ts).
                let (input, unit) =
                    match (&a, keyword(&b).and_then(|k| TruncUnit::parse(&k))) {
                        (_, Some(u)) if !matches!(a, Expr::Str(_)) => (a, u),
                        (Expr::Str(s), _) => {
                            match TruncUnit::parse(s) {
                                Some(u) => (b, u),
                                None => return Err(err(
                                    text,
                                    "the unit of date_trunc is one of second, minute, hour, day, \
                                 month, year",
                                )),
                            }
                        }
                        _ => return Err(err(
                            text,
                            "the unit of date_trunc is one of second, minute, hour, day, month, \
                             year",
                        )),
                    };
                if matches!(input, Expr::Str(_)) {
                    return Err(err(
                        text,
                        "date_trunc truncates a timestamp or a date, not a string literal",
                    ));
                }
                Ok(Expr::DateTrunc {
                    input: Box::new(input),
                    unit,
                })
            }
            "hash" => {
                let args = self.args()?;
                arity(&args, "at least 1", !args.is_empty())?;
                Ok(Expr::Hash(args))
            }
            _ => Err(err(
                self.text,
                format!(
                    "unknown function {name:?}; the language has exactly json_get, lower, substr, \
                     cast, date_trunc, and hash"
                ),
            )),
        }
    }
}

/// Parses a JSON path: `$`, then `.key`, `['key']`, `["key"]`, or `[n]`.
pub fn parse_path(p: &str) -> std::result::Result<Vec<Step>, String> {
    let bad =
        |why: &str| format!("JSON path {p:?}: {why}; write it as $.a.b, $[\"a b\"], or $.a[0]");
    let chars: Vec<char> = p.chars().collect();
    if chars.first() != Some(&'$') {
        return Err(bad("it must start with $"));
    }
    let mut i = 1usize;
    let mut out = Vec::new();
    while i < chars.len() {
        match chars[i] {
            '.' => {
                i += 1;
                let start = i;
                while chars
                    .get(i)
                    .is_some_and(|x| x.is_ascii_alphanumeric() || *x == '_')
                {
                    i += 1;
                }
                let key: String = chars[start..i].iter().collect();
                if !is_ident(&key) {
                    return Err(bad(
                        "a key after '.' is a name such as symbol; quote others",
                    ));
                }
                out.push(Step::Key(key));
            }
            '[' => {
                i += 1;
                match chars.get(i) {
                    Some(&q @ ('\'' | '"')) => {
                        i += 1;
                        let mut key = String::new();
                        loop {
                            match chars.get(i) {
                                None => return Err(bad("a quoted key is not closed")),
                                Some('\\') => {
                                    match chars.get(i + 1) {
                                        Some(&e @ ('\\' | '"' | '\'')) => key.push(e),
                                        _ => return Err(bad("only \\\\, \\\", and \\' escapes are allowed in a quoted key")),
                                    }
                                    i += 2;
                                }
                                Some(&x) if x == q => {
                                    i += 1;
                                    break;
                                }
                                Some(&x) => {
                                    key.push(x);
                                    i += 1;
                                }
                            }
                        }
                        out.push(Step::Key(key));
                    }
                    Some(c) if c.is_ascii_digit() => {
                        let start = i;
                        while chars.get(i).is_some_and(char::is_ascii_digit) {
                            i += 1;
                        }
                        let n: String = chars[start..i].iter().collect();
                        if n.len() > 1 && n.starts_with('0') {
                            return Err(bad("an index has no leading zeros"));
                        }
                        let n: usize = n.parse().map_err(|_| bad("an index is too large"))?;
                        out.push(Step::Index(n));
                    }
                    _ => return Err(bad("'[' takes a quoted key or an index")),
                }
                if chars.get(i) != Some(&']') {
                    return Err(bad("a '[' step is not closed with ']'"));
                }
                i += 1;
            }
            _ => return Err(bad("a step starts with '.' or '['")),
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// An expression checked against a dataset schema.
#[derive(Clone, Debug)]
pub struct Typed {
    expr: Expr,
    out: DataType,
    schema: Arc<Schema>,
}

fn unwrap_dict(dt: &DataType) -> &DataType {
    match dt {
        DataType::Dictionary(_, v) => unwrap_dict(v),
        other => other,
    }
}

fn is_string(dt: &DataType) -> bool {
    matches!(
        unwrap_dict(dt),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_signed(dt: &DataType) -> bool {
    matches!(
        unwrap_dict(dt),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn is_unsigned(dt: &DataType) -> bool {
    matches!(
        unwrap_dict(dt),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
    )
}

fn is_int(dt: &DataType) -> bool {
    is_signed(dt) || is_unsigned(dt)
}

// One arm per function of the closed language.
#[allow(clippy::too_many_lines)]
fn type_of(e: &Expr, schema: &Schema, text: &str) -> Result<DataType> {
    let need = |dt: &DataType, ok: bool, f: &str, want: &str| -> Result<()> {
        if ok {
            Ok(())
        } else {
            Err(Error::KeySpec(format!(
                "expression {text:?}: {f} takes {want}, not {dt}"
            )))
        }
    };
    Ok(match e {
        Expr::Column(c) => {
            let dt = schema
                .field_with_name(c)
                .map_err(|_| {
                    Error::KeySpec(format!(
                        "expression {text:?}: {}",
                        encoding::not_in_schema(schema, c)
                    ))
                })?
                .data_type()
                .clone();
            encoding::check_supported(&dt)
                .map_err(|e| Error::KeySpec(format!("expression {text:?}: {e}")))?;
            unwrap_dict(&dt).clone()
        }
        Expr::Str(_) => DataType::Utf8,
        Expr::Int(_) => DataType::Int64,
        Expr::JsonGet { input, .. } => {
            let t = type_of(input, schema, text)?;
            need(&t, is_string(&t), "json_get", "a string")?;
            DataType::Utf8
        }
        Expr::Lower(input) => {
            let t = type_of(input, schema, text)?;
            need(&t, is_string(&t), "lower", "a string")?;
            DataType::Utf8
        }
        Expr::Substr { input, .. } => {
            let t = type_of(input, schema, text)?;
            need(&t, is_string(&t), "substr", "a string")?;
            DataType::Utf8
        }
        Expr::Cast { input, to } => {
            let t = type_of(input, schema, text)?;
            match to {
                CastType::Int64 => {
                    need(
                        &t,
                        is_string(&t)
                            || is_int(&t)
                            || matches!(
                                t,
                                DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
                            ),
                        "cast(_, int64)",
                        "a string, an integer, a timestamp, or a date",
                    )?;
                    DataType::Int64
                }
                CastType::Utf8 => {
                    need(
                        &t,
                        is_string(&t) || is_int(&t),
                        "cast(_, utf8)",
                        "a string or an integer",
                    )?;
                    DataType::Utf8
                }
                CastType::Timestamp(u) => {
                    need(&t, is_int(&t), "cast(_, timestamp_*)", "an integer")?;
                    DataType::Timestamp(*u, None)
                }
            }
        }
        Expr::DateTrunc { input, unit } => {
            let t = type_of(input, schema, text)?;
            match &t {
                DataType::Timestamp(..) => t,
                DataType::Date32 | DataType::Date64 => {
                    if *unit < TruncUnit::Day {
                        return Err(Error::KeySpec(format!(
                            "expression {text:?}: date_trunc of a date takes day, month, or \
                             year, not {}",
                            unit.as_str()
                        )));
                    }
                    t
                }
                other => {
                    return Err(Error::KeySpec(format!(
                        "expression {text:?}: date_trunc takes a timestamp or a date, not {other}"
                    )))
                }
            }
        }
        Expr::Hash(args) => {
            for a in args {
                type_of(a, schema, text)?;
            }
            DataType::UInt64
        }
    })
}

impl Typed {
    /// Checks `expr` against `schema`: every column exists with a canonical
    /// encoding, and every function gets the types it takes.
    pub fn new(expr: Expr, schema: &Schema) -> Result<Self> {
        let text = expr.to_string();
        let out = type_of(&expr, schema, &text)?;
        // Keep only the columns read, so evaluation needs only them.
        let fields: Vec<Field> = expr
            .columns()
            .iter()
            .filter_map(|c| schema.field_with_name(c).ok().cloned())
            .collect();
        Ok(Self {
            expr,
            out,
            schema: Arc::new(Schema::new(fields)),
        })
    }

    /// Parses `text` and checks it against `schema`.
    pub fn parse(text: &str, schema: &Schema) -> Result<Self> {
        Self::new(Expr::parse(text)?, schema)
    }

    /// The expression.
    #[must_use]
    pub fn expr(&self) -> &Expr {
        &self.expr
    }

    /// The value's type.
    #[must_use]
    pub fn data_type(&self) -> &DataType {
        &self.out
    }

    /// The dataset columns the expression reads, sorted.
    #[must_use]
    pub fn input_columns(&self) -> Vec<String> {
        self.expr.columns()
    }

    /// The expression's value for every row of `batch`, which must hold
    /// every input column (at a type that converts to the dataset's): one
    /// array of [`Typed::data_type`], null where the value is null.
    pub fn eval(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let names = self.input_columns();
        let arrays = names
            .iter()
            .map(|c| {
                batch.column_by_name(c).cloned().ok_or_else(|| {
                    Error::KeySpec(format!(
                        "expression {} reads column {c:?}, which the batch does not hold",
                        self.expr
                    ))
                })
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        let cols: Vec<(&str, &dyn Array)> = names
            .iter()
            .zip(&arrays)
            .map(|(n, a)| (n.as_str(), a.as_ref()))
            .collect();
        let ctx = self.ctx(&cols)?;
        let rows = batch.num_rows();
        let mut b = OutBuilder::new(&self.out, rows);
        for row in 0..rows {
            b.push(ctx.eval(&self.expr, row)?)?;
        }
        Ok(b.finish())
    }

    /// The canonical encoding of the value of every row of `batch` (`None`
    /// where it is null), for a residual that recomputes the expression.
    pub fn encode_rows(&self, batch: &RecordBatch) -> Result<Vec<Option<Vec<u8>>>> {
        let arr = self.eval(batch)?;
        (0..arr.len())
            .map(|row| {
                let mut out = Vec::new();
                Ok(
                    encoding::encode_array_value_as(arr.as_ref(), row, &self.out, &mut out)?
                        .then_some(out),
                )
            })
            .collect()
    }

    /// The canonical encoding of the value at `row` (`None` when null),
    /// reading each input column from `cols` by name: one row at a time, for
    /// a consumer that applies an `ExprIn` residual row by row.
    pub fn encode_row(&self, cols: &[(&str, &dyn Array)], row: usize) -> Result<Option<Vec<u8>>> {
        let ctx = self.ctx(cols)?;
        let Some(v) = ctx.eval(&self.expr, row)? else {
            return Ok(None);
        };
        let mut out = Vec::new();
        match (&v, &self.out) {
            // A timestamp keeps its column's zone in the output type; the
            // encoding does not depend on it.
            (Val::Ts(t, u), dt @ DataType::Timestamp(..)) => encoding::encode_value_as(
                &Value::Timestamp {
                    value: *t,
                    unit: *u,
                },
                dt,
                &mut out,
            )?,
            _ => encode_val(&v, &mut out)?,
        }
        Ok(Some(out))
    }

    /// The evaluation context over `cols`: every input column present, at a
    /// type that converts to the dataset's.
    fn ctx<'a>(&self, cols: &[(&str, &'a dyn Array)]) -> Result<Ctx<'a>> {
        let mut out = Vec::new();
        for c in self.input_columns() {
            let a = cols
                .iter()
                .find(|(n, _)| *n == c)
                .map(|(_, a)| *a)
                .ok_or_else(|| {
                    Error::KeySpec(format!(
                        "expression {} reads column {c:?}, which the batch does not hold",
                        self.expr
                    ))
                })?;
            let dt = self
                .schema
                .field_with_name(&c)
                .map_err(|_| Error::KeySpec(format!("column {c:?} not in schema")))?
                .data_type()
                .clone();
            encoding::check_convertible(a.data_type(), &dt)?;
            out.push((c, a, dt));
        }
        Ok(Ctx { cols: out })
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// One row's intermediate value.
#[derive(Clone, Debug, PartialEq)]
enum Val {
    Str(String),
    Int(i64),
    UInt(u64),
    Ts(i64, TimeUnit),
    Date32(i32),
    Date64(i64),
    /// The canonical encoding of a value of another type (only `hash`
    /// reads it).
    Enc(Vec<u8>),
}

struct Ctx<'a> {
    cols: Vec<(String, &'a dyn Array, DataType)>,
}

impl Ctx<'_> {
    fn eval(&self, e: &Expr, row: usize) -> Result<Option<Val>> {
        Ok(match e {
            Expr::Column(c) => {
                let (_, a, dt) = self
                    .cols
                    .iter()
                    .find(|(n, _, _)| n == c)
                    .ok_or_else(|| Error::KeySpec(format!("column {c:?} not read")))?;
                read(*a, row, dt)?
            }
            Expr::Str(s) => Some(Val::Str(s.clone())),
            Expr::Int(i) => Some(Val::Int(*i)),
            Expr::JsonGet { input, path } => match self.eval(input, row)? {
                Some(Val::Str(s)) => json_get(&s, path).map(Val::Str),
                None => None,
                Some(other) => return Err(type_bug("json_get", &other)),
            },
            Expr::Lower(input) => match self.eval(input, row)? {
                Some(Val::Str(s)) => Some(Val::Str(s.to_lowercase())),
                None => None,
                Some(other) => return Err(type_bug("lower", &other)),
            },
            Expr::Substr { input, start, len } => match self.eval(input, row)? {
                Some(Val::Str(s)) => Some(Val::Str(substr(&s, *start, *len))),
                None => None,
                Some(other) => return Err(type_bug("substr", &other)),
            },
            Expr::Cast { input, to } => match self.eval(input, row)? {
                None => None,
                Some(v) => cast(v, *to)?,
            },
            Expr::DateTrunc { input, unit } => match self.eval(input, row)? {
                None => None,
                Some(Val::Ts(v, u)) => trunc_ts(v, u, *unit).map(|t| Val::Ts(t, u)),
                Some(Val::Date32(d)) => trunc_days(i64::from(d), *unit)
                    .and_then(|x| i32::try_from(x).ok())
                    .map(Val::Date32),
                Some(Val::Date64(ms)) => {
                    trunc_ts(ms, TimeUnit::Millisecond, *unit).map(Val::Date64)
                }
                Some(other) => return Err(type_bug("date_trunc", &other)),
            },
            Expr::Hash(args) => {
                let mut buf = Vec::new();
                for a in args {
                    match self.eval(a, row)? {
                        None => return Ok(None),
                        Some(v) => encode_val(&v, &mut buf)?,
                    }
                }
                Some(Val::UInt(xxh64(&buf, 0)))
            }
        })
    }
}

fn type_bug(f: &str, v: &Val) -> Error {
    Error::Other(format!(
        "{f} received {v:?}; the type check should have refused it"
    ))
}

#[allow(clippy::many_single_char_names)]
fn read(a: &dyn Array, row: usize, dt: &DataType) -> Result<Option<Val>> {
    let Some(v) = encoding::value_at(a, row)? else {
        return Ok(None);
    };
    Ok(Some(match (unwrap_dict(dt), v) {
        (t, Value::Utf8(s)) if is_string(t) => Val::Str(s.to_string()),
        (t, Value::Int(i)) if is_signed(t) => Val::Int(i),
        (t, Value::UInt(u)) if is_unsigned(t) => Val::UInt(u),
        // A file column of another width of the same family converts like
        // every key column does (`check_convertible`).
        (t, Value::Int(i)) if is_unsigned(t) => match u64::try_from(i) {
            Ok(u) => Val::UInt(u),
            Err(_) => return Err(Error::Other(format!("{i} does not convert to {t}"))),
        },
        (t, Value::UInt(u)) if is_signed(t) => match i64::try_from(u) {
            Ok(i) => Val::Int(i),
            Err(_) => return Err(Error::Other(format!("{u} does not convert to {t}"))),
        },
        (DataType::Timestamp(unit, _), Value::Timestamp { value, unit: from }) => {
            Val::Ts(encoding::convert_time_unit(value, from, *unit)?, *unit)
        }
        (DataType::Date32, Value::Date32(d)) => Val::Date32(d),
        (DataType::Date64, Value::Date64(d)) => Val::Date64(d),
        (t, v) => {
            let mut out = Vec::new();
            encoding::encode_value_as(&v, t, &mut out)?;
            Val::Enc(out)
        }
    }))
}

fn encode_val(v: &Val, out: &mut Vec<u8>) -> Result<()> {
    match v {
        Val::Str(s) => encoding::encode_value_as(&Value::Utf8(s), &DataType::Utf8, out),
        Val::Int(i) => encoding::encode_value_as(&Value::Int(*i), &DataType::Int64, out),
        Val::UInt(u) => encoding::encode_value_as(&Value::UInt(*u), &DataType::UInt64, out),
        Val::Ts(t, u) => encoding::encode_value_as(
            &Value::Timestamp {
                value: *t,
                unit: *u,
            },
            &DataType::Timestamp(*u, None),
            out,
        ),
        Val::Date32(d) => encoding::encode_value_as(&Value::Date32(*d), &DataType::Date32, out),
        Val::Date64(d) => encoding::encode_value_as(&Value::Date64(*d), &DataType::Date64, out),
        Val::Enc(b) => {
            out.extend_from_slice(b);
            Ok(())
        }
    }
}

fn substr(s: &str, start: i64, len: i64) -> String {
    let skip = usize::try_from(start - 1).unwrap_or(usize::MAX);
    let take = usize::try_from(len).unwrap_or(usize::MAX);
    s.chars().skip(skip).take(take).collect()
}

/// A string of an optional `-` and ASCII digits that fits `i64`.
fn parse_i64(s: &str) -> Option<i64> {
    let digits = s.strip_prefix('-').unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn cast(v: Val, to: CastType) -> Result<Option<Val>> {
    Ok(match (to, v) {
        (CastType::Int64, Val::Str(s)) => parse_i64(&s).map(Val::Int),
        (CastType::Int64, Val::Int(i)) => Some(Val::Int(i)),
        (CastType::Int64, Val::UInt(u)) => i64::try_from(u).ok().map(Val::Int),
        (CastType::Int64, Val::Ts(t, _)) => Some(Val::Int(t)),
        (CastType::Int64, Val::Date32(d)) => Some(Val::Int(i64::from(d))),
        (CastType::Int64, Val::Date64(d)) => Some(Val::Int(d)),
        (CastType::Utf8, Val::Str(s)) => Some(Val::Str(s)),
        (CastType::Utf8, Val::Int(i)) => Some(Val::Str(i.to_string())),
        (CastType::Utf8, Val::UInt(u)) => Some(Val::Str(u.to_string())),
        (CastType::Timestamp(u), Val::Int(i)) => Some(Val::Ts(i, u)),
        (CastType::Timestamp(u), Val::UInt(x)) => i64::try_from(x).ok().map(|i| Val::Ts(i, u)),
        (to, other) => {
            return Err(Error::Other(format!(
                "cast to {} received {other:?}; the type check should have refused it",
                to.as_str()
            )))
        }
    })
}

fn per_second(u: TimeUnit) -> i64 {
    match u {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// `v` (in `u` since the epoch, UTC) floored to `unit`; `None` if the result
/// does not fit `i64`.
fn trunc_ts(v: i64, u: TimeUnit, unit: TruncUnit) -> Option<i64> {
    let ps = per_second(u);
    if let Some(secs) = unit.seconds() {
        let step = secs.checked_mul(ps)?;
        return v.div_euclid(step).checked_mul(step);
    }
    let per_day = 86_400i64.checked_mul(ps)?;
    let days = v.div_euclid(per_day);
    trunc_days(days, unit)?.checked_mul(per_day)
}

/// Days since the epoch floored to `unit` (`day` and finer: unchanged).
fn trunc_days(days: i64, unit: TruncUnit) -> Option<i64> {
    let (y, m, _) = civil_from_days(days)?;
    match unit {
        TruncUnit::Month => days_from_civil(y, m, 1),
        TruncUnit::Year => days_from_civil(y, 1, 1),
        _ => Some(days),
    }
}

/// The proleptic Gregorian `(year, month, day)` of days since 1970-01-01.
fn civil_from_days(z: i64) -> Option<(i64, i64, i64)> {
    let z = z.checked_add(719_468)?;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe.checked_add(era.checked_mul(400)?)?;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    Some((if m <= 2 { y + 1 } else { y }, m, d))
}

/// Days since 1970-01-01 of a proleptic Gregorian date.
fn days_from_civil(y: i64, m: i64, d: i64) -> Option<i64> {
    let y = if m <= 2 { y.checked_sub(1)? } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era.checked_mul(146_097)?.checked_add(doe - 719_468)
}

enum OutBuilder {
    Str(StringBuilder),
    Int(Int64Builder),
    UInt(UInt64Builder),
    TsS(PrimitiveBuilder<TimestampSecondType>),
    TsMs(PrimitiveBuilder<TimestampMillisecondType>),
    TsUs(PrimitiveBuilder<TimestampMicrosecondType>),
    TsNs(PrimitiveBuilder<TimestampNanosecondType>),
    Date32(Date32Builder),
    Date64(Date64Builder),
}

impl OutBuilder {
    fn new(dt: &DataType, n: usize) -> Self {
        match dt {
            DataType::Int64 => Self::Int(Int64Builder::with_capacity(n)),
            DataType::UInt64 => Self::UInt(UInt64Builder::with_capacity(n)),
            DataType::Timestamp(u, tz) => match u {
                TimeUnit::Second => {
                    Self::TsS(PrimitiveBuilder::with_capacity(n).with_timezone_opt(tz.clone()))
                }
                TimeUnit::Millisecond => {
                    Self::TsMs(PrimitiveBuilder::with_capacity(n).with_timezone_opt(tz.clone()))
                }
                TimeUnit::Microsecond => {
                    Self::TsUs(PrimitiveBuilder::with_capacity(n).with_timezone_opt(tz.clone()))
                }
                TimeUnit::Nanosecond => {
                    Self::TsNs(PrimitiveBuilder::with_capacity(n).with_timezone_opt(tz.clone()))
                }
            },
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(n)),
            DataType::Date64 => Self::Date64(Date64Builder::with_capacity(n)),
            _ => Self::Str(StringBuilder::with_capacity(n, n * 8)),
        }
    }

    fn push(&mut self, v: Option<Val>) -> Result<()> {
        match (self, v) {
            (Self::Str(b), Some(Val::Str(s))) => b.append_value(s),
            (Self::Str(b), None) => b.append_null(),
            (Self::Int(b), Some(Val::Int(i))) => b.append_value(i),
            (Self::Int(b), None) => b.append_null(),
            (Self::UInt(b), Some(Val::UInt(u))) => b.append_value(u),
            (Self::UInt(b), None) => b.append_null(),
            (Self::TsS(b), v) => b.append_option(ts_of(v)?),
            (Self::TsMs(b), v) => b.append_option(ts_of(v)?),
            (Self::TsUs(b), v) => b.append_option(ts_of(v)?),
            (Self::TsNs(b), v) => b.append_option(ts_of(v)?),
            (Self::Date32(b), Some(Val::Date32(d))) => b.append_value(d),
            (Self::Date32(b), None) => b.append_null(),
            (Self::Date64(b), Some(Val::Date64(d))) => b.append_value(d),
            (Self::Date64(b), None) => b.append_null(),
            (_, Some(v)) => {
                return Err(Error::Other(format!(
                    "expression value {v:?} does not match its checked type"
                )))
            }
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Str(mut b) => Arc::new(b.finish()),
            Self::Int(mut b) => Arc::new(b.finish()),
            Self::UInt(mut b) => Arc::new(b.finish()),
            Self::TsS(mut b) => Arc::new(b.finish()),
            Self::TsMs(mut b) => Arc::new(b.finish()),
            Self::TsUs(mut b) => Arc::new(b.finish()),
            Self::TsNs(mut b) => Arc::new(b.finish()),
            Self::Date32(mut b) => Arc::new(b.finish()),
            Self::Date64(mut b) => Arc::new(b.finish()),
        }
    }
}

fn ts_of(v: Option<Val>) -> Result<Option<i64>> {
    match v {
        None => Ok(None),
        Some(Val::Ts(t, _)) => Ok(Some(t)),
        Some(other) => Err(Error::Other(format!(
            "expression value {other:?} does not match its checked timestamp type"
        ))),
    }
}

// ---------------------------------------------------------------------------
// JSON
// ---------------------------------------------------------------------------

/// What a path points at.
enum Found<'a> {
    Str(String),
    Token(&'a str),
    Nothing,
}

struct Json<'a> {
    s: &'a str,
    b: &'a [u8],
    i: usize,
    found: Option<Found<'a>>,
}

/// The `json_get` value of `doc` at `path` (module docs); `None` for null.
#[must_use]
pub fn json_get(doc: &str, path: &[Step]) -> Option<String> {
    let mut j = Json {
        s: doc,
        b: doc.as_bytes(),
        i: 0,
        found: None,
    };
    j.ws();
    j.value(Some(path), 0).ok()?;
    j.ws();
    if j.i != j.b.len() {
        return None;
    }
    match j.found? {
        Found::Str(s) => Some(s),
        Found::Token(t) => Some(t.to_string()),
        Found::Nothing => None,
    }
}

impl Json<'_> {
    fn ws(&mut self) {
        while self
            .b
            .get(self.i)
            .is_some_and(|c| matches!(c, b' ' | b'\t' | b'\n' | b'\r'))
        {
            self.i += 1;
        }
    }

    fn eat(&mut self, c: u8) -> std::result::Result<(), ()> {
        if self.b.get(self.i) == Some(&c) {
            self.i += 1;
            Ok(())
        } else {
            Err(())
        }
    }

    /// One value. `path` is `Some(rest)` while the value lies on the path;
    /// `Some(&[])` is the target.
    // One arm per JSON value kind (RFC 8259).
    #[allow(clippy::too_many_lines)]
    fn value(&mut self, path: Option<&[Step]>, depth: usize) -> std::result::Result<(), ()> {
        if depth > MAX_JSON_DEPTH {
            return Err(());
        }
        let target = path.is_some_and(<[Step]>::is_empty);
        match *self.b.get(self.i).ok_or(())? {
            b'{' => {
                self.i += 1;
                self.ws();
                let mut taken = false;
                if self.b.get(self.i) == Some(&b'}') {
                    self.i += 1;
                } else {
                    loop {
                        self.ws();
                        let key = self.string()?;
                        self.ws();
                        self.eat(b':')?;
                        self.ws();
                        let sub = match path {
                            Some([Step::Key(k), rest @ ..]) if !taken && *k == key => {
                                taken = true;
                                Some(rest)
                            }
                            _ => None,
                        };
                        self.value(sub, depth + 1)?;
                        self.ws();
                        match self.b.get(self.i) {
                            Some(b',') => self.i += 1,
                            Some(b'}') => {
                                self.i += 1;
                                break;
                            }
                            _ => return Err(()),
                        }
                    }
                }
                if target {
                    self.found = Some(Found::Nothing);
                }
            }
            b'[' => {
                self.i += 1;
                self.ws();
                if self.b.get(self.i) == Some(&b']') {
                    self.i += 1;
                } else {
                    let mut n = 0usize;
                    loop {
                        self.ws();
                        let sub = match path {
                            Some([Step::Index(k), rest @ ..]) if *k == n => Some(rest),
                            _ => None,
                        };
                        self.value(sub, depth + 1)?;
                        n += 1;
                        self.ws();
                        match self.b.get(self.i) {
                            Some(b',') => self.i += 1,
                            Some(b']') => {
                                self.i += 1;
                                break;
                            }
                            _ => return Err(()),
                        }
                    }
                }
                if target {
                    self.found = Some(Found::Nothing);
                }
            }
            b'"' => {
                let s = self.string()?;
                if target {
                    self.found = Some(Found::Str(s));
                }
            }
            b't' | b'f' | b'n' => {
                let (word, found) = match self.b[self.i] {
                    b't' => ("true", Found::Token("true")),
                    b'f' => ("false", Found::Token("false")),
                    _ => ("null", Found::Nothing),
                };
                if !self.b[self.i..].starts_with(word.as_bytes()) {
                    return Err(());
                }
                self.i += word.len();
                if target {
                    self.found = Some(found);
                }
            }
            b'-' | b'0'..=b'9' => {
                let start = self.i;
                self.number()?;
                if target {
                    self.found = Some(Found::Token(&self.s[start..self.i]));
                }
            }
            _ => return Err(()),
        }
        Ok(())
    }

    fn digits(&mut self) -> usize {
        let start = self.i;
        while self.b.get(self.i).is_some_and(u8::is_ascii_digit) {
            self.i += 1;
        }
        self.i - start
    }

    fn number(&mut self) -> std::result::Result<(), ()> {
        if self.b.get(self.i) == Some(&b'-') {
            self.i += 1;
        }
        match self.b.get(self.i) {
            Some(b'0') => self.i += 1,
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return Err(()),
        }
        if self.b.get(self.i) == Some(&b'.') {
            self.i += 1;
            if self.digits() == 0 {
                return Err(());
            }
        }
        if matches!(self.b.get(self.i), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.b.get(self.i), Some(b'+' | b'-')) {
                self.i += 1;
            }
            if self.digits() == 0 {
                return Err(());
            }
        }
        Ok(())
    }

    fn hex4(&mut self) -> std::result::Result<u32, ()> {
        let h = self.s.get(self.i..self.i + 4).ok_or(())?;
        if !h.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(());
        }
        self.i += 4;
        u32::from_str_radix(h, 16).map_err(|_| ())
    }

    /// A string, unescaped. A lone surrogate is invalid.
    fn string(&mut self) -> std::result::Result<String, ()> {
        self.eat(b'"')?;
        let mut out = String::new();
        loop {
            let start = self.i;
            while self
                .b
                .get(self.i)
                .is_some_and(|c| *c != b'"' && *c != b'\\' && *c >= 0x20)
            {
                self.i += 1;
            }
            // `start..i` ends before an ASCII byte, so on a char boundary.
            out.push_str(&self.s[start..self.i]);
            match *self.b.get(self.i).ok_or(())? {
                b'"' => {
                    self.i += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.i += 1;
                    let e = *self.b.get(self.i).ok_or(())?;
                    self.i += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hi = self.hex4()?;
                            let cp = if (0xD800..0xDC00).contains(&hi) {
                                if self.b.get(self.i..self.i + 2) != Some(b"\\u") {
                                    return Err(());
                                }
                                self.i += 2;
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return Err(());
                                }
                                0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                            } else if (0xDC00..0xE000).contains(&hi) {
                                return Err(());
                            } else {
                                hi
                            };
                            out.push(char::from_u32(cp).ok_or(())?);
                        }
                        _ => return Err(()),
                    }
                }
                _ => return Err(()),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Conformance vectors
// ---------------------------------------------------------------------------

/// The pinned conformance vectors, per function: `(function, closed
/// expression, canonical encoding of its value in lowercase hex, or "null")`.
/// `docs/format.md` lists the same rows; `tests/expr_conformance.rs` checks
/// both.
pub const VECTORS: &[(&str, &str, &str)] = &[
    (
        "json_get",
        r#"json_get('{"symbol":"SYM1","px":1.50}','$.symbol')"#,
        "0653594d 310000",
    ),
    (
        "json_get",
        r#"json_get('{"px":1.50}','$.px')"#,
        "06312e35 300000",
    ),
    (
        "json_get",
        r#"json_get('{"a":{"b c":[10,"x\u00e9"]}}','$.a["b c"][1]')"#,
        "0678c3a9 0000",
    ),
    ("json_get", r#"json_get('{"a":1,"a":2}','$.a')"#, "06310000"),
    (
        "json_get",
        r#"json_get('{"a":"\ud83d\ude00"}','$.a')"#,
        "06f09f98 800000",
    ),
    (
        "json_get",
        r#"json_get('{"a":true}','$.a')"#,
        "06747275 650000",
    ),
    ("json_get", r#"json_get('{"a":null}','$.a')"#, "null"),
    ("json_get", r#"json_get('{"a":[1]}','$.a')"#, "null"),
    ("json_get", r#"json_get('{"a":1','$.a')"#, "null"),
    ("lower", "lower('AbC-Z')", "06616263 2d7a0000"),
    ("lower", "lower('ÀB')", "06c3a062 0000"),
    ("lower", "lower('İ')", "0669cc87 0000"),
    ("lower", "lower('ΣΑΣ')", "06cf83ce b1cf8200 00"),
    ("substr", "substr('héllo',2,3)", "06c3a96c 6c0000"),
    ("substr", "substr('abc',5,2)", "060000"),
    ("cast", "cast('007',int64)", "02800000 00000000 07"),
    ("cast", "cast('-12',int64)", "027fffff ffffffff f4"),
    ("cast", "cast('1.5',int64)", "null"),
    ("cast", "cast(42,utf8)", "06343200 00"),
    (
        "cast",
        "cast(86400000000,timestamp_us)",
        "080403 80000014 1dd76000",
    ),
    (
        "date_trunc",
        "date_trunc(cast(90061000001,timestamp_us),day)",
        "080403 80000014 1dd76000",
    ),
    (
        "date_trunc",
        "date_trunc(cast(90061000001,timestamp_us),hour)",
        "080403 80000014 f46b0400",
    ),
    (
        "date_trunc",
        "date_trunc(cast(-1,timestamp_us),day)",
        "080403 7fffffeb e228a000",
    ),
    (
        "date_trunc",
        "date_trunc(cast(6307200000000,timestamp_us),month)",
        "080403 800004a2 e0a32000",
    ),
    (
        "date_trunc",
        "date_trunc(cast(6307200000000,timestamp_us),year)",
        "080403 80000000 00000000",
    ),
    ("hash", "hash('SYM1')", "03 29185bcc 2d6b9e2a"),
    ("hash", "hash('SYM1','X1')", "03 2be2127e 8956e278"),
    ("hash", "hash(1,'a')", "03 e51a8fcd c38e4db3"),
];

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// The canonical encoding (lowercase hex) of the value of a closed
/// expression, or `None` when it is null.
pub fn eval_closed(text: &str) -> Result<Option<String>> {
    let e = Expr::parse(text)?;
    if !e.columns().is_empty() {
        return Err(Error::KeySpec(format!(
            "conformance vector {text:?} reads a column; a vector is closed (literals only)"
        )));
    }
    let t = Typed::new(e, &Schema::empty())?;
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|e| Error::Other(e.to_string()))?;
    Ok(t.encode_rows(&batch)?
        .into_iter()
        .next()
        .flatten()
        .map(|b| hex(&b)))
}

/// The vectors an `expr` key over `expr` carries: every [`VECTORS`] row of
/// every function it calls, with the expected values written without
/// spaces.
#[must_use]
pub fn vectors_for(expr: &Expr) -> Vec<ExprVector> {
    let used = expr.functions();
    VECTORS
        .iter()
        .filter(|(f, _, _)| used.contains(f))
        .map(|(_, e, out)| ExprVector {
            expr: normalize(e).unwrap_or_else(|_| (*e).to_string()),
            value: match *out {
                "null" => None,
                o => Some(o.replace(' ', "")),
            },
        })
        .collect()
}

/// Vectors this process has already reproduced. Reproducing one is
/// deterministic for a given binary, so a hit skips the work; bounded.
static REPRODUCED: std::sync::Mutex<BTreeSet<(String, Option<String>)>> =
    std::sync::Mutex::new(BTreeSet::new());

/// Most entries [`REPRODUCED`] holds before it is cleared.
const REPRODUCED_CAP: usize = 4096;

/// Recomputes every vector of an `expr` key; refuses the first one this
/// binary does not reproduce, naming what each side computed.
pub fn verify_vectors(spec_name: &str, vectors: &[ExprVector]) -> Result<()> {
    for v in vectors {
        let key = (v.expr.clone(), v.value.clone());
        if REPRODUCED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&key)
        {
            continue;
        }
        let got = eval_closed(&v.expr)?;
        if got != v.value {
            return Err(Error::KeySpec(format!(
                "expr key {spec_name} records that {} is {}, but this binary computes {}; its \
                 segments would hold values this binary does not recompute, so lookups could \
                 miss rows: rebuild the index with this binary, or use the binary that built it",
                v.expr,
                v.value.as_deref().unwrap_or("null"),
                got.as_deref().unwrap_or("null"),
            )));
        }
        let mut done = REPRODUCED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if done.len() >= REPRODUCED_CAP {
            done.clear();
        }
        done.insert(key);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Key specs
// ---------------------------------------------------------------------------

/// The segment an `expr` key builds over its virtual column.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Over {
    /// An exact segment.
    Exact,
    /// An exact segment that also stores, per derived value and row group,
    /// its rows (storage version 2 counts), so `count` on the expression is
    /// answered from the index.
    Counts,
    /// A bloom segment at a false-positive rate.
    Bloom(f64),
}

/// The `expr` key spec named `name` over `text`, indexed by `over`: the
/// expression normalized, with the conformance vectors of the functions it
/// calls.
pub fn spec(name: &str, text: &str, over: Over) -> Result<KeySpec> {
    let e = Expr::parse(text)?;
    let columns = vec![name.to_string()];
    let spec = KeySpec::Expr {
        name: name.to_string(),
        expr: e.to_string(),
        over: Box::new(match over {
            Over::Exact | Over::Counts => KeySpec::Exact {
                columns,
                counts: over == Over::Counts,
                bounds: None,
            },
            Over::Bloom(fpr) => KeySpec::Bloom {
                columns,
                fpr,
                bounds: None,
            },
        }),
        conformance: vectors_for(&e),
    };
    check_spec(&spec)?;
    Ok(spec)
}

/// The rules of an `expr` key spec that need no schema: the name is an
/// identifier; the expression parses, calls a function, reads a column, and
/// is in normalized form; the inner segment is exact or bloom over exactly
/// the name; every conformance vector reproduces. A spec of another kind
/// passes.
pub fn check_spec(spec: &KeySpec) -> Result<()> {
    let KeySpec::Expr {
        name,
        expr,
        over,
        conformance,
    } = spec
    else {
        return Ok(());
    };
    if !is_ident(name) {
        return Err(Error::KeySpec(format!(
            "the name of an expr key is a column-like identifier (letters, digits, _), not \
             {name:?}; write expr:<name>=<expression>:exact"
        )));
    }
    let e = Expr::parse(expr)?;
    if !e.is_call() {
        return Err(Error::KeySpec(format!(
            "expr key {name}: {expr:?} calls no function; index a column directly with \
             exact:<column>"
        )));
    }
    if e.columns().is_empty() {
        return Err(Error::KeySpec(format!(
            "expr key {name}: {expr:?} reads no column, so its value is the same on every row"
        )));
    }
    let normalized = e.to_string();
    if normalized != *expr {
        return Err(Error::KeySpec(format!(
            "expr key {name}: the expression is stored normalized, as {normalized:?}, not \
             {expr:?}"
        )));
    }
    match over.as_ref() {
        KeySpec::Exact {
            columns,
            bounds: None,
            ..
        }
        | KeySpec::Bloom {
            columns,
            bounds: None,
            ..
        } if columns.as_slice() == std::slice::from_ref(name) => {}
        other => {
            return Err(Error::KeySpec(format!(
                "expr key {name}: its segment is exact (optionally with counts) or bloom over \
                 the virtual column {name}, with no time bounds, not {}",
                other.name()
            )))
        }
    }
    // Every vector this binary pins for a function the expression calls
    // must be recorded: a spec that omits one (an empty list, or vectors of
    // other functions) would let an evaluator that moved go unnoticed.
    if let Some(missing) = vectors_for(&e)
        .into_iter()
        .find(|v| !conformance.iter().any(|c| c.expr == v.expr))
    {
        return Err(Error::KeySpec(format!(
            "expr key {name} does not record the conformance vector {} that this binary pins \
             for a function its expression calls, so its derived values cannot be trusted; \
             declare it again with this binary: bellwether index <data root> --key {}",
            missing.expr,
            shell_quote(&spec.identity())
        )));
    }
    verify_vectors(&spec.name(), conformance)
}

/// [`check_spec`], then the schema's rules: the name is not a dataset
/// column, and the expression type-checks. Returns the checked expression.
pub fn typed_spec(spec: &KeySpec, schema: &Schema) -> Result<Typed> {
    check_spec(spec)?;
    let KeySpec::Expr { name, expr, .. } = spec else {
        return Err(Error::KeySpec(format!(
            "{} is not an expr key",
            spec.name()
        )));
    };
    if schema.field_with_name(name).is_ok() {
        return Err(Error::KeySpec(format!(
            "expr key {name}: {name} is already a dataset column; name the virtual column \
             something else, for example expr:{name}_key={expr}:exact"
        )));
    }
    Typed::parse(expr, schema)
}

/// The dataset columns an `expr` key reads; an `agg` key reads its key
/// columns and its value column; a spec of another kind reads its own
/// columns.
pub fn input_columns(spec: &KeySpec) -> Result<Vec<String>> {
    match spec {
        KeySpec::Expr { expr, .. } => Ok(Expr::parse(expr)?.columns()),
        KeySpec::Agg { columns, value, .. } => {
            let mut out = columns.clone();
            out.push(value.clone());
            Ok(out)
        }
        other => Ok(other.columns().to_vec()),
    }
}

/// `batch` with the virtual column of an `expr` key appended, computed from
/// the batch's input columns (one row group's worth: a transient derived
/// column, ADR 018). A spec of another kind returns the batch unchanged.
pub fn with_derived(spec: &KeySpec, schema: &Schema, batch: &RecordBatch) -> Result<RecordBatch> {
    let KeySpec::Expr { name, .. } = spec else {
        return Ok(batch.clone());
    };
    if batch.column_by_name(name).is_some() {
        return Ok(batch.clone());
    }
    let typed = typed_spec(spec, schema)?;
    let derived = typed.eval(batch)?;
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(name, typed.data_type().clone(), true));
    let mut columns = batch.columns().to_vec();
    columns.push(derived);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| Error::Other(e.to_string()))
}

/// `schema` with the virtual column of every `expr` key in `specs`
/// appended at its expression's type, so a segment over the virtual column
/// opens and encodes against it like any key column.
pub fn derived_schema(schema: &Schema, specs: &[KeySpec]) -> Result<Schema> {
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    for s in specs {
        if let KeySpec::Expr { name, .. } = s {
            if fields.iter().any(|f| f.name() == name) {
                continue;
            }
            let t = typed_spec(s, schema)?;
            fields.push(Field::new(name, t.data_type().clone(), true));
        }
    }
    Ok(Schema::new(fields))
}

/// Whether `column` names an expression rather than a column of `schema`:
/// it is not a dataset column and parses as a function call.
#[must_use]
pub fn is_expression(column: &str, schema: &Schema) -> bool {
    schema.field_with_name(column).is_err() && Expr::parse(column).is_ok_and(|e| e.is_call())
}

/// A virtual column name for an `expr` key over `expr`, for a candidate the
/// engine proposes: the identifiers and alphanumeric literal text of the
/// expression joined by `_`, lowercase, at most 48 characters
/// (`json_get(payload,'$.symbol')` gives `json_get_payload_symbol`), with
/// `_key` appended while it names a column of `schema`.
#[must_use]
pub fn suggested_name(expr: &Expr, schema: &Schema) -> String {
    let text = expr.to_string().to_ascii_lowercase();
    let mut name = String::new();
    for part in text
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|p| !p.is_empty())
    {
        if name.len() + part.len() + 1 > 48 {
            break;
        }
        if !name.is_empty() {
            name.push('_');
        }
        name.push_str(part);
    }
    if !name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
        name.insert_str(0, "k_");
    }
    while schema.field_with_name(&name).is_ok() {
        name.push_str("_key");
    }
    name
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
