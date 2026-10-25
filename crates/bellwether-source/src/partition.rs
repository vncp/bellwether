//! Partition values from paths (`docs/format.md`, "Partition columns"):
//! Hive-style `name=value` directories, or the placeholders of a path
//! template such as `grouped_daily_us/{day:date}.parquet`.
//!
//! Under hive partitioning, every directory component of a file's path
//! *below the data root* of the form `name=value` is a virtual column of
//! that file: every row of the file has `value` in column `name`. Under a
//! path template, every placeholder is one, with the value the file's path
//! spells there. The source reports each as a column with
//! `min = max = value` in each row group's statistics and synthesizes it in
//! `read_columns`, so the profiler sees it and the planner prunes on it
//! without reading any data page.
//!
//! The rules, each a refusal rather than a guess:
//!
//! - Only the path below the root counts, so a root such as
//!   `/lake/env=prod/trades` never injects `env`. A file not under the root
//!   is refused. Under hive partitioning the file name itself never counts.
//! - Hive: a component is split at its first `=`. An empty name, an empty
//!   value, or a name repeated within one path is refused. Values are
//!   percent-decoded (`%2F` is `/`), as Hive writes them; a `%` not followed
//!   by two hex digits, or a decoded value that is not UTF-8, is refused.
//!   `__HIVE_DEFAULT_PARTITION__` is a null.
//! - Template ([`PathTemplate`]): the whole path below the root must match
//!   the template. A file that does not, or that matches it in more than
//!   one way, is refused, and discovery names up to ten such files at once,
//!   never skipping one. Values are taken as written (no decoding, no null).
//! - Every file under one root has the same partition columns in the same
//!   order (checked at discovery; a template guarantees it).
//! - A value is typed by the dataset schema when the column is there (an
//!   exact Arrow cast; a value that does not cast is refused). Without a
//!   dataset schema it takes a template placeholder's declared type, else
//!   [`infer_type`], a fixed table that is part of the format (it never
//!   follows a dependency's inference).
//! - A file that also stores the column must agree with its path: every row
//!   group's footer bounds must both equal the path value and its null count
//!   must be zero (or, for a null path value, equal the row count). Unknown
//!   or disagreeing statistics are refused, never trusted.

use std::sync::Arc;

use arrow::array::{new_null_array, Array, ArrayRef, StringArray};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::DataType;
use arrow::util::display::FormatOptions;
use bellwether_types::{Error, Result};

/// The partitioning scheme of a source folder, as the manifest records it
/// in `source.partitioning`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Partitioning {
    /// No partition columns: paths carry no data. The default, and what an
    /// index that records no scheme means.
    #[default]
    None,
    /// Hive-style `name=value` directories are virtual columns.
    Hive,
    /// Each placeholder of a path template is a virtual column.
    Template(PathTemplate),
}

/// The manifest's `source.partitioning` prefix of a path template.
const TEMPLATE_PREFIX: &str = "template:";

/// How many non-matching paths a template refusal names.
pub const NON_MATCHING_SHOWN: usize = 10;

impl Partitioning {
    /// The manifest's `source.partitioning` value: `None` for no scheme (the
    /// field is then omitted), `"hive"` for hive, `"template:<pattern>"` for
    /// a path template (the pattern exactly as given, so a changed template
    /// is a changed scheme).
    #[must_use]
    pub fn manifest_name(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::Hive => Some("hive".to_string()),
            Self::Template(t) => Some(format!("{TEMPLATE_PREFIX}{}", t.pattern())),
        }
    }

    /// How messages name the scheme: `none`, `hive`, or
    /// `path template "<pattern>"`.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::None => "none".to_string(),
            Self::Hive => "hive".to_string(),
            Self::Template(t) => format!("path template {:?}", t.pattern()),
        }
    }

    /// Whether paths carry no data.
    #[must_use]
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Parses `source.partitioning`; absent means [`Partitioning::None`].
    /// A malformed template is [`Error::Source`]; anything else unknown is
    /// [`Error::UnknownKind`].
    pub fn parse(name: Option<&str>) -> Result<Self> {
        match name {
            None => Ok(Self::None),
            Some("hive") => Ok(Self::Hive),
            Some(t) if t.starts_with(TEMPLATE_PREFIX) => Ok(Self::Template(PathTemplate::parse(
                &t[TEMPLATE_PREFIX.len()..],
            )?)),
            Some(other) => Err(Error::UnknownKind {
                what: "partitioning",
                found: other.to_string(),
            }),
        }
    }

    /// The partition columns of `path` under `root`, in path order (see the
    /// module docs for the rules); none without a scheme.
    pub fn values(&self, root: &str, path: &str) -> Result<Vec<PartitionValue>> {
        match self {
            Self::None => Ok(Vec::new()),
            Self::Hive => hive_values(root, path),
            Self::Template(t) => t.values(root, path),
        }
    }

    /// [`Partitioning::values`] of every path. Under a template, every path
    /// that does not match is collected first, and the refusal names up to
    /// [`NON_MATCHING_SHOWN`] of them and counts the rest.
    pub fn values_of_all(&self, root: &str, paths: &[String]) -> Result<Vec<Vec<PartitionValue>>> {
        let Self::Template(t) = self else {
            return paths.iter().map(|p| self.values(root, p)).collect();
        };
        let mut out = Vec::with_capacity(paths.len());
        let mut refused: Vec<String> = Vec::new();
        let mut first_reason: Option<String> = None;
        for p in paths {
            match t.values(root, p) {
                Ok(v) => out.push(v),
                Err(e) => {
                    if first_reason.is_none() {
                        first_reason = Some(match e {
                            Error::Source(m) => m,
                            other => other.to_string(),
                        });
                    }
                    refused.push(relative(root, p));
                }
            }
        }
        if refused.is_empty() {
            return Ok(out);
        }
        let shown = refused
            .iter()
            .take(NON_MATCHING_SHOWN)
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        let more = refused.len().saturating_sub(NON_MATCHING_SHOWN);
        let more = if more > 0 {
            format!(" and {more} more")
        } else {
            String::new()
        };
        Err(Error::Source(format!(
            "{} of {} Parquet files under {root} do not match the path template {:?}: \
             {shown}{more} (first: {}); every file under the root must match, so point the root \
             at the files the template describes, or change the template",
            refused.len(),
            paths.len(),
            t.pattern(),
            first_reason.unwrap_or_default()
        )))
    }

    /// The type a template placeholder declares for column `name`, if any
    /// (`None` under hive partitioning, and for an untyped placeholder).
    #[must_use]
    pub fn declared_type(&self, name: &str) -> Option<DataType> {
        match self {
            Self::Template(t) => t.declared_type(name),
            _ => None,
        }
    }

    /// The partition columns this scheme gives the files under `root`: a
    /// template's placeholders, or the `name=value` directories of the
    /// first file listed under it (every file has the same ones; discovery
    /// checks).
    pub fn column_names(&self, root: &str) -> Result<Vec<String>> {
        use crate::storage::{LocalFs, Storage};
        Ok(match self {
            Self::None => Vec::new(),
            Self::Template(t) => t.names().map(str::to_string).collect(),
            Self::Hive => match LocalFs.list(root)?.first() {
                Some(p) => hive_values(root, p)?.into_iter().map(|v| v.name).collect(),
                None => Vec::new(),
            },
        })
    }
}

/// An error's message without the `source error:` prefix a re-wrap into
/// [`Error::Source`] would add again.
pub(crate) fn plain(e: Error) -> String {
    match e {
        Error::Source(m) => m,
        other => other.to_string(),
    }
}

/// `path` relative to `root` for messages, or `path` itself when it is not
/// under it.
fn relative(root: &str, path: &str) -> String {
    std::path::Path::new(path)
        .strip_prefix(root)
        .map_or_else(|_| path.to_string(), |r| r.to_string_lossy().into_owned())
}

/// The type of a path-template placeholder (`{day:date}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldType {
    /// `str`: any non-empty text without `/`; typed `Utf8`.
    Str,
    /// `int`: an optional `-` then digits as [`infer_type`] reads an
    /// integer (no leading zero, fits `i64`); typed `Int64`.
    Int,
    /// `date`: `YYYY-MM-DD`, a valid proleptic Gregorian date; typed
    /// `Date32`.
    Date,
}

impl FieldType {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "str" => Some(Self::Str),
            "int" => Some(Self::Int),
            "date" => Some(Self::Date),
            _ => None,
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Str => DataType::Utf8,
            Self::Int => DataType::Int64,
            Self::Date => DataType::Date32,
        }
    }

    fn accepts(self, v: &str) -> bool {
        match self {
            Self::Str => true,
            Self::Int => is_canonical_int(v),
            Self::Date => parse_date(v).is_some(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Part {
    Literal(String),
    Field { name: String, ty: Option<FieldType> },
}

/// A path template: the path of every data file below the root, with
/// `{name}` or `{name:type}` placeholders (`type` is `str`, `int`, or
/// `date`) whose values become columns. `{{` and `}}` spell literal braces.
///
/// The template matches the whole path below the root, `/`-separated. A
/// placeholder matches one or more characters other than `/` that its type
/// accepts, so `{day:date}` matches exactly `YYYY-MM-DD`. Parsing refuses an
/// empty template, an absolute one, `.` or `..` or empty components, a
/// template with no placeholder, an unknown type, a placeholder name that is
/// not `[A-Za-z_][A-Za-z0-9_]*` or appears twice, and two placeholders with
/// nothing between them (they could split a value either way).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathTemplate {
    pattern: String,
    parts: Vec<Part>,
}

impl PathTemplate {
    /// Parses `pattern` (see the type docs); every refusal says what is
    /// wrong and is [`Error::Source`].
    pub fn parse(pattern: &str) -> Result<Self> {
        let bad = |why: &str| {
            Error::Source(format!(
                "the path template {pattern:?} {why}; write the path of each file below the \
                 root with placeholders, for example \"{{day:date}}.parquet\" or \
                 \"grouped_daily_us/{{symbol:str}}/{{day:date}}.parquet\""
            ))
        };
        if pattern.is_empty() {
            return Err(bad("is empty"));
        }
        if pattern.starts_with('/') {
            return Err(bad(
                "is absolute; it is matched against the path below the root",
            ));
        }
        if pattern
            .split('/')
            .any(|c| c.is_empty() || c == "." || c == "..")
        {
            return Err(bad("has an empty, `.`, or `..` path component"));
        }
        let mut parts: Vec<Part> = Vec::new();
        let mut literal = String::new();
        let mut chars = pattern.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '{' if chars.peek() == Some(&'{') => {
                    chars.next();
                    literal.push('{');
                }
                '}' if chars.peek() == Some(&'}') => {
                    chars.next();
                    literal.push('}');
                }
                '}' => return Err(bad("has a `}` that closes no placeholder (write `}}`)")),
                '{' => {
                    let mut body = String::new();
                    loop {
                        match chars.next() {
                            Some('}') => break,
                            Some(c) if c == '{' || c == '/' => {
                                return Err(bad("has a placeholder that is not closed by `}`"))
                            }
                            Some(c) => body.push(c),
                            None => return Err(bad("has a placeholder that is not closed by `}`")),
                        }
                    }
                    let (name, ty) = parse_placeholder(&body).map_err(|why| bad(&why))?;
                    if parts
                        .iter()
                        .any(|p| matches!(p, Part::Field { name: n, .. } if n == name))
                    {
                        return Err(bad(&format!("names the placeholder {name:?} twice")));
                    }
                    if literal.is_empty() && matches!(parts.last(), Some(Part::Field { .. })) {
                        return Err(bad(
                            "has two placeholders with nothing between them, so a value could \
                             split either way; put a literal separator between them",
                        ));
                    }
                    if !literal.is_empty() {
                        parts.push(Part::Literal(std::mem::take(&mut literal)));
                    }
                    parts.push(Part::Field {
                        name: name.to_string(),
                        ty,
                    });
                }
                c => literal.push(c),
            }
        }
        if !literal.is_empty() {
            parts.push(Part::Literal(literal));
        }
        if !parts.iter().any(|p| matches!(p, Part::Field { .. })) {
            return Err(bad("has no {placeholder}, so it gives no column"));
        }
        Ok(Self {
            pattern: pattern.to_string(),
            parts,
        })
    }

    /// The pattern as given.
    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// The placeholder names, in template order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.parts.iter().filter_map(|p| match p {
            Part::Field { name, .. } => Some(name.as_str()),
            Part::Literal(_) => None,
        })
    }

    /// The type placeholder `name` declares, if it is one and is typed.
    #[must_use]
    pub fn declared_type(&self, name: &str) -> Option<DataType> {
        self.parts.iter().find_map(|p| match p {
            Part::Field { name: n, ty } if n == name => ty.map(FieldType::data_type),
            _ => None,
        })
    }

    /// The placeholder values of `path` under `root`, in template order: an
    /// error when the path is not under the root, does not match, or
    /// matches in more than one way.
    pub fn values(&self, root: &str, path: &str) -> Result<Vec<PartitionValue>> {
        let rel = std::path::Path::new(path)
            .strip_prefix(root)
            .map_err(|_| {
                Error::Source(format!(
                    "{path} is not under the data root {root}, so its path-template columns are \
                     unknown"
                ))
            })?
            .to_str()
            .ok_or_else(|| Error::Source(format!("{path}: the path is not UTF-8")))?
            .replace(std::path::MAIN_SEPARATOR, "/");
        let mut found: Vec<Vec<String>> = Vec::new();
        self.matches(0, &rel, &mut Vec::new(), &mut found);
        match found.len() {
            0 => Err(Error::Source(format!(
                "{rel} does not match the path template {:?}",
                self.pattern
            ))),
            1 => Ok(self
                .names()
                .zip(found.remove(0))
                .map(|(name, value)| PartitionValue {
                    name: name.to_string(),
                    value: Some(value),
                })
                .collect()),
            _ => Err(Error::Source(format!(
                "{rel} matches the path template {:?} in more than one way ({}); put a literal \
                 that values cannot contain between the placeholders, or type them",
                self.pattern,
                found
                    .iter()
                    .map(|vs| self
                        .names()
                        .zip(vs)
                        .map(|(n, v)| format!("{n}={v}"))
                        .collect::<Vec<_>>()
                        .join(" "))
                    .collect::<Vec<_>>()
                    .join(" or ")
            ))),
        }
    }

    /// Every way parts `i..` match `rest`, given the values so far; stops
    /// after two (one is a match, two is ambiguous).
    fn matches(&self, i: usize, rest: &str, values: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
        if out.len() >= 2 {
            return;
        }
        let Some(part) = self.parts.get(i) else {
            if rest.is_empty() {
                out.push(values.clone());
            }
            return;
        };
        match part {
            Part::Literal(l) => {
                if let Some(r) = rest.strip_prefix(l.as_str()) {
                    self.matches(i + 1, r, values, out);
                }
            }
            Part::Field { ty, .. } => {
                let span = rest.find('/').unwrap_or(rest.len());
                for end in rest[..span].char_indices().map(|(at, c)| at + c.len_utf8()) {
                    let v = &rest[..end];
                    if ty.is_none_or(|t| t.accepts(v)) {
                        values.push(v.to_string());
                        self.matches(i + 1, &rest[end..], values, out);
                        values.pop();
                    }
                }
            }
        }
    }
}

/// The name and type of a placeholder's body (`day:date`), or why it is
/// not one.
fn parse_placeholder(body: &str) -> std::result::Result<(&str, Option<FieldType>), String> {
    let (name, ty) = match body.split_once(':') {
        None => (body, None),
        Some((n, t)) => match FieldType::parse(t) {
            Some(ty) => (n, Some(ty)),
            None => {
                return Err(format!(
                    "gives placeholder {{{body}}} the type {t:?}; the types are str, int, and date"
                ))
            }
        },
    };
    let valid_name = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid_name {
        return Err(format!(
            "has a placeholder {{{body}}} whose name is not a letter or `_` followed by \
             letters, digits, or `_`"
        ));
    }
    Ok((name, ty))
}

impl std::fmt::Display for PathTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.pattern)
    }
}

/// The value Hive writes for a null partition value.
pub const HIVE_NULL: &str = "__HIVE_DEFAULT_PARTITION__";

/// One partition column of one file, as its path spells it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionValue {
    /// Column name.
    pub name: String,
    /// The decoded value, or `None` for [`HIVE_NULL`].
    pub value: Option<String>,
}

/// The partition columns of `path` under `root`, in path order (see the
/// module docs for the rules).
pub fn hive_values(root: &str, path: &str) -> Result<Vec<PartitionValue>> {
    let root_path = std::path::Path::new(root);
    let rel = std::path::Path::new(path)
        .strip_prefix(root_path)
        .map_err(|_| {
            Error::Source(format!(
                "{path} is not under the data root {root}, so its partition columns are unknown"
            ))
        })?;
    let mut components: Vec<&str> = Vec::new();
    for c in rel.components() {
        match c {
            std::path::Component::Normal(s) => {
                components.push(s.to_str().ok_or_else(|| {
                    Error::Source(format!("{path}: a path component is not UTF-8"))
                })?);
            }
            _ => {
                return Err(Error::Source(format!(
                    "{path}: unexpected path component {c:?} below the data root"
                )))
            }
        }
    }
    // The file name never names a partition.
    components.pop();
    let mut out: Vec<PartitionValue> = Vec::new();
    for comp in components {
        let Some((name, raw)) = comp.split_once('=') else {
            continue;
        };
        if name.is_empty() {
            return Err(Error::Source(format!(
                "{path}: directory {comp:?} has an empty partition column name"
            )));
        }
        if raw.is_empty() {
            return Err(Error::Source(format!(
                "{path}: directory {comp:?} has an empty partition value (Hive writes {HIVE_NULL} \
                 for a null)"
            )));
        }
        let name = percent_decode(name, path)?;
        if out.iter().any(|p| p.name == name) {
            return Err(Error::Source(format!(
                "{path}: partition column {name:?} appears twice in one path"
            )));
        }
        let value = if raw == HIVE_NULL {
            None
        } else {
            Some(percent_decode(raw, path)?)
        };
        out.push(PartitionValue { name, value });
    }
    Ok(out)
}

fn percent_decode(s: &str, path: &str) -> Result<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| {
                    Error::Source(format!(
                        "{path}: {s:?} has a '%' not followed by two hex digits"
                    ))
                })?;
            out.push(hex);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|_| Error::Source(format!("{path}: {s:?} does not decode to UTF-8")))
}

/// The type of a partition value when no dataset schema declares the
/// column. A fixed table, part of the format (`docs/format.md`, partition
/// inference), chosen so the value round-trips exactly:
///
/// | Value | Type |
/// |---|---|
/// | an optional `-` then digits, no leading zero (except `0` itself), fitting `i64` | `Int64` |
/// | `YYYY-MM-DD`, a valid proleptic Gregorian date | `Date32` |
/// | anything else, and a null | `Utf8` |
///
/// `007` and `+7` stay strings (they would not print back as written), and
/// so do `true`, `1.5`, and timestamps.
#[must_use]
pub fn infer_type(value: Option<&str>) -> DataType {
    match value {
        Some(v) if is_canonical_int(v) => DataType::Int64,
        Some(v) if parse_date(v).is_some() => DataType::Date32,
        _ => DataType::Utf8,
    }
}

/// The type of a partition column from every value its directories spell
/// under one root: the type [`infer_type`] gives every non-null value when
/// they all agree, else `Utf8` (every value is a valid string). A column
/// whose every value is null is `Utf8`. Inferring over all files, never the
/// first one, means one folder always yields one type, whatever the listing
/// order.
#[must_use]
pub fn infer_column_type<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> DataType {
    let mut out: Option<DataType> = None;
    for v in values.into_iter().flatten() {
        let t = infer_type(Some(v));
        match &out {
            None => out = Some(t),
            Some(o) if *o == t => {}
            Some(_) => return DataType::Utf8,
        }
    }
    out.unwrap_or(DataType::Utf8)
}

fn is_canonical_int(v: &str) -> bool {
    v.parse::<i64>().is_ok_and(|n| n.to_string() == v)
}

/// Days since 1970-01-01 of a `YYYY-MM-DD` date, or `None` when `v` is not
/// exactly that shape or not a valid date.
#[must_use]
pub fn parse_date(text: &str) -> Option<i32> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let digits = |span: std::ops::Range<usize>| -> Option<i64> {
        let part = text.get(span)?;
        part.bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| part.parse().ok())?
    };
    let (year, month, day) = (digits(0..4)?, digits(5..7)?, digits(8..10)?);
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    i32::try_from(days_from_civil(year, month, day)).ok()
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since the Unix epoch of a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// A one-element array holding `value` at `target`, by an exact Arrow cast
/// from its string form; a value that does not cast is refused. A null is a
/// one-element null array.
pub fn typed_value(name: &str, value: Option<&str>, target: &DataType) -> Result<ArrayRef> {
    let Some(v) = value else {
        return Ok(new_null_array(target, 1));
    };
    let strings: ArrayRef = Arc::new(StringArray::from(vec![v]));
    let opts = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let typed = cast_with_options(&strings, target, &opts).map_err(|e| {
        Error::Source(format!(
            "partition value {name}={v} is not a {target} ({e}); the dataset schema types \
             {name} as {target}"
        ))
    })?;
    if typed.is_null(0) {
        return Err(Error::Source(format!(
            "partition value {name}={v} does not convert to {target}"
        )));
    }
    Ok(typed)
}

/// `rows` copies of a one-element array.
pub(crate) fn repeat(one: &ArrayRef, rows: usize) -> Result<ArrayRef> {
    let idx = arrow::array::UInt32Array::from(vec![0u32; rows]);
    arrow::compute::take(one.as_ref(), &idx, None)
        .map_err(|e| Error::Source(format!("partition column: {e}")))
}

/// A partition value as a time in `target`'s unit, the way a window bound
/// is given (days for `Date32`, milliseconds for `Date64`, the timestamp's
/// own unit, an integer as itself), or `None` for a null. A type that is no
/// time or integer column is refused.
pub fn time_value(name: &str, value: Option<&str>, target: &DataType) -> Result<Option<i64>> {
    use arrow::array::AsArray;
    use arrow::datatypes::{
        Date32Type, Date64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt8Type,
    };
    if value.is_none() {
        return Ok(None);
    }
    let a = typed_value(name, value, target)?;
    let v = match target {
        DataType::Date32 => i64::from(a.as_primitive::<Date32Type>().value(0)),
        DataType::Date64 => a.as_primitive::<Date64Type>().value(0),
        DataType::Timestamp(TimeUnit::Second, _) => {
            a.as_primitive::<TimestampSecondType>().value(0)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            a.as_primitive::<TimestampMillisecondType>().value(0)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            a.as_primitive::<TimestampMicrosecondType>().value(0)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            a.as_primitive::<TimestampNanosecondType>().value(0)
        }
        DataType::Int8 => i64::from(a.as_primitive::<Int8Type>().value(0)),
        DataType::Int16 => i64::from(a.as_primitive::<Int16Type>().value(0)),
        DataType::Int32 => i64::from(a.as_primitive::<Int32Type>().value(0)),
        DataType::Int64 => a.as_primitive::<Int64Type>().value(0),
        DataType::UInt8 => i64::from(a.as_primitive::<UInt8Type>().value(0)),
        DataType::UInt16 => i64::from(a.as_primitive::<UInt16Type>().value(0)),
        DataType::UInt32 => i64::from(a.as_primitive::<UInt32Type>().value(0)),
        other => {
            return Err(Error::Source(format!(
                "partition column {name} is {other}, not a date, timestamp, or integer column, so \
                 it cannot bound a time window"
            )))
        }
    };
    Ok(Some(v))
}

/// For each of `paths` under `root`, whether the value `scheme` gives it
/// for `column` lies in `[lo, hi]`, as a time in `ty`'s unit
/// ([`time_value`]); a null value lies in no window (ADR 014). `None` when
/// the scheme does not make `column` a column of every path, so the paths
/// cannot bound it.
pub fn paths_in_window<'a>(
    scheme: &Partitioning,
    root: &str,
    paths: impl IntoIterator<Item = &'a str>,
    column: &str,
    ty: &DataType,
    lo: i64,
    hi: i64,
) -> Result<Option<Vec<bool>>> {
    if scheme.is_none() {
        return Ok(None);
    }
    let mut out = Vec::new();
    for path in paths {
        let values = scheme.values(root, path)?;
        let Some(v) = values.iter().find(|v| v.name == column) else {
            return Ok(None);
        };
        let t = time_value(column, v.value.as_deref(), ty)?;
        out.push(t.is_some_and(|t| lo <= t && t <= hi));
    }
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pv(name: &str, value: Option<&str>) -> PartitionValue {
        PartitionValue {
            name: name.into(),
            value: value.map(str::to_string),
        }
    }

    #[test]
    fn values_come_from_directories_below_the_root_only() {
        let got = hive_values(
            "/lake/env=prod",
            "/lake/env=prod/day=2026-01-02/symbol=AB%2FC/part=1.parquet",
        )
        .unwrap();
        assert_eq!(
            got,
            vec![pv("day", Some("2026-01-02")), pv("symbol", Some("AB/C"))]
        );
        let got = hive_values(
            "/lake",
            "/lake/data/day=__HIVE_DEFAULT_PARTITION__/f.parquet",
        )
        .unwrap();
        assert_eq!(got, vec![pv("day", None)]);
        assert!(hive_values("/lake", "/elsewhere/day=1/f.parquet").is_err());
    }

    #[test]
    fn malformed_components_are_refused() {
        for bad in [
            "/r/=x/f.parquet",
            "/r/day=/f.parquet",
            "/r/day=1/day=2/f.parquet",
            "/r/s=%zz/f.parquet",
            "/r/s=%c3/f.parquet",
        ] {
            assert!(hive_values("/r", bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_column_takes_one_type_from_all_its_values() {
        assert_eq!(infer_column_type([Some("1"), Some("22")]), DataType::Int64);
        assert_eq!(infer_column_type([Some("1"), None]), DataType::Int64);
        assert_eq!(infer_column_type([Some("1"), Some("x")]), DataType::Utf8);
        assert_eq!(infer_column_type([Some("x"), Some("1")]), DataType::Utf8);
        assert_eq!(
            infer_column_type([Some("2026-01-02"), Some("2026-01-03")]),
            DataType::Date32
        );
        assert_eq!(
            infer_column_type([Some("2026-01-02"), Some("7")]),
            DataType::Utf8
        );
        assert_eq!(infer_column_type([None, None]), DataType::Utf8);
    }

    #[test]
    fn inference_is_the_fixed_table() {
        assert_eq!(infer_type(Some("42")), DataType::Int64);
        assert_eq!(infer_type(Some("-7")), DataType::Int64);
        assert_eq!(infer_type(Some("0")), DataType::Int64);
        assert_eq!(infer_type(Some("007")), DataType::Utf8);
        assert_eq!(infer_type(Some("+7")), DataType::Utf8);
        assert_eq!(infer_type(Some("9223372036854775808")), DataType::Utf8);
        assert_eq!(infer_type(Some("2026-02-28")), DataType::Date32);
        assert_eq!(infer_type(Some("2024-02-29")), DataType::Date32);
        assert_eq!(infer_type(Some("2026-02-29")), DataType::Utf8);
        assert_eq!(infer_type(Some("2026-1-02")), DataType::Utf8);
        assert_eq!(infer_type(Some("1.5")), DataType::Utf8);
        assert_eq!(infer_type(Some("true")), DataType::Utf8);
        assert_eq!(infer_type(Some("AAPL")), DataType::Utf8);
        assert_eq!(infer_type(None), DataType::Utf8);
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("2026-01-02"), Some(20_455));
        assert_eq!(parse_date("1969-12-31"), Some(-1));
    }

    #[test]
    fn typed_values_cast_exactly_or_refuse() {
        let a = typed_value("n", Some("12"), &DataType::Int32).unwrap();
        assert_eq!(a.data_type(), &DataType::Int32);
        assert!(typed_value("n", Some("1.5"), &DataType::Int64).is_err());
        assert!(typed_value("d", Some("2026-13-01"), &DataType::Date32).is_err());
        let n = typed_value("d", None, &DataType::Date32).unwrap();
        assert!(n.is_null(0));
    }

    fn tv(values: &[PartitionValue]) -> Vec<(String, String)> {
        values
            .iter()
            .map(|v| (v.name.clone(), v.value.clone().unwrap()))
            .collect()
    }

    fn pair(n: &str, v: &str) -> (String, String) {
        (n.to_string(), v.to_string())
    }

    #[test]
    fn templates_match_the_whole_path_below_the_root() {
        let t = PathTemplate::parse("grouped_daily_us/{day:date}.parquet").unwrap();
        assert_eq!(
            tv(&t
                .values("/lake", "/lake/grouped_daily_us/2026-03-02.parquet")
                .unwrap()),
            vec![pair("day", "2026-03-02")]
        );
        for bad in [
            "/lake/grouped_daily_us/2026-02-30.parquet",
            "/lake/grouped_daily_us/x/2026-03-02.parquet",
            "/lake/other/2026-03-02.parquet",
            "/lake/grouped_daily_us/2026-03-02.parquet.bak",
            "/elsewhere/grouped_daily_us/2026-03-02.parquet",
        ] {
            assert!(t.values("/lake", bad).is_err(), "{bad}");
        }
        let t = PathTemplate::parse("{symbol:str}/{day}_{n:int}.parquet").unwrap();
        assert_eq!(
            tv(&t.values("/r", "/r/BRK_B/2026-01-02_7.parquet").unwrap()),
            vec![
                pair("symbol", "BRK_B"),
                pair("day", "2026-01-02"),
                pair("n", "7")
            ]
        );
        assert_eq!(t.declared_type("symbol"), Some(DataType::Utf8));
        assert_eq!(t.declared_type("day"), None);
        assert_eq!(t.declared_type("n"), Some(DataType::Int64));
        // `{{` and `}}` are literal braces.
        let t = PathTemplate::parse("{{x}}{day:date}.parquet").unwrap();
        assert_eq!(
            tv(&t.values("/r", "/r/{x}2026-01-02.parquet").unwrap()),
            vec![pair("day", "2026-01-02")]
        );
    }

    #[test]
    fn an_ambiguous_match_is_refused_not_guessed() {
        let t = PathTemplate::parse("{a}_{b}.parquet").unwrap();
        let e = t.values("/r", "/r/x_y_z.parquet").unwrap_err().to_string();
        assert!(e.contains("more than one way"), "{e}");
        assert!(e.contains("a=x b=y_z") && e.contains("a=x_y b=z"), "{e}");
        // A type can make it unique.
        let t = PathTemplate::parse("{a}_{b:int}.parquet").unwrap();
        assert_eq!(
            tv(&t.values("/r", "/r/x_y_7.parquet").unwrap()),
            vec![pair("a", "x_y"), pair("b", "7")]
        );
    }

    #[test]
    fn malformed_templates_are_refused() {
        for bad in [
            "",
            "/abs/{d}.parquet",
            "a//{d}.parquet",
            "../{d}.parquet",
            "./{d}.parquet",
            "plain.parquet",
            "{d:float}.parquet",
            "{1d}.parquet",
            "{}.parquet",
            "{d}{e}.parquet",
            "{d}_{d}.parquet",
            "{d.parquet",
            "d}.parquet",
            "{a/b}.parquet",
        ] {
            assert!(PathTemplate::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_manifest_spelling_round_trips() {
        let p = Partitioning::Template(PathTemplate::parse("{day:date}.parquet").unwrap());
        let name = p.manifest_name().unwrap();
        assert_eq!(name, "template:{day:date}.parquet");
        assert_eq!(Partitioning::parse(Some(&name)).unwrap(), p);
        assert_ne!(
            Partitioning::parse(Some("template:{day}.parquet")).unwrap(),
            p
        );
        assert!(Partitioning::parse(Some("template:")).is_err());
        assert_eq!(
            Partitioning::parse(Some("hive")).unwrap(),
            Partitioning::Hive
        );
    }

    #[test]
    fn every_non_matching_file_is_counted_and_ten_are_named() {
        let p = Partitioning::Template(PathTemplate::parse("{day:date}.parquet").unwrap());
        let mut paths: Vec<String> = (0..12).map(|i| format!("/r/bad{i:02}.parquet")).collect();
        paths.push("/r/2026-01-02.parquet".into());
        let e = p.values_of_all("/r", &paths).unwrap_err().to_string();
        assert!(e.contains("12 of 13 Parquet files"), "{e}");
        assert!(e.contains("bad09.parquet and 2 more"), "{e}");
        assert!(!e.contains("bad10"), "{e}");
        assert_eq!(p.values_of_all("/r", &paths[12..]).unwrap().len(), 1);
    }

    #[test]
    fn time_values_are_in_the_column_unit() {
        use arrow::datatypes::TimeUnit;
        assert_eq!(
            time_value("d", Some("1970-01-02"), &DataType::Date32).unwrap(),
            Some(1)
        );
        assert_eq!(
            time_value(
                "d",
                Some("1970-01-02"),
                &DataType::Timestamp(TimeUnit::Second, None)
            )
            .unwrap(),
            Some(86_400)
        );
        assert_eq!(
            time_value("n", Some("-3"), &DataType::Int64).unwrap(),
            Some(-3)
        );
        assert_eq!(time_value("d", None, &DataType::Date32).unwrap(), None);
        assert!(time_value("s", Some("x"), &DataType::Utf8).is_err());
    }
}
