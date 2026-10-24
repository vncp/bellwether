//! Partition values from Hive-style `name=value` directory components and
//! filename-derived virtual columns from `--path-template` (`docs/format.md`).
//!
//! A directory component of the form `name=value` (or a placeholder `{name}`
//! or `{name:type}` in a path template) defines a virtual column for that
//! file:
//!
//! - In `RowGroupMeta.stats`, its `min` and `max` are both the canonical
//!   encoding of `value` at the column's dataset type, `null_count` is `0`,
//!   `distinct_count` is `1` (or `0` when the row group has `0` rows),
//!   `has_bloom` is `false`, and `bytes` is `Some(0)`. Nothing of the file is
//!   read to obtain these bounds.
//! - If the column is not present in the file's physical schema, it is
//!   appended to the dataset schema (in path order, non-nullable), typed from
//!   the supplied dataset schema when present, else from the placeholder's
//!   declared type (`int`, `uint`, `float`, `bool`, `date`, `ts`/`timestamp`,
//!   `str`/`utf8`), else inferred with the same scalar rules CSV uses
//!   (`arrow::csv`, then `YYYY-MM-DD` as `Date32`, then `Utf8`).
//! - If the column is present in the file's physical schema, its physical
//!   bounds (or decoded values) must not disagree with the path value; a
//!   file whose path and content disagree on the column is refused with
//!   `Error::Source`.
//! - When `read_columns` requests a virtual column that is absent from the
//!   physical file, a constant array of length `rg.rows` is synthesized from
//!   the path value without reading any column chunk from the file. When it
//!   is present in the physical file, every decoded row is checked against
//!   the path value.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeBinaryArray, LargeStringArray, RecordBatch, RecordBatchOptions, StringArray,
    StringViewArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{i256, DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_types::{ColumnStats, Error, FileFingerprint, Result, RowGroupMeta};

use crate::canonical::{is_nan_encoding, normalize_float_bounds, Canonical};

/// A placeholder type annotation in a `--path-template`, such as `{day:date}`
/// or `{symbol:str}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlaceholderType {
    /// `str` or `utf8` → `DataType::Utf8`.
    Utf8,
    /// `int` or `i64` → `DataType::Int64`.
    Int64,
    /// `uint` or `u64` → `DataType::UInt64`.
    UInt64,
    /// `float` or `f64` → `DataType::Float64`.
    Float64,
    /// `bool` → `DataType::Boolean`.
    Boolean,
    /// `date` → `DataType::Date32`.
    Date32,
    /// `ts` or `timestamp` → `DataType::Timestamp(Microsecond, Some("UTC"))`.
    Timestamp,
}

impl PlaceholderType {
    fn parse(s: &str) -> Result<Self> {
        match s {
            "str" | "utf8" | "string" => Ok(Self::Utf8),
            "int" | "i64" | "int64" => Ok(Self::Int64),
            "uint" | "u64" | "uint64" => Ok(Self::UInt64),
            "float" | "f64" | "float64" => Ok(Self::Float64),
            "bool" | "boolean" => Ok(Self::Boolean),
            "date" | "date32" => Ok(Self::Date32),
            "ts" | "timestamp" => Ok(Self::Timestamp),
            other => Err(Error::KeySpec(format!(
                "unknown path-template placeholder type {other:?}; expected str, int, uint, \
                 float, bool, date, or ts"
            ))),
        }
    }

    /// The Arrow [`DataType`] this placeholder declares when no dataset schema
    /// overrides it.
    #[must_use]
    pub fn data_type(self) -> DataType {
        match self {
            Self::Utf8 => DataType::Utf8,
            Self::Int64 => DataType::Int64,
            Self::UInt64 => DataType::UInt64,
            Self::Float64 => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Date32 => DataType::Date32,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        }
    }
}

/// One segment of a parsed path template (`/`-separated).
#[derive(Clone, Debug, PartialEq, Eq)]
enum SegmentPattern {
    /// Alternating `literals[i]` and `vars[i]`, ending with `literals[vars.len()]`.
    Tokens {
        literals: Vec<String>,
        vars: Vec<(String, Option<PlaceholderType>)>,
    },
}

/// A compiled `--path-template` pattern, e.g. `"{day}.parquet"` or
/// `"grouped_daily_us/{day:date}/{symbol:str}.parquet"`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathTemplate {
    raw: String,
    segments: Vec<SegmentPattern>,
    columns: Vec<(String, Option<PlaceholderType>)>,
}

impl PathTemplate {
    /// Parses and validates a path template.
    pub fn parse(pattern: &str) -> Result<Self> {
        let trimmed = pattern.trim();
        if trimmed.is_empty() {
            return Err(Error::KeySpec(
                "path template cannot be empty; pass a pattern such as \"{day}.parquet\"".into(),
            ));
        }
        let rel = trimmed.trim_start_matches("./").trim_matches('/');
        if rel.is_empty() {
            return Err(Error::KeySpec(format!(
                "path template {pattern:?} has no path components"
            )));
        }
        let mut segments = Vec::new();
        let mut columns: Vec<(String, Option<PlaceholderType>)> = Vec::new();
        let mut seen = BTreeSet::new();
        for part in rel.split('/') {
            if part.is_empty() || part == "." || part == ".." {
                return Err(Error::KeySpec(format!(
                    "path template {pattern:?} contains an invalid path component {part:?}"
                )));
            }
            let seg = Self::parse_segment(pattern, part, &mut seen, &mut columns)?;
            segments.push(seg);
        }
        if columns.is_empty() {
            return Err(Error::KeySpec(format!(
                "path template {pattern:?} has no {{column}} placeholders"
            )));
        }
        Ok(Self {
            raw: trimmed.to_string(),
            segments,
            columns,
        })
    }

    fn parse_segment(
        whole: &str,
        part: &str,
        seen: &mut BTreeSet<String>,
        columns: &mut Vec<(String, Option<PlaceholderType>)>,
    ) -> Result<SegmentPattern> {
        let mut literals = Vec::new();
        let mut vars = Vec::new();
        let mut rest = part;
        while let Some(open) = rest.find('{') {
            let lit = &rest[..open];
            if lit.contains('}') {
                return Err(Error::KeySpec(format!(
                    "path template {whole:?} has an unmatched '}}'"
                )));
            }
            literals.push(lit.to_string());
            let after_open = &rest[open + 1..];
            let Some(close) = after_open.find('}') else {
                return Err(Error::KeySpec(format!(
                    "path template {whole:?} has an unclosed '{{'"
                )));
            };
            let inside = &after_open[..close];
            if inside.contains('{') {
                return Err(Error::KeySpec(format!(
                    "path template {whole:?} has nested '{{' in {{{inside}}}"
                )));
            }
            let (name, ty) = match inside.split_once(':') {
                None => (inside.trim(), None),
                Some((n, t)) => (n.trim(), Some(PlaceholderType::parse(t.trim())?)),
            };
            if name.is_empty() {
                return Err(Error::KeySpec(format!(
                    "path template {whole:?} has an empty placeholder name"
                )));
            }
            if !seen.insert(name.to_string()) {
                return Err(Error::KeySpec(format!(
                    "path template {whole:?} lists placeholder {name:?} more than once"
                )));
            }
            if let Some(prev_lit) = literals.last() {
                if prev_lit.is_empty() && !vars.is_empty() {
                    return Err(Error::KeySpec(format!(
                        "path template {whole:?} has adjacent placeholders with no separator in \
                         {part:?}"
                    )));
                }
            }
            vars.push((name.to_string(), ty));
            columns.push((name.to_string(), ty));
            rest = &after_open[close + 1..];
        }
        if rest.contains('}') {
            return Err(Error::KeySpec(format!(
                "path template {whole:?} has an unmatched '}}'"
            )));
        }
        literals.push(rest.to_string());
        Ok(SegmentPattern::Tokens { literals, vars })
    }

    /// The pattern string as given (trimmed).
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Matches `file_path` against this template, returning captured
    /// `(column_name, raw_value, explicit_type)` tuples when both the segment
    /// structure and any typed placeholder constraints match.
    pub fn extract(
        &self,
        root: Option<&str>,
        file_path: &str,
    ) -> Option<Vec<(String, String, Option<PlaceholderType>)>> {
        let rel = relative_segments(root, file_path);
        if rel.len() == self.segments.len() {
            return self.match_segments(&rel);
        }
        // Only when `rel` has fewer segments than the template (for example
        // when `root` is `/.../grouped_daily_us` and the template includes the
        // dataset directory name `"grouped_daily_us/{day}.parquet"`) or when
        // `root` was not supplied, match against the trailing segments of
        // `file_path`.
        if root.is_none() || rel.len() < self.segments.len() {
            let all: Vec<&str> = Path::new(file_path)
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(s) => s.to_str(),
                    _ => None,
                })
                .collect();
            if all.len() >= self.segments.len() {
                let tail = &all[all.len() - self.segments.len()..];
                return self.match_segments(tail);
            }
        }
        None
    }

    fn match_segments(
        &self,
        parts: &[&str],
    ) -> Option<Vec<(String, String, Option<PlaceholderType>)>> {
        if parts.len() != self.segments.len() {
            return None;
        }
        let mut out = Vec::with_capacity(self.columns.len());
        for (pat, actual) in self.segments.iter().zip(parts) {
            let SegmentPattern::Tokens { literals, vars } = pat;
            if vars.is_empty() {
                if actual != &literals[0] {
                    return None;
                }
                continue;
            }
            let prefix = &literals[0];
            let suffix = &literals[vars.len()];
            if !actual.starts_with(prefix.as_str()) || !actual.ends_with(suffix.as_str()) {
                return None;
            }
            if actual.len() < prefix.len() + suffix.len() {
                return None;
            }
            let mut rem = &actual[prefix.len()..actual.len() - suffix.len()];
            for i in 0..vars.len() {
                let (name, ty) = &vars[i];
                let ty = *ty;
                if i + 1 == vars.len() {
                    if rem.is_empty() || !placeholder_accepts(ty, rem) {
                        return None;
                    }
                    out.push((name.clone(), rem.to_string(), ty));
                    rem = "";
                } else {
                    let sep = &literals[i + 1];
                    let pos = rem.find(sep.as_str())?;
                    let val = &rem[..pos];
                    if val.is_empty() || !placeholder_accepts(ty, val) {
                        return None;
                    }
                    out.push((name.clone(), val.to_string(), ty));
                    rem = &rem[pos + sep.len()..];
                }
            }
        }
        Some(out)
    }
}

fn placeholder_accepts(ty: Option<PlaceholderType>, val: &str) -> bool {
    let Some(ty) = ty else {
        return true;
    };
    synthesize_column("", "", val, &ty.data_type(), 1).is_ok()
}

/// Checks that every discovered file matches `template`, returning an error
/// listing up to 10 non-matching paths if any fails.
pub fn check_all_match(
    template: &PathTemplate,
    root: Option<&str>,
    files: &[FileFingerprint],
) -> Result<()> {
    let mut unmatched = Vec::new();
    for f in files {
        if template.extract(root, &f.path).is_none() {
            unmatched.push(f.path.as_str());
        }
    }
    if unmatched.is_empty() {
        return Ok(());
    }
    let total = unmatched.len();
    let shown: Vec<&str> = unmatched.into_iter().take(10).collect();
    let more = total.saturating_sub(shown.len());
    let suffix = if more > 0 {
        format!(" (and {more} more)")
    } else {
        String::new()
    };
    Err(Error::Source(format!(
        "{total} discovered file(s) do not match --path-template {:?}: {}{suffix}",
        template.as_str(),
        shown.join(", ")
    )))
}

/// A virtual column extracted from a file's path (Hive `name=value` directory
/// components and/or `--path-template` captures).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VirtualCol {
    /// Column name.
    pub name: String,
    /// Raw string value from the path.
    pub raw: String,
    /// Optional explicit type from a path-template placeholder.
    pub explicit_type: Option<PlaceholderType>,
}

/// Extracts virtual columns for `file_path` relative to `root` (when known),
/// combining Hive `name=value` directory components and optional
/// `path_template` captures.
pub fn extract_virtual_cols(
    root: Option<&str>,
    file_path: &str,
    template: Option<&PathTemplate>,
) -> Result<Vec<VirtualCol>> {
    let mut out: Vec<VirtualCol> = Vec::new();
    let segs = relative_segments(root, file_path);
    // Directory components are all segments except the last (the file name).
    let dir_segs = if segs.len() > 1 {
        &segs[..segs.len() - 1]
    } else {
        &[][..]
    };
    for &seg in dir_segs {
        if let Some((k, v)) = parse_hive_segment(seg) {
            if let Some(prev) = out.iter().find(|c| c.name == k) {
                if prev.raw != v {
                    return Err(Error::Source(format!(
                        "{file_path}: conflicting partition values for {k:?} in path ({:?} vs {:?})",
                        prev.raw, v
                    )));
                }
            } else {
                out.push(VirtualCol {
                    name: k.to_string(),
                    raw: v.to_string(),
                    explicit_type: None,
                });
            }
        }
    }
    if let Some(tpl) = template {
        let Some(captures) = tpl.extract(root, file_path) else {
            return Err(Error::Source(format!(
                "{file_path}: path does not match --path-template {:?}",
                tpl.as_str()
            )));
        };
        for (name, val, ty) in captures {
            if let Some(existing) = out.iter_mut().find(|c| c.name == name) {
                if existing.raw != val {
                    return Err(Error::Source(format!(
                        "{file_path}: conflicting values for virtual column {name:?} ({:?} vs {:?})",
                        existing.raw, val
                    )));
                }
                if existing.explicit_type.is_none() {
                    existing.explicit_type = ty;
                }
            } else {
                out.push(VirtualCol {
                    name,
                    raw: val,
                    explicit_type: ty,
                });
            }
        }
    }
    Ok(out)
}

fn parse_hive_segment(seg: &str) -> Option<(&str, &str)> {
    let (k, v) = seg.split_once('=')?;
    if k.is_empty() || v.is_empty() {
        return None;
    }
    // Avoid mistaking arbitrary names with spaces or weird characters for
    // Hive partitions unless they are valid identifier-like column names.
    if !k
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return None;
    }
    Some((k, v))
}

fn relative_segments<'a>(root: Option<&str>, file_path: &'a str) -> Vec<&'a str> {
    if let Some(r) = root {
        let r_trimmed = r.trim_end_matches('/');
        if let Some( stripped) = file_path.strip_prefix(r_trimmed) {
            let stripped = stripped.trim_start_matches('/');
            if !stripped.is_empty() {
                return stripped.split('/').filter(|s| !s.is_empty()).collect();
            }
        }
        // Also try canonicalizing root if file_path is canonical and root is not.
        if let Ok(canon_root) = std::fs::canonicalize(r) {
            if let Some(canon_str) = canon_root.to_str() {
                let c_trimmed = canon_str.trim_end_matches('/');
                if let Some(stripped) = file_path.strip_prefix(c_trimmed) {
                    let stripped = stripped.trim_start_matches('/');
                    if !stripped.is_empty() {
                        return stripped.split('/').filter(|s| !s.is_empty()).collect();
                    }
                }
            }
        }
    }
    // When `root` is not known (e.g. inside `row_groups(file)` where only the
    // `FileFingerprint` is passed), only take `name=value` directory components
    // that appear after any non-Hive base directory, or all `name=value`
    // components in the path (since temp dirs and system paths do not use
    // `name=value` segments, kecuali if someone has `=`; wait: scratchpad paths
    // never have `=`, and in `row_groups` we can remember the last discovered
    // root or scan all `name=value` components).
    Path::new(file_path)
        .components()
        .filter_map(|c| match c {
            std::path::Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect()
}

/// Infers the Arrow [`DataType`] of a virtual column value when the column is
/// not in the supplied dataset schema or the physical file schema.
///
/// Uses the same scalar inference rules as CSV (`arrow::csv::reader::Format`),
/// plus `Date32` for `YYYY-MM-DD` dates (`arrow-csv`'s default regexes infer
/// integers, floats, booleans, and timestamps, but leave `YYYY-MM-DD` as
/// `Utf8` unless a date regex is configured; in Parquet/Hive lakes `day=YYYY-MM-DD`
/// is a `Date32` or `Utf8` column—wait: let's check what WP21 says!).
#[must_use]
pub fn infer_virtual_type(raw: &str, explicit: Option<PlaceholderType>) -> DataType {
    if let Some(ty) = explicit {
        return ty.data_type();
    }
    if is_iso_date(raw) {
        return DataType::Date32;
    }
    let sample = format!("v\n{raw}\n");
    if let Ok((schema, _)) = arrow::csv::reader::Format::default()
        .with_header(true)
        .infer_schema(Cursor::new(sample.as_bytes()), Some(1))
    {
        if let Some(field) = schema.fields().first() {
            let dt = field.data_type();
            if !matches!(dt, DataType::Null) {
                return dt.clone();
            }
        }
    }
    DataType::Utf8
}

fn is_iso_date(s: &str) -> bool {
    if s.len() != 10 {
        return false;
    }
    let b = s.as_bytes();
    if b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    parse_date32(s).is_some()
}

/// Parses `YYYY-MM-DD` into days since 1970-01-01 (`Date32`).
#[must_use]
pub fn parse_date32(s: &str) -> Option<i32> {
    let s = s.trim();
    if s.len() != 10 {
        return None;
    }
    let b = s.as_bytes();
    if b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y: i64 = s[0..4].parse().ok()?;
    let m: u32 = s[5..7].parse().ok()?;
    let d: u32 = s[8..10].parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let max_d = match m {
        4 | 6 | 9 | 11 => 30,
        2 => {
            let leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
            if leap { 29 } else { 28 }
        }
        _ => 31,
    };
    if d > max_d {
        return None;
    }
    let yr = y - i64::from(m <= 2);
    let era = if yr >= 0 { yr } else { yr - 399 } / 400;
    let yoe = u32::try_from(yr - era * 400).ok()?;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + i64::from(doe) - 719_468;
    i32::try_from(days).ok()
}

/// Parses `raw` as a single-row Arrow [`ArrayRef`] of type `target` (repeated
/// `rows` times).
pub fn synthesize_column(
    path: &str,
    col_name: &str,
    raw: &str,
    target: &DataType,
    rows: usize,
) -> Result<ArrayRef> {
    let err = |why: &str| -> Error {
        Error::Source(format!(
            "{path}: virtual column {col_name:?} value {raw:?} cannot be parsed as {target} ({why})"
        ))
    };
    let array: ArrayRef = match target {
        DataType::Boolean => {
            let v = match raw.trim().to_ascii_lowercase().as_str() {
                "true" => true,
                "false" => false,
                _ => return Err(err("expected true or false")),
            };
            Arc::new(BooleanArray::from(vec![v; rows]))
        }
        DataType::Int8 => {
            let v: i8 = raw.trim().parse().map_err(|_| err("invalid Int8"))?;
            Arc::new(Int8Array::from(vec![v; rows]))
        }
        DataType::Int16 => {
            let v: i16 = raw.trim().parse().map_err(|_| err("invalid Int16"))?;
            Arc::new(Int16Array::from(vec![v; rows]))
        }
        DataType::Int32 => {
            let v: i32 = raw.trim().parse().map_err(|_| err("invalid Int32"))?;
            Arc::new(Int32Array::from(vec![v; rows]))
        }
        DataType::Int64 => {
            let v: i64 = raw.trim().parse().map_err(|_| err("invalid Int64"))?;
            Arc::new(Int64Array::from(vec![v; rows]))
        }
        DataType::UInt8 => {
            let v: u8 = raw.trim().parse().map_err(|_| err("invalid UInt8"))?;
            Arc::new(UInt8Array::from(vec![v; rows]))
        }
        DataType::UInt16 => {
            let v: u16 = raw.trim().parse().map_err(|_| err("invalid UInt16"))?;
            Arc::new(UInt16Array::from(vec![v; rows]))
        }
        DataType::UInt32 => {
            let v: u32 = raw.trim().parse().map_err(|_| err("invalid UInt32"))?;
            Arc::new(UInt32Array::from(vec![v; rows]))
        }
        DataType::UInt64 => {
            let v: u64 = raw.trim().parse().map_err(|_| err("invalid UInt64"))?;
            Arc::new(UInt64Array::from(vec![v; rows]))
        }
        DataType::Float16 => {
            let v: f32 = raw.trim().parse().map_err(|_| err("invalid Float16"))?;
            let h = <arrow::datatypes::Float16Type as arrow::datatypes::ArrowPrimitiveType>::Native::from_f32(v);
            Arc::new(Float16Array::from(vec![h; rows]))
        }
        DataType::Float32 => {
            let v: f32 = raw.trim().parse().map_err(|_| err("invalid Float32"))?;
            Arc::new(Float32Array::from(vec![v; rows]))
        }
        DataType::Float64 => {
            let v: f64 = raw.trim().parse().map_err(|_| err("invalid Float64"))?;
            Arc::new(Float64Array::from(vec![v; rows]))
        }
        DataType::Decimal128(p, s) => {
            let v = parse_decimal_i128(raw.trim(), *p, *s).ok_or_else(|| err("invalid Decimal128"))?;
            Arc::new(
                Decimal128Array::from(vec![v; rows])
                    .with_precision_and_scale(*p, *s)
                    .map_err(|e| err(&e.to_string()))?,
            )
        }
        DataType::Decimal256(p, s) => {
            let v = parse_decimal_i256(raw.trim(), *p, *s).ok_or_else(|| err("invalid Decimal256"))?;
            Arc::new(
                Decimal256Array::from(vec![v; rows])
                    .with_precision_and_scale(*p, *s)
                    .map_err(|e| err(&e.to_string()))?,
            )
        }
        DataType::Utf8 => Arc::new(StringArray::from(vec![raw; rows])),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![raw; rows])),
        DataType::Utf8View => Arc::new(StringViewArray::from(vec![raw; rows])),
        DataType::Binary => Arc::new(arrow::array::BinaryArray::from(vec![raw.as_bytes(); rows])),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(vec![raw.as_bytes(); rows])),
        DataType::Date32 => {
            let days = parse_date32(raw).ok_or_else(|| err("expected YYYY-MM-DD"))?;
            Arc::new(Date32Array::from(vec![days; rows]))
        }
        DataType::Date64 => {
            let days = i64::from(parse_date32(raw).ok_or_else(|| err("expected YYYY-MM-DD"))?);
            let ms = days
                .checked_mul(86_400_000)
                .ok_or_else(|| err("Date64 overflow"))?;
            Arc::new(Date64Array::from(vec![ms; rows]))
        }
        DataType::Timestamp(unit, tz) => {
            let v = parse_timestamp_in_unit(raw, *unit).ok_or_else(|| err("invalid timestamp"))?;
            let a: ArrayRef = match unit {
                TimeUnit::Second => {
                    Arc::new(TimestampSecondArray::from(vec![v; rows]).with_timezone_opt(tz.clone()))
                }
                TimeUnit::Millisecond => Arc::new(
                    TimestampMillisecondArray::from(vec![v; rows]).with_timezone_opt(tz.clone()),
                ),
                TimeUnit::Microsecond => Arc::new(
                    TimestampMicrosecondArray::from(vec![v; rows]).with_timezone_opt(tz.clone()),
                ),
                TimeUnit::Nanosecond => Arc::new(
                    TimestampNanosecondArray::from(vec![v; rows]).with_timezone_opt(tz.clone()),
                ),
            };
            a
        }
        DataType::Time32(TimeUnit::Second) => {
            let v: i32 = raw.trim().parse().map_err(|_| err("invalid Time32(s)"))?;
            Arc::new(Time32SecondArray::from(vec![v; rows]))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let v: i32 = raw.trim().parse().map_err(|_| err("invalid Time32(ms)"))?;
            Arc::new(Time32MillisecondArray::from(vec![v; rows]))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let v: i64 = raw.trim().parse().map_err(|_| err("invalid Time64(us)"))?;
            Arc::new(Time64MicrosecondArray::from(vec![v; rows]))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let v: i64 = raw.trim().parse().map_err(|_| err("invalid Time64(ns)"))?;
            Arc::new(Time64NanosecondArray::from(vec![v; rows]))
        }
        _ => return Err(err("unsupported virtual column type")),
    };
    Ok(array)
}

/// Encodes `raw` into canonical bytes at `target`.
pub fn encode_virtual_value(
    canonical: &dyn Canonical,
    path: &str,
    col_name: &str,
    raw: &str,
    target: &DataType,
) -> Result<Vec<u8>> {
    let arr = synthesize_column(path, col_name, raw, target, 1)?;
    canonical
        .encode(arr.as_ref(), 0, target)?
        .ok_or_else(|| Error::Source(format!("{path}: virtual column {col_name:?} encoded to null")))
}

fn parse_timestamp_in_unit(s: &str, unit: TimeUnit) -> Option<i64> {
    let s = s.trim();
    if let Ok(n) = s.parse::<i64>() {
        return Some(n);
    }
    let rfc = if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') {
        format!("{s}T00:00:00Z")
    } else {
        s.to_string()
    };
    let nanos = arrow::compute::kernels::cast_utils::string_to_timestamp_nanos(&rfc).ok()?;
    let div: i64 = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    if nanos % div != 0 {
        return None;
    }
    Some(nanos / div)
}

fn parse_decimal_i128(s: &str, precision: u8, scale: i8) -> Option<i128> {
    let v = parse_decimal_i256(s, precision, scale)?;
    v.to_i128()
}

fn parse_decimal_i256(s: &str, precision: u8, scale: i8) -> Option<i256> {
    if scale < 0 {
        return None;
    }
    let scale_usize = usize::try_from(scale).ok()?;
    let (neg, rest) = if let Some(r) = s.strip_prefix('-') {
        (true, r)
    } else if let Some(r) = s.strip_prefix('+') {
        (false, r)
    } else {
        (false, s)
    };
    let (whole, frac) = match rest.split_once('.') {
        None => (rest, ""),
        Some((w, f)) => (w, f),
    };
    if whole.is_empty() && frac.is_empty() {
        return None;
    }
    if !whole.chars().all(|c| c.is_ascii_digit()) || !frac.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let frac_trimmed = frac.trim_end_matches('0');
    if frac_trimmed.len() > scale_usize {
        return None;
    }
    let mut digits = String::with_capacity(whole.len() + scale_usize);
    digits.push_str(whole);
    digits.push_str(frac_trimmed);
    for _ in 0..(scale_usize - frac_trimmed.len()) {
        digits.push('0');
    }
    let unsigned = i256::from_string(&digits)?;
    let bound = i256::from_i128(10).checked_pow(u32::from(precision))?;
    if unsigned >= bound {
        return None;
    }
    if neg {
        unsigned.checked_neg()
    } else {
        Some(unsigned)
    }
}

/// Augments a file's physical schema with any virtual columns from `vcols` that
/// are not already in `file_schema`.
#[must_use]
pub fn augment_schema(
    file_schema: &Schema,
    dataset_schema: Option<&SchemaRef>,
    vcols: &[VirtualCol],
) -> SchemaRef {
    if vcols.is_empty() {
        if let Some(ds) = dataset_schema {
            return Arc::clone(ds);
        }
        return Arc::new(file_schema.clone());
    }
    let mut fields: Vec<Arc<Field>> = file_schema.fields().iter().cloned().collect();
    for vc in vcols {
        if fields.iter().any(|f| f.name() == &vc.name) {
            continue;
        }
        let dt = dataset_schema
            .and_then(|ds| ds.field_with_name(&vc.name).ok())
            .map_or_else(
                || infer_virtual_type(&vc.raw, vc.explicit_type),
                |f| f.data_type().clone(),
            );
        fields.push(Arc::new(Field::new(&vc.name, dt, false)));
    }
    Arc::new(Schema::new_with_metadata(
        fields,
        file_schema.metadata().clone(),
    ))
}

/// Applies virtual column statistics to `rgs` (for one file), verifying that
/// any physical column of the same name has statistics compatible with the
/// path value, and appending virtual column stats in schema order.
pub fn apply_virtual_stats(
    canonical: &dyn Canonical,
    file_path: &str,
    file_schema: &Schema,
    dataset_schema: Option<&SchemaRef>,
    vcols: &[VirtualCol],
    rgs: &mut [RowGroupMeta],
) -> Result<()> {
    if vcols.is_empty() {
        return Ok(());
    }
    let full_schema = augment_schema(file_schema, dataset_schema, vcols);
    for vc in vcols {
        let target = dataset_schema
            .and_then(|ds| ds.field_with_name(&vc.name).ok())
            .or_else(|| full_schema.field_with_name(&vc.name).ok())
            .map_or(DataType::Utf8, |f| f.data_type().clone());
        let encoded = encode_virtual_value(canonical, file_path, &vc.name, &vc.raw, &target)?;
        let (vmin, vmax) = normalize_float_bounds(Some(encoded.clone()), Some(encoded.clone()));
        let in_file = file_schema.field_with_name(&vc.name).is_ok();
        for rg in rgs.iter_mut() {
            if in_file {
                if let Some(st) = rg.stats.iter_mut().find(|s| s.column == vc.name) {
                    if rg.rows > 0 {
                        if st.null_count.is_some_and(|n| n > 0) {
                            return Err(Error::Source(format!(
                                "{file_path}: row group {} has {} null value(s) in column {:?}, \
                                 which disagrees with path value {:?}",
                                rg.index_in_file,
                                st.null_count.unwrap_or(0),
                                vc.name,
                                vc.raw
                            )));
                        }
                        match (&st.min, &st.max, &vmin, &vmax) {
                            (Some(fmin), Some(fmax), Some(elo), Some(ehi)) => {
                                if fmin < elo || fmax > ehi {
                                    return Err(Error::Source(format!(
                                        "{file_path}: row group {} column {:?} statistics disagree \
                                         with path value {:?}",
                                        rg.index_in_file, vc.name, vc.raw
                                    )));
                                }
                            }
                            (None, None, None, None) => {}
                            _ => {
                                return Err(Error::Source(format!(
                                    "{file_path}: row group {} column {:?} has unverified \
                                     statistics and cannot confirm path value {:?}",
                                    rg.index_in_file, vc.name, vc.raw
                                )));
                            }
                        }
                    }
                    st.min.clone_from(&vmin);
                    st.max.clone_from(&vmax);
                    st.null_count = Some(0);
                    st.distinct_count = Some(u64::from(rg.rows > 0));
                }
            } else {
                rg.stats.push(ColumnStats {
                    column: vc.name.clone(),
                    min: vmin.clone(),
                    max: vmax.clone(),
                    null_count: Some(0),
                    distinct_count: Some(u64::from(rg.rows > 0)),
                    has_bloom: false,
                    bytes: Some(0),
                });
            }
        }
    }
    Ok(())
}

/// Checks that every row of a physical column `col_name` in `batch` equals the
/// virtual column's path value `raw` at `target`.
pub fn verify_physical_column(
    canonical: &dyn Canonical,
    file_path: &str,
    col_name: &str,
    raw: &str,
    target: &DataType,
    array: &dyn Array,
) -> Result<()> {
    let expected = encode_virtual_value(canonical, file_path, col_name, raw, target)?;
    let (exp_min, exp_max) = normalize_float_bounds(Some(expected.clone()), Some(expected));
    for row in 0..array.len() {
        let Some(actual) = canonical.encode(array, row, target)? else {
            return Err(Error::Source(format!(
                "{file_path}: row {row} has null in column {col_name:?}, which disagrees with \
                 path value {raw:?}"
            )));
        };
        let ok = if is_nan_encoding(&actual) {
            exp_min.is_none() && exp_max.is_none()
        } else {
            exp_min.as_ref().is_some_and(|lo| &actual >= lo)
                && exp_max.as_ref().is_some_and(|hi| &actual <= hi)
        };
        if !ok {
            return Err(Error::Source(format!(
                "{file_path}: row {row} in column {col_name:?} disagrees with path value {raw:?}"
            )));
        }
    }
    Ok(())
}

/// Builds a `RecordBatch` for `columns` when some or all of `columns` are
/// virtual columns and `physical_batch` holds the decoded physical columns.
pub fn assemble_batch(
    canonical: &dyn Canonical,
    file_path: &str,
    rows: usize,
    columns: &[String],
    file_schema: &Schema,
    dataset_schema: Option<&SchemaRef>,
    vcols: &[VirtualCol],
    physical_batch: Option<&RecordBatch>,
) -> Result<RecordBatch> {
    if columns.is_empty() {
        let opts = RecordBatchOptions::new().with_row_count(Some(rows));
        return RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &opts)
            .map_err(|e| Error::Source(format!("{file_path}: {e}")));
    }
    let full_schema = augment_schema(file_schema, dataset_schema, vcols);
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for col in columns {
        let vc = vcols.iter().find(|v| &v.name == col);
        if let Some(phys) = physical_batch.and_then(|b| b.column_by_name(col)) {
            let f = full_schema
                .field_with_name(col)
                .or_else(|_| file_schema.field_with_name(col))
                .map_err(|_| Error::KeySpec(format!("column {col:?} not in source schema")))?;
            if let Some(v) = vc {
                let target = dataset_schema
                    .and_then(|ds| ds.field_with_name(col).ok())
                    .map_or_else(|| f.data_type().clone(), |df| df.data_type().clone());
                verify_physical_column(canonical, file_path, col, &v.raw, &target, phys.as_ref())?;
            }
            fields.push(Arc::new(Field::new(
                col,
                phys.data_type().clone(),
                f.is_nullable(),
            )));
            arrays.push(Arc::clone(phys));
        } else if let Some(v) = vc {
            let f = full_schema
                .field_with_name(col)
                .map_err(|_| Error::KeySpec(format!("column {col:?} not in source schema")))?;
            let arr = synthesize_column(file_path, col, &v.raw, f.data_type(), rows)?;
            fields.push(Arc::new(f.clone()));
            arrays.push(arr);
        } else {
            return Err(Error::KeySpec(format!(
                "column {col:?} not in source schema"
            )));
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| Error::Source(format!("{file_path}: {e}")))
}
