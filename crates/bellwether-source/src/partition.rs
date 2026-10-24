//! Hive-style partition values from paths (`docs/format.md`, "Partition
//! columns").
//!
//! Under hive partitioning, every directory component of a file's path
//! *below the data root* of the form `name=value` is a virtual column of
//! that file: every row of the file has `value` in column `name`. The source
//! reports it as a column with `min = max = value` in each row group's
//! statistics and synthesizes it in `read_columns`, so the profiler sees it
//! and the planner prunes on it without reading any data page.
//!
//! The rules, each a refusal rather than a guess:
//!
//! - Only components below the root count, so a root such as
//!   `/lake/env=prod/trades` never injects `env`. A file not under the root
//!   is refused. The file name itself never counts.
//! - A component is split at its first `=`. An empty name, an empty value,
//!   or a name repeated within one path is refused.
//! - Values are percent-decoded (`%2F` is `/`), as Hive writes them; a `%`
//!   not followed by two hex digits, or a decoded value that is not UTF-8,
//!   is refused. `__HIVE_DEFAULT_PARTITION__` is a null.
//! - Every file under one root has the same partition columns in the same
//!   order (checked at discovery).
//! - A value is typed by the dataset schema when the column is there (an
//!   exact Arrow cast; a value that does not cast is refused). Without a
//!   dataset schema it is inferred by [`infer_type`], a fixed table that is
//!   part of the format (it never follows a dependency's inference).
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Partitioning {
    /// No partition columns: paths carry no data. The default, and what an
    /// index that records no scheme means.
    #[default]
    None,
    /// Hive-style `name=value` directories are virtual columns.
    Hive,
}

impl Partitioning {
    /// The manifest's `source.partitioning` value: `None` for no scheme (the
    /// field is then omitted), `"hive"` for hive.
    #[must_use]
    pub fn manifest_name(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Hive => Some("hive"),
        }
    }

    /// Parses `source.partitioning`; absent means [`Partitioning::None`].
    /// Anything else is [`Error::UnknownKind`].
    pub fn parse(name: Option<&str>) -> Result<Self> {
        match name {
            None => Ok(Self::None),
            Some("hive") => Ok(Self::Hive),
            Some(other) => Err(Error::UnknownKind {
                what: "partitioning",
                found: other.to_string(),
            }),
        }
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
}
