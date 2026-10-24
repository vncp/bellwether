//! Build options, the source kind, the observer hook, and how the dataset
//! schema and source description are persisted in the manifest.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_source::{FingerprintPolicy, TextFormat};
use bellwether_store::{ColumnDef, CommitOptions, Extra};
use bellwether_types::{Error, FileFingerprint, Result, RowGroupId};

/// Default cap on the keys one segment accumulates (see [`BuildOptions`]).
pub const DEFAULT_MAX_SEGMENT_KEYS: u64 = 1 << 20;
/// Default cap on the key bytes one segment accumulates.
pub const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
/// Default number of row groups in one unit of parallel work.
pub const DEFAULT_MAX_SEGMENT_ROW_GROUPS: usize = 64;
/// Default number of times a build restarts after a conflicting commit.
pub const DEFAULT_MAX_RESTARTS: u32 = 8;

/// What kind of files the source folder holds. Recorded in the manifest's
/// `source.kind`; a later build with another kind is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceKind {
    /// Parquet files (`*.parquet`); row groups are the files' own.
    Parquet,
    /// Delimited text (`*.csv`); row groups are byte-range chunks.
    Csv {
        /// Field delimiter.
        delimiter: u8,
        /// Whether each file starts with a header row.
        has_header: bool,
    },
    /// JSON lines (`*.jsonl`, `*.ndjson`); row groups are byte-range chunks.
    JsonLines,
}

impl SourceKind {
    /// Comma-separated with a header row.
    #[must_use]
    pub fn csv() -> Self {
        Self::Csv {
            delimiter: b',',
            has_header: true,
        }
    }

    /// The manifest's `source.kind` string: `parquet`, `jsonl`, `csv` (comma
    /// with a header), or `csv;delimiter=<byte>;header=<bool>` for any other
    /// dialect. `docs/format.md` has no field for a CSV dialect, so it is
    /// carried in the kind string, where a change is detected.
    #[must_use]
    pub fn manifest_kind(&self) -> String {
        match *self {
            Self::Parquet => "parquet".into(),
            Self::JsonLines => "jsonl".into(),
            Self::Csv {
                delimiter: b',',
                has_header: true,
            } => "csv".into(),
            Self::Csv {
                delimiter,
                has_header,
            } => format!("csv;delimiter={delimiter};header={has_header}"),
        }
    }

    /// Parses a manifest `source.kind` string written by
    /// [`SourceKind::manifest_kind`]. Anything else is
    /// [`Error::UnknownKind`].
    pub fn parse(kind: &str) -> Result<Self> {
        let unknown = || Error::UnknownKind {
            what: "source kind",
            found: kind.to_string(),
        };
        match kind {
            "parquet" => return Ok(Self::Parquet),
            "jsonl" => return Ok(Self::JsonLines),
            "csv" => return Ok(Self::csv()),
            _ => {}
        }
        let rest = kind.strip_prefix("csv;").ok_or_else(unknown)?;
        let mut delimiter = None;
        let mut has_header = None;
        for part in rest.split(';') {
            match part.split_once('=') {
                Some(("delimiter", v)) => delimiter = Some(v.parse::<u8>().map_err(|_| unknown())?),
                Some(("header", v)) => has_header = Some(v.parse::<bool>().map_err(|_| unknown())?),
                _ => return Err(unknown()),
            }
        }
        match (delimiter, has_header) {
            (Some(delimiter), Some(has_header)) => Ok(Self::Csv {
                delimiter,
                has_header,
            }),
            _ => Err(unknown()),
        }
    }

    /// The text format, or `None` for Parquet.
    pub(crate) fn text_format(self) -> Option<TextFormat> {
        match self {
            Self::Parquet => None,
            Self::Csv {
                delimiter,
                has_header,
            } => Some(TextFormat::Csv {
                delimiter,
                has_header,
            }),
            Self::JsonLines => Some(TextFormat::JsonLines),
        }
    }
}

/// The manifest's `source.fingerprint_policy` string (`docs/format.md`).
pub(crate) fn policy_name(policy: FingerprintPolicy) -> &'static str {
    match policy {
        FingerprintPolicy::Tail64K => "tail64k",
        FingerprintPolicy::Full => "full",
    }
}

/// Parses `source.fingerprint_policy`; absent means the default `tail64k`.
pub(crate) fn parse_policy(name: Option<&str>) -> Result<FingerprintPolicy> {
    match name {
        None | Some("tail64k") => Ok(FingerprintPolicy::Tail64K),
        Some("full") => Ok(FingerprintPolicy::Full),
        Some(other) => Err(Error::UnknownKind {
            what: "fingerprint policy",
            found: other.to_string(),
        }),
    }
}

/// Instrumentation callbacks, for tests and benchmarks. Every method has an
/// empty default. Callbacks run on the build's worker threads.
pub trait BuildObserver: Send + Sync {
    /// The build asked the source for a file's layout (row-group count and,
    /// for text, chunk ranges) to assign ids at first sighting.
    fn file_laid_out(&self, _file: &FileFingerprint) {}

    /// A worker called `Source::row_groups` for `file`.
    fn row_groups_listed(&self, _file: &FileFingerprint) {}

    /// A worker called `Source::read_columns` for row group `id` with
    /// `columns` and got `batch`, which it is about to index. The worker
    /// drops the batch before it reads the next row group.
    fn row_group_read(&self, _id: RowGroupId, _columns: &[String], _batch: &RecordBatch) {}
}

/// Options for [`crate::Builder::build`].
///
/// Memory (ADR 018): each worker holds one row group's key columns at a
/// time plus one open segment per key spec it is building. A segment is
/// finished and uploaded as soon as adding the next row group would take it
/// past `max_segment_keys` keys or `max_segment_bytes` key bytes, counted as
/// the sum over its row groups of each row group's distinct key tuples
/// (distinct `(key, ts)` pairs for temporal specs) and of their canonical
/// bytes. Those sums bound what the writer holds from above. A single row
/// group larger than the cap gets a segment of its own. The build-side bound
/// is therefore `threads × (one row group's key columns + specs × max(cap,
/// one row group))`.
#[derive(Clone)]
pub struct BuildOptions {
    /// The kind of files under the source root.
    pub source: SourceKind,
    /// Which bytes a fingerprint hashes. `None` keeps the policy recorded in
    /// the manifest, or the default `Tail64K` for a new index. Only
    /// [`FingerprintPolicy::Full`] detects an in-place patch outside the last
    /// 64 KiB that keeps the size and mtime (ADR 013). A policy different
    /// from the manifest's is refused, because it changes every file's
    /// identity.
    pub fingerprint_policy: Option<FingerprintPolicy>,
    /// Chunk size for text files seen for the first time. `None` uses the
    /// chunk size the manifest's text files record, or 64 MiB for a new
    /// index. Files already tracked are always read at the chunk size
    /// recorded for them (`docs/format.md`).
    pub chunk_size: Option<u64>,
    /// The dataset schema: the single source of column types (ADR 016).
    /// `None` keeps the schema recorded in the manifest, or infers one from
    /// the first file for a new index. A key column whose type differs from
    /// the manifest's rebuilds that key spec's segments.
    pub schema: Option<SchemaRef>,
    /// Worker threads. `None` uses the available parallelism.
    pub threads: Option<usize>,
    /// Cap on the keys one segment accumulates (see the type docs).
    pub max_segment_keys: u64,
    /// Cap on the key bytes one segment accumulates.
    pub max_segment_bytes: u64,
    /// Row groups in one unit of parallel work. Work is split at file
    /// boundaries where possible, independently of the thread count, so the
    /// segments a build writes do not depend on it.
    pub max_segment_row_groups: usize,
    /// How many times a build restarts from discovery after a concurrent
    /// commit changed the row-group space, the schema, or the source, or
    /// after a garbage collector claimed one of its uploaded segments.
    pub max_restarts: u32,
    /// Whether key specs the manifest declares but `key_specs` does not
    /// list are removed (their segments dropped, their completeness
    /// cleared). Off by default: a build keeps every declared spec and
    /// indexes new files for it, so a caller that names only the spec it
    /// cares about never destroys the others. See also
    /// [`crate::Builder::remove_spec`].
    pub remove_unlisted: bool,
    /// Retry policy of the manifest commit.
    pub commit: CommitOptions,
    /// Directories under the root whose files are never data: the index
    /// location when it lies inside the data root, whose own `.json` files
    /// would otherwise pass for JSON lines. Absolute, as the root's files
    /// are listed. Text sources only.
    pub exclude: Vec<String>,
    /// Instrumentation hook.
    pub observer: Option<Arc<dyn BuildObserver>>,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            source: SourceKind::Parquet,
            fingerprint_policy: None,
            chunk_size: None,
            schema: None,
            threads: None,
            max_segment_keys: DEFAULT_MAX_SEGMENT_KEYS,
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            max_segment_row_groups: DEFAULT_MAX_SEGMENT_ROW_GROUPS,
            max_restarts: DEFAULT_MAX_RESTARTS,
            remove_unlisted: false,
            commit: CommitOptions::default(),
            exclude: Vec::new(),
            observer: None,
        }
    }
}

impl fmt::Debug for BuildOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuildOptions")
            .field("source", &self.source)
            .field("fingerprint_policy", &self.fingerprint_policy)
            .field("chunk_size", &self.chunk_size)
            .field("schema", &self.schema.as_ref().map(|s| s.fields().len()))
            .field("threads", &self.threads)
            .field("max_segment_keys", &self.max_segment_keys)
            .field("max_segment_bytes", &self.max_segment_bytes)
            .field("max_segment_row_groups", &self.max_segment_row_groups)
            .field("max_restarts", &self.max_restarts)
            .field("remove_unlisted", &self.remove_unlisted)
            .field("exclude", &self.exclude)
            .field("observer", &self.observer.is_some())
            .finish_non_exhaustive()
    }
}

impl BuildOptions {
    /// Default options for `source`.
    #[must_use]
    pub fn new(source: SourceKind) -> Self {
        Self {
            source,
            ..Self::default()
        }
    }

    pub(crate) fn thread_count(&self) -> usize {
        self.threads
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from))
            .max(1)
    }
}

/// Options for [`crate::Builder::compact`].
#[derive(Clone, Copy, Debug)]
pub struct CompactOptions {
    /// Largest merge, as the summed size of its input segments. Merge holds
    /// its inputs and its output in memory, so this bounds compaction's
    /// memory at about twice the value.
    pub max_output_bytes: u64,
    /// Most segments merged into one.
    pub max_inputs: usize,
    /// Retry policy of the manifest commit.
    pub commit: CommitOptions,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            max_output_bytes: 256 * 1024 * 1024,
            max_inputs: 64,
            commit: CommitOptions::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Schema persistence
// ---------------------------------------------------------------------------

/// The manifest columns for `schema`: name, Arrow `Display` type, nullable.
/// Extra fields of an existing column of the same name are kept (ADR 017).
pub(crate) fn schema_to_columns(schema: &Schema, old: &[ColumnDef]) -> Vec<ColumnDef> {
    let extras: BTreeMap<&str, &Extra> = old.iter().map(|c| (c.name.as_str(), &c.extra)).collect();
    schema
        .fields()
        .iter()
        .map(|f| ColumnDef {
            name: f.name().clone(),
            data_type: f.data_type().to_string(),
            nullable: f.is_nullable(),
            extra: extras
                .get(f.name().as_str())
                .map_or_else(Extra::new, |e| (*e).clone()),
        })
        .collect()
}

/// Parses manifest columns back into an Arrow schema. A type string Arrow
/// cannot parse, or a duplicate column name, is an error.
///
/// Nested columns (structs, lists, maps, unions, run-end encoding) are
/// recorded by their Arrow spelling like any other column, so a dataset
/// with one can be indexed on its flat columns. Only a key column needs a
/// canonical encoding, and the build refuses a nested key column by name
/// (`plan_specs`). Before the 2026-10-24 Arrow upgrade every nested column
/// was refused here, implicitly: Arrow 53 printed a nested type with its
/// fields' debug form, which its own parser could not read.
pub(crate) fn columns_to_schema(columns: &[ColumnDef]) -> Result<SchemaRef> {
    let mut seen = std::collections::BTreeSet::new();
    let fields = columns
        .iter()
        .map(|c| {
            if !seen.insert(c.name.as_str()) {
                return Err(Error::KeySpec(format!(
                    "the dataset schema lists column {:?} twice",
                    c.name
                )));
            }
            let dt = parse_manifest_type(&c.data_type).map_err(|e| {
                Error::Other(format!(
                    "the manifest records column {:?} with the type `{}`, which this build of \
                     Arrow cannot read ({e}); the manifest was written by a build this one \
                     cannot read, so re-index the folder with this build",
                    c.name, c.data_type
                ))
            })?;
            Ok(Field::new(c.name.clone(), dt, c.nullable))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// A manifest column type: Arrow's `Display` form as this build prints it,
/// or the form Arrow 53 printed (manifests written before the 2026-10-24
/// dependency upgrade).
///
/// Arrow 59 changed the spelling of time units: `Timestamp(Millisecond,
/// Some("UTC"))` became `Timestamp(ms, "UTC")`, `Time64(Microsecond)` became
/// `Time64(µs)`, and `Timestamp(Second, None)` became `Timestamp(s)`. Arrow
/// 59's own parser still accepts the old form; this reader does not rely on
/// that, and rewrites the old form itself when Arrow refuses it, so an old
/// manifest stays readable even if a later Arrow drops the old grammar.
/// Every other type the manifest can hold is spelled the same by both.
pub(crate) fn parse_manifest_type(spelled: &str) -> std::result::Result<DataType, String> {
    match DataType::from_str(spelled) {
        Ok(dt) => Ok(dt),
        Err(e) => match legacy_type_spelling(spelled) {
            Some(rewritten) => DataType::from_str(&rewritten).map_err(|e2| format!("{e}; {e2}")),
            None => Err(e.to_string()),
        },
    }
}

/// The current spelling of an Arrow 53 temporal type spelling, or `None`
/// when `spelled` is not one. Covers `Time32`, `Time64`, `Timestamp` and
/// `Duration`, the types whose display form changed, and a `Dictionary`
/// whose value type is one of them, at any depth: Arrow 53 spelled
/// `Dictionary(Int32, Timestamp(Millisecond, Some("UTC")))`.
pub(crate) fn legacy_type_spelling(spelled: &str) -> Option<String> {
    let (head, rest) = spelled.split_once('(')?;
    if head == "Dictionary" {
        // The key is an integer type, spelled without commas or parentheses.
        let (key, value) = rest.strip_suffix(')')?.split_once(", ")?;
        if key.contains('(') {
            return None;
        }
        return Some(format!(
            "Dictionary({key}, {})",
            legacy_type_spelling(value)?
        ));
    }
    if !matches!(head, "Time32" | "Time64" | "Timestamp" | "Duration") {
        return None;
    }
    let inner = rest.strip_suffix(')')?;
    let (unit, tz) = match inner.split_once(", ") {
        Some((u, tz)) => (u, Some(tz)),
        None => (inner, None),
    };
    let unit = match unit {
        "Second" => "s",
        "Millisecond" => "ms",
        "Microsecond" => "µs",
        "Nanosecond" => "ns",
        _ => return None,
    };
    match (head, tz) {
        (_, None) => Some(format!("{head}({unit})")),
        ("Timestamp", Some("None")) => Some(format!("Timestamp({unit})")),
        ("Timestamp", Some(tz)) => {
            let name = tz.strip_prefix("Some(")?.strip_suffix(')')?;
            Some(format!("Timestamp({unit}, {name})"))
        }
        _ => None,
    }
}

/// `schema` reduced to what the manifest records (names, types,
/// nullability), checked to survive the round trip through the manifest's
/// type strings exactly. The build always works with this form, so the first
/// build and every later one see the same schema.
pub(crate) fn persistable(schema: &Schema) -> Result<SchemaRef> {
    let columns = schema_to_columns(schema, &[]);
    for (f, c) in schema.fields().iter().zip(&columns) {
        let back = parse_manifest_type(&c.data_type);
        if back.as_ref().ok() != Some(f.data_type()) {
            let read = match back {
                Ok(b) => format!("it reads back as `{b}`"),
                Err(e) => format!("Arrow cannot read its spelling back: {e}"),
            };
            return Err(Error::Other(format!(
                "column {:?} has a type the manifest cannot record exactly ({read}); index a \
                 copy of the data without that column, or with the column's nested field \
                 metadata removed if it has any (Arrow's type spelling does not carry it)",
                f.name(),
            )));
        }
    }
    columns_to_schema(&columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{IntervalUnit, TimeUnit};

    #[test]
    fn source_kinds_round_trip_through_the_manifest_string() {
        for kind in [
            SourceKind::Parquet,
            SourceKind::JsonLines,
            SourceKind::csv(),
            SourceKind::Csv {
                delimiter: b';',
                has_header: false,
            },
            SourceKind::Csv {
                delimiter: b'\t',
                has_header: true,
            },
        ] {
            assert_eq!(SourceKind::parse(&kind.manifest_kind()).unwrap(), kind);
        }
        assert_eq!(SourceKind::csv().manifest_kind(), "csv");
        for bad in [
            "",
            "orc",
            "csv;",
            "csv;delimiter=300;header=true",
            "csv;header=true",
        ] {
            assert!(
                matches!(SourceKind::parse(bad), Err(Error::UnknownKind { .. })),
                "{bad}"
            );
        }
    }

    #[test]
    fn fingerprint_policies_round_trip() {
        for p in [FingerprintPolicy::Tail64K, FingerprintPolicy::Full] {
            assert_eq!(parse_policy(Some(policy_name(p))).unwrap(), p);
        }
        assert_eq!(parse_policy(None).unwrap(), FingerprintPolicy::Tail64K);
        assert!(parse_policy(Some("tail1m")).is_err());
    }

    /// The manifest spells types with Arrow's `Display`, which depends on the
    /// Arrow version; this pins the round trip for the types an index keys
    /// on, so an Arrow upgrade that changes the spelling fails here.
    #[test]
    fn persisted_types_round_trip_exactly() {
        let types = [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Decimal128(38, -3),
            DataType::Decimal256(76, 10),
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(16),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+05:30".into())),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into())),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ];
        let schema = Schema::new(
            types
                .iter()
                .enumerate()
                .map(|(i, t)| Field::new(format!("c{i}"), t.clone(), i % 2 == 0))
                .collect::<Vec<_>>(),
        );
        let back = persistable(&schema).unwrap();
        assert_eq!(back.fields().len(), types.len());
        for (f, t) in back.fields().iter().zip(&types) {
            assert_eq!(f.data_type(), t);
        }
        assert_eq!(
            schema_to_columns(&schema, &[])[1].data_type,
            "Int8",
            "the spelling is Arrow's Display form"
        );
    }

    /// Every type `persisted_types_round_trip_exactly` pins, with the
    /// spelling Arrow 53 wrote into manifests before the 2026-10-24 upgrade
    /// (captured from arrow-schema 53.4.1's `Display`).
    fn arrow53_spellings() -> Vec<(&'static str, DataType)> {
        let tz = |s: &str| Some(s.into());
        vec![
            ("Boolean", DataType::Boolean),
            ("Int8", DataType::Int8),
            ("Int16", DataType::Int16),
            ("Int32", DataType::Int32),
            ("Int64", DataType::Int64),
            ("UInt8", DataType::UInt8),
            ("UInt16", DataType::UInt16),
            ("UInt32", DataType::UInt32),
            ("UInt64", DataType::UInt64),
            ("Float16", DataType::Float16),
            ("Float32", DataType::Float32),
            ("Float64", DataType::Float64),
            ("Decimal128(10, 2)", DataType::Decimal128(10, 2)),
            ("Decimal128(38, -3)", DataType::Decimal128(38, -3)),
            ("Decimal256(76, 10)", DataType::Decimal256(76, 10)),
            ("Utf8", DataType::Utf8),
            ("LargeUtf8", DataType::LargeUtf8),
            ("Utf8View", DataType::Utf8View),
            ("Binary", DataType::Binary),
            ("LargeBinary", DataType::LargeBinary),
            ("BinaryView", DataType::BinaryView),
            ("FixedSizeBinary(16)", DataType::FixedSizeBinary(16)),
            ("Date32", DataType::Date32),
            ("Date64", DataType::Date64),
            ("Time32(Second)", DataType::Time32(TimeUnit::Second)),
            (
                "Time32(Millisecond)",
                DataType::Time32(TimeUnit::Millisecond),
            ),
            (
                "Time64(Microsecond)",
                DataType::Time64(TimeUnit::Microsecond),
            ),
            ("Time64(Nanosecond)", DataType::Time64(TimeUnit::Nanosecond)),
            (
                "Timestamp(Second, None)",
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                r#"Timestamp(Millisecond, Some("UTC"))"#,
                DataType::Timestamp(TimeUnit::Millisecond, tz("UTC")),
            ),
            (
                r#"Timestamp(Microsecond, Some("+05:30"))"#,
                DataType::Timestamp(TimeUnit::Microsecond, tz("+05:30")),
            ),
            (
                r#"Timestamp(Nanosecond, Some("America/New_York"))"#,
                DataType::Timestamp(TimeUnit::Nanosecond, tz("America/New_York")),
            ),
            (
                "Dictionary(Int32, Utf8)",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            ),
            (
                r#"Dictionary(Int32, Timestamp(Millisecond, Some("UTC")))"#,
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Timestamp(TimeUnit::Millisecond, tz("UTC"))),
                ),
            ),
            (
                "Dictionary(Int16, Time64(Nanosecond))",
                DataType::Dictionary(
                    Box::new(DataType::Int16),
                    Box::new(DataType::Time64(TimeUnit::Nanosecond)),
                ),
            ),
            (
                "Interval(MonthDayNano)",
                DataType::Interval(IntervalUnit::MonthDayNano),
            ),
        ]
    }

    /// Manifests written by the old release stay readable: each old
    /// spelling reads back as the type it named, both through the manifest
    /// reader and through the legacy rewrite alone (so the guarantee does not
    /// rest on Arrow's parser continuing to accept the old grammar).
    #[test]
    fn arrow_53_spellings_read_back_as_the_same_types() {
        for (old, want) in arrow53_spellings() {
            assert_eq!(parse_manifest_type(old).unwrap(), want, "{old}");
            if let Some(rewritten) = legacy_type_spelling(old) {
                assert_eq!(DataType::from_str(&rewritten).unwrap(), want, "{old}");
                assert_eq!(rewritten, want.to_string(), "the current spelling");
            } else {
                assert_eq!(old, want.to_string(), "{old} is spelled as before");
            }
        }
        let columns: Vec<ColumnDef> = arrow53_spellings()
            .iter()
            .enumerate()
            .map(|(i, (old, _))| ColumnDef {
                name: format!("c{i}"),
                data_type: (*old).to_string(),
                nullable: true,
                extra: Extra::new(),
            })
            .collect();
        let schema = columns_to_schema(&columns).unwrap();
        for (f, (old, want)) in schema.fields().iter().zip(arrow53_spellings()) {
            assert_eq!(f.data_type(), &want, "{old}");
        }
    }

    /// Nested and field-carrying columns are recorded by their Arrow
    /// spelling and read back as the same type (defect 1 of the 2026-10-24
    /// real-data run: they were refused, so no dataset with one could be
    /// indexed). One whose nested fields carry metadata, which the spelling
    /// drops, is refused with a sentence naming the column.
    #[test]
    fn nested_columns_round_trip_through_the_manifest() {
        use arrow::datatypes::{Fields, UnionFields, UnionMode};
        let item = |t: DataType| Arc::new(Field::new("item", t, true));
        let nested = [
            DataType::new_list(DataType::Int64, true),
            DataType::LargeList(item(DataType::Utf8)),
            DataType::FixedSizeList(item(DataType::Float32), 3),
            DataType::ListView(item(DataType::Int64)),
            DataType::LargeListView(item(DataType::Int64)),
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, false),
            ])),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Int64, true),
                    ])),
                    false,
                )),
                false,
            ),
            DataType::Union(
                UnionFields::try_new(vec![0], vec![Field::new("a", DataType::Int64, true)])
                    .unwrap(),
                UnionMode::Dense,
            ),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                item(DataType::Utf8),
            ),
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::new_list(DataType::Utf8, true)),
            ),
        ];
        for t in nested {
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("x", t.clone(), true),
            ]);
            let back = persistable(&schema).unwrap_or_else(|e| panic!("{t}: {e}"));
            assert_eq!(back.field(1).data_type(), &t);
            let columns = schema_to_columns(&schema, &[]);
            let read = columns_to_schema(&columns).unwrap();
            assert_eq!(read.field(1).data_type(), &t);
        }
        let tagged = Field::new("id", DataType::Int64, true).with_metadata(
            [("PARQUET:field_id".to_string(), "7".to_string())]
                .into_iter()
                .collect(),
        );
        let schema = Schema::new(vec![Field::new(
            "repo",
            DataType::Struct(Fields::from(vec![tagged])),
            true,
        )]);
        let err = persistable(&schema).unwrap_err().to_string();
        assert!(err.contains("column \"repo\""), "{err}");
        assert!(!err.contains("Field {"), "no Debug output: {err}");
    }

    #[test]
    fn the_legacy_rewrite_is_exact_and_refuses_what_it_does_not_know() {
        assert_eq!(
            legacy_type_spelling("Duration(Millisecond)").as_deref(),
            Some("Duration(ms)")
        );
        for not_legacy in [
            "Int64",
            "Timestamp(µs)",
            "Timestamp(Fortnight, None)",
            "Timestamp(Second, Some(UTC)",
            "Time64(Microsecond, None)",
            "Duration(Second, None)",
            "List(Int64)",
            "Dictionary(Int32, Utf8)",
            "Dictionary(Int32, Timestamp(Fortnight, None))",
            "Dictionary(Timestamp(Second, None), Utf8)",
        ] {
            assert_eq!(legacy_type_spelling(not_legacy), None, "{not_legacy}");
        }
        assert!(parse_manifest_type("Timestamp(Fortnight, None)").is_err());
        assert!(parse_manifest_type("NotAType").is_err());
    }

    #[test]
    fn column_extras_survive_a_schema_rewrite_and_duplicates_are_refused() {
        let mut old = schema_to_columns(
            &Schema::new(vec![Field::new("a", DataType::Int64, false)]),
            &[],
        );
        old[0].extra.insert("future".into(), serde_json_value());
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let cols = schema_to_columns(&schema, &old);
        assert_eq!(cols[0].data_type, "Int32");
        assert!(cols[0].extra.contains_key("future"));
        assert!(cols[1].extra.is_empty());
        let mut dup = cols.clone();
        dup[1].name = "a".into();
        assert!(columns_to_schema(&dup).is_err());
        let mut bad = cols;
        bad[1].data_type = "NotAType".into();
        assert!(columns_to_schema(&bad).is_err());
    }

    fn serde_json_value() -> serde_json::Value {
        serde_json::Value::Bool(true)
    }
}
