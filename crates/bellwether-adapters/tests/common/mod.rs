//! The adapters' test corpus, brute-force plans, the reference residual
//! evaluator, and the engine runners.
//!
//! The corpus is a small hive-partitioned folder of Parquet files written
//! with Arrow, with every type a key can have, both float zeros and NaN,
//! decimals at two scales, timestamps in two units with nulls, ties at a
//! key's newest timestamp, an all-null key row group, a tombstone column,
//! a list column, and a text column. A plan is computed by brute force
//! over the decoded rows (the row groups holding a queried key, with the
//! residual the planner writes), and the rows a lookup must return are
//! the reference evaluator's: the residual applied over the planned row
//! groups' rows, over canonical bytes.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

pub mod range_server;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
    Int64Array, ListBuilder, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMillisecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_adapters::{ColumnDef, Partitions, RowOffsets};
use bellwether_core::encoding::{encode_nullable_as, encode_value_as, KeyColumns, Value};
use bellwether_types::{FileFingerprint, Hash, Predicate, ScanEntry, ScanPlan, TextOp};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Rows per row group.
pub const RG_ROWS: usize = 40;
/// Row groups per file.
pub const RGS: usize = 3;
/// The expression the corpus's `ExprIn` tests use.
pub const EXPR: &str = "lower(sym)";

/// The file columns, in order (the partition column `region` is last in
/// the dataset schema).
pub fn file_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("sym", DataType::Utf8, true),
        Field::new("f", DataType::Float64, true),
        Field::new("d2", DataType::Decimal128(10, 2), true),
        Field::new("d4", DataType::Decimal128(20, 4), true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("ts_ms", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("day", DataType::Date32, true),
        Field::new("u", DataType::UInt32, true),
        Field::new("void", DataType::Boolean, true),
        Field::new("bin", DataType::Binary, true),
        Field::new("body", DataType::Utf8, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]))
}

/// The dataset schema: the file columns and the hive column `region`.
pub fn dataset() -> Vec<ColumnDef> {
    let mut out: Vec<ColumnDef> = file_schema()
        .fields()
        .iter()
        .map(|f| ColumnDef::new(f.name(), f.data_type().clone()))
        .collect();
    out.push(ColumnDef::new("region", DataType::Utf8));
    out
}

/// The flat columns rows are compared on (every column but the list).
pub fn flat_columns() -> Vec<String> {
    dataset()
        .into_iter()
        .filter(|c| !matches!(c.data_type, DataType::List(_)))
        .map(|c| c.name)
        .collect()
}

/// One row, decoded: canonical bytes per flat column (`None` for null),
/// the list's canonical elements, the text column, and the expression's
/// value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    /// Index of the file in [`Corpus::files`].
    pub file: usize,
    /// Row group in the file.
    pub rg: u32,
    /// Row number in the file.
    pub row: u64,
    /// Flat columns.
    pub cols: BTreeMap<String, Option<Vec<u8>>>,
    /// The list column's elements (`None` for a null list; a null element
    /// is `None`).
    pub tags: Option<Vec<Option<Vec<u8>>>>,
    /// The text column.
    pub body: Option<String>,
    /// [`EXPR`]'s value, canonical.
    pub expr: Option<Vec<u8>>,
}

impl Row {
    /// A flat column.
    pub fn get(&self, c: &str) -> Option<&Vec<u8>> {
        self.cols.get(c).and_then(Option::as_ref)
    }

    /// The canonical tuple over `columns`, `None` if any is null.
    pub fn tuple(&self, columns: &[String]) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        for c in columns {
            out.extend_from_slice(self.get(c)?);
        }
        Some(out)
    }

    /// The row as the engines' output is compared: every output column
    /// rendered from canonical bytes.
    pub fn render(&self, columns: &[String]) -> Vec<String> {
        columns
            .iter()
            .map(|c| {
                if c == "tags" {
                    render_list(self.tags.as_ref())
                } else {
                    render_bytes(self.cols.get(c).and_then(Option::as_ref))
                }
            })
            .collect()
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn render_bytes(b: Option<&Vec<u8>>) -> String {
    b.map_or_else(|| "null".into(), |b| hex(b))
}

fn render_list(l: Option<&Vec<Option<Vec<u8>>>>) -> String {
    match l {
        None => "null".into(),
        Some(items) => format!(
            "[{}]",
            items
                .iter()
                .map(|e| render_bytes(e.as_ref()))
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

/// One written file.
#[derive(Clone, Debug)]
pub struct CorpusFile {
    /// Absolute path.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Rows per row group.
    pub rg_rows: Vec<u64>,
    /// Compressed bytes per row group (the sum of its column chunks).
    pub rg_bytes: Vec<u64>,
    /// The hive value of `region` (`None` for Hive's null).
    pub region: Option<String>,
}

/// The written corpus and its decoded rows.
#[derive(Clone, Debug)]
pub struct Corpus {
    /// The data root.
    pub root: PathBuf,
    /// The files, in path order.
    pub files: Vec<CorpusFile>,
    /// Every row of every file.
    pub rows: Vec<Row>,
}

const FLOATS: [f64; 7] = [0.0, -0.0, 1.5, f64::NAN, -2.25, f64::INFINITY, 0.1];
const SYMS: [&str; 6] = ["AAPL", "o'k", "Straße", "\"q\"", "msft", "ÉTÉ"];
const BODIES: [&str; 5] = [
    "Quarterly STRASSE report",
    "guidance raised",
    "restated guidance",
    "nothing",
    "Straße and more",
];

/// The rows of file `fi`'s row group `rg`, as arrays.
fn batch(fi: usize, rg: usize) -> RecordBatch {
    let n = RG_ROWS;
    let g = |r: usize| (fi * RGS + rg) * n + r;
    // File 1's last row group holds no key at all: an all-null row group.
    let all_null = fi == 1 && rg == RGS - 1;
    let id: Int64Array = (0..n)
        .map(|r| (!all_null).then(|| i64::try_from(g(r) * 7 % 23).unwrap()))
        .collect();
    let sym: StringArray = (0..n)
        .map(|r| (g(r) % 17 != 0).then(|| SYMS[g(r) % SYMS.len()]))
        .collect();
    let f: Float64Array = (0..n).map(|r| Some(FLOATS[g(r) % FLOATS.len()])).collect();
    let d2 = Decimal128Array::from(
        (0..n)
            .map(|r| Some(i128::try_from(g(r) % 13).unwrap() * 125 - 700))
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(10, 2)
    .unwrap();
    let d4 = Decimal128Array::from(
        (0..n)
            .map(|r| Some((i128::try_from(g(r) % 11).unwrap() - 5) * 12_345))
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(20, 4)
    .unwrap();
    // Five timestamps per key cycle, so a key's newest timestamp is held
    // by several rows (ties); every 11th is null.
    let base = 1_704_067_200_000_000i64;
    let ts = TimestampMicrosecondArray::from(
        (0..n)
            .map(|r| (g(r) % 11 != 3).then(|| base + i64::try_from(g(r) % 5).unwrap() * 1_000_001))
            .collect::<Vec<_>>(),
    )
    .with_timezone("UTC");
    let ts_ms = TimestampMillisecondArray::from(
        (0..n)
            .map(|r| (g(r) % 13 != 5).then(|| 1_700_000_000_000 + i64::try_from(g(r) % 4).unwrap() * 997))
            .collect::<Vec<_>>(),
    );
    let day: Date32Array = (0..n)
        .map(|r| Some(19_700 + i32::try_from(g(r) % 6).unwrap()))
        .collect();
    let u: UInt32Array = (0..n).map(|r| Some(u32::try_from(g(r) % 9).unwrap() * 1000)).collect();
    let void: BooleanArray = (0..n)
        .map(|r| (g(r) % 19 != 2).then(|| g(r) % 4 == 0))
        .collect();
    let bin = BinaryArray::from_iter((0..n).map(|r| {
        (g(r) % 5 != 1).then(|| vec![0u8, u8::try_from(g(r) % 7).unwrap(), 0xff])
    }));
    let body: StringArray = (0..n)
        .map(|r| (g(r) % 9 != 4).then(|| BODIES[g(r) % BODIES.len()]))
        .collect();
    let mut tags = ListBuilder::new(StringBuilder::new());
    for r in 0..n {
        match g(r) % 6 {
            0 => tags.append_null(),
            1 => tags.append(true),
            k => {
                for j in 0..k {
                    if (g(r) + j) % 7 == 3 {
                        tags.values().append_null();
                    } else {
                        tags.values().append_value(format!("t{}", (g(r) + j) % 8));
                    }
                }
                tags.append(true);
            }
        }
    }
    RecordBatch::try_new(
        file_schema(),
        vec![
            Arc::new(id),
            Arc::new(sym),
            Arc::new(f),
            Arc::new(d2),
            Arc::new(d4),
            Arc::new(ts),
            Arc::new(ts_ms),
            Arc::new(day),
            Arc::new(u),
            Arc::new(void),
            Arc::new(bin),
            Arc::new(body),
            Arc::new(tags.finish()),
        ],
    )
    .unwrap()
}

/// The hive directory of each file.
const REGIONS: [Option<&str>; 4] = [Some("eu"), Some("eu"), Some("us"), None];

/// Writes the corpus under `dir` and decodes it.
pub fn corpus(dir: &Path) -> Corpus {
    let mut files = Vec::new();
    let mut rows = Vec::new();
    for (fi, region) in REGIONS.iter().enumerate() {
        let sub = match region {
            Some(r) => format!("region={r}"),
            None => "region=__HIVE_DEFAULT_PARTITION__".into(),
        };
        let d = dir.join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let path = d.join(format!("part-{fi}.parquet"));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(RG_ROWS))
            .build();
        let mut w = ArrowWriter::try_new(File::create(&path).unwrap(), file_schema(), Some(props)).unwrap();
        for rg in 0..RGS {
            w.write(&batch(fi, rg)).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
        let path_s = path.canonicalize().unwrap().display().to_string();
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        let md = reader.metadata().clone();
        let rg_rows: Vec<u64> = md.row_groups().iter().map(|g| g.num_rows() as u64).collect();
        let rg_bytes: Vec<u64> = md
            .row_groups()
            .iter()
            .map(|g| g.columns().iter().map(|c| c.byte_range().1).sum())
            .collect();
        let mut row_no = 0u64;
        for (rg, n) in rg_rows.iter().enumerate() {
            let b = batch(fi, rg);
            assert_eq!(b.num_rows() as u64, *n);
            rows.extend(decode_rows(&b, fi, u32::try_from(rg).unwrap(), row_no, *region));
            row_no += n;
        }
        files.push(CorpusFile {
            size: std::fs::metadata(&path).unwrap().len(),
            path: path_s,
            rg_rows,
            rg_bytes,
            region: region.map(str::to_string),
        });
    }
    Corpus {
        root: dir.canonicalize().unwrap(),
        files,
        rows,
    }
}

fn decode_rows(b: &RecordBatch, fi: usize, rg: u32, first: u64, region: Option<&str>) -> Vec<Row> {
    let schema = dataset();
    let arrow_schema = bellwether_adapters::residual::arrow_schema(&schema);
    let typed = bellwether_core::expr::Typed::parse(EXPR, &arrow_schema).unwrap();
    let exprs = typed
        .encode_rows(&RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("sym", DataType::Utf8, true)])),
            vec![Arc::clone(b.column_by_name("sym").unwrap())],
        )
        .unwrap())
        .unwrap();
    let tags = bellwether_core::list::ListElements::new(b.column_by_name("tags").unwrap()).unwrap();
    let enc = tags.encoder(&DataType::Utf8).unwrap();
    let body = b.column_by_name("body").unwrap();
    let body = body.as_any().downcast_ref::<StringArray>().unwrap();
    (0..b.num_rows())
        .map(|r| {
            let mut cols = BTreeMap::new();
            for f in file_schema().fields() {
                if matches!(f.data_type(), DataType::List(_)) {
                    continue;
                }
                let a = b.column_by_name(f.name()).unwrap();
                let mut out = Vec::new();
                let v = if a.is_null(r) {
                    None
                } else {
                    encode_nullable_as(a.as_ref(), r, f.data_type(), &mut out).unwrap();
                    Some(out)
                };
                cols.insert(f.name().clone(), v);
            }
            cols.insert(
                "region".into(),
                region.map(|v| {
                    let mut out = Vec::new();
                    encode_value_as(&Value::Utf8(v), &DataType::Utf8, &mut out).unwrap();
                    out
                }),
            );
            let list = tags.row(r).map(|range| {
                range
                    .map(|i| {
                        let mut out = Vec::new();
                        enc.encode_into(i, &mut out).unwrap().then_some(out)
                    })
                    .collect()
            });
            Row {
                file: fi,
                rg,
                row: first + r as u64,
                cols,
                tags: list,
                body: (!body.is_null(r)).then(|| body.value(r).to_string()),
                expr: exprs[r].clone(),
            }
        })
        .collect()
}

/// Canonical bytes of one value at `dt`.
pub fn enc(v: Value<'_>, dt: &DataType) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_as(&v, dt, &mut out).unwrap();
    out
}

/// The dataset type of a column.
pub fn dt(c: &str) -> DataType {
    dataset().into_iter().find(|x| x.name == c).unwrap().data_type
}

/// Both encodings of a float zero, or the value alone.
pub fn zero_variants(k: &[u8]) -> Vec<Vec<u8>> {
    let pos = enc(Value::Float(0.0), &DataType::Float64);
    let neg = enc(Value::Float(-0.0), &DataType::Float64);
    if k == pos || k == neg {
        vec![neg, pos]
    } else {
        vec![k.to_vec()]
    }
}

/// A plan reading `rgs` (`(file, row group)` pairs) with `residual`.
pub fn plan(c: &Corpus, rgs: &BTreeSet<(usize, u32)>, residual: Option<Predicate>) -> ScanPlan {
    let mut entries = Vec::new();
    for (fi, f) in c.files.iter().enumerate() {
        let mine: roaring::RoaringBitmap = rgs
            .iter()
            .filter(|(i, _)| *i == fi)
            .map(|(_, rg)| *rg)
            .collect();
        if mine.is_empty() {
            continue;
        }
        let bytes = mine.iter().map(|rg| f.rg_bytes[rg as usize]).sum();
        entries.push(ScanEntry {
            file: FileFingerprint {
                path: f.path.clone(),
                size: f.size,
                etag: None,
                tail_blake3: Hash([0; 32]),
            },
            row_groups: mine,
            bytes,
            columns: None,
        });
    }
    let total: u64 = c.files.iter().map(|f| f.rg_rows.len() as u64).sum();
    ScanPlan {
        manifest_version: "1".into(),
        bytes: entries.iter().map(|e| e.bytes).sum(),
        row_groups_selected: rgs.len() as u64,
        entries,
        residual,
        row_groups_total: total,
        projected_bytes: None,
        order: None,
        rows: None,
    }
}

/// Every `(file, row group)` holding a row `pred` holds for.
pub fn holding(c: &Corpus, pred: impl Fn(&Row) -> bool) -> BTreeSet<(usize, u32)> {
    c.rows.iter().filter(|r| pred(r)).map(|r| (r.file, r.rg)).collect()
}

/// The rows of the plan's row groups.
pub fn planned_rows(c: &Corpus, plan: &ScanPlan) -> Vec<Row> {
    let index: BTreeMap<&str, usize> = c
        .files
        .iter()
        .enumerate()
        .map(|(i, f)| (f.path.as_str(), i))
        .collect();
    let rgs: BTreeSet<(usize, u32)> = plan
        .entries
        .iter()
        .flat_map(|e| {
            let fi = index[e.file.path.as_str()];
            e.row_groups.iter().map(move |rg| (fi, rg))
        })
        .collect();
    c.rows.iter().filter(|r| rgs.contains(&(r.file, r.rg))).cloned().collect()
}

/// The reference evaluator over canonical bytes, with SQL's nulls: a null
/// satisfies no comparison and is in no list, and a null key or timestamp
/// never wins `LatestPerKey` (every row at the newest timestamp does).
pub fn apply(pred: &Predicate, rows: Vec<Row>) -> Vec<Row> {
    match pred {
        Predicate::In { columns, values } => {
            let set: BTreeSet<&Vec<u8>> = values.iter().collect();
            rows.into_iter()
                .filter(|r| r.tuple(columns).is_some_and(|t| set.contains(&t)))
                .collect()
        }
        Predicate::Between { column, lo, hi } => rows
            .into_iter()
            .filter(|r| r.get(column).is_some_and(|v| lo <= v && v <= hi))
            .collect(),
        Predicate::NotOlderThan {
            columns,
            ts_column,
            bounds,
        } => {
            let bounds: BTreeMap<&Vec<u8>, &Vec<u8>> = bounds.iter().map(|(k, t)| (k, t)).collect();
            rows.into_iter()
                .filter(|r| match r.tuple(columns).and_then(|k| bounds.get(&k).copied()) {
                    None => true,
                    Some(b) => r.get(ts_column).is_some_and(|t| t >= b),
                })
                .collect()
        }
        Predicate::LatestPerKey { columns, ts_column } => {
            let mut best: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            for r in &rows {
                if let (Some(k), Some(t)) = (r.tuple(columns), r.get(ts_column)) {
                    let e = best.entry(k).or_insert_with(|| t.clone());
                    if t > e {
                        e.clone_from(t);
                    }
                }
            }
            rows.into_iter()
                .filter(|r| match (r.tuple(columns), r.get(ts_column)) {
                    (Some(k), Some(t)) => best.get(&k) == Some(t),
                    _ => false,
                })
                .collect()
        }
        Predicate::And { all } => all.iter().fold(rows, |rows, p| apply(p, rows)),
        Predicate::Text {
            column,
            operator,
            values,
        } => {
            assert_eq!(column, "body");
            rows.into_iter()
                .filter(|r| {
                    r.body
                        .as_deref()
                        .is_some_and(|v| values.iter().any(|a| bellwether_core::text::matches(*operator, a, v)))
                })
                .collect()
        }
        Predicate::ExprIn { values, .. } => rows
            .into_iter()
            .filter(|r| r.expr.as_ref().is_some_and(|v| values.contains(v)))
            .collect(),
        Predicate::ContainsAny { values, .. } => rows
            .into_iter()
            .filter(|r| {
                r.tags
                    .as_ref()
                    .is_some_and(|l| l.iter().flatten().any(|e| values.contains(e)))
            })
            .collect(),
        Predicate::ContainsAll { values, .. } => rows
            .into_iter()
            .filter(|r| {
                r.tags.as_ref().is_some_and(|l| {
                    values.iter().all(|g| l.iter().flatten().any(|e| g.contains(e)))
                })
            })
            .collect(),
        Predicate::Live { column, tombstone } => rows
            .into_iter()
            .filter(|r| r.get(column).is_some_and(|v| v != tombstone))
            .collect(),
        other => panic!("not a lookup residual: {other:?}"),
    }
}

/// The rows `plan` must return, rendered and sorted.
pub fn expected(c: &Corpus, plan: &ScanPlan, columns: &[String]) -> Vec<Vec<String>> {
    let rows = planned_rows(c, plan);
    let rows = match &plan.residual {
        Some(p) => apply(p, rows),
        None => rows,
    };
    let mut out: Vec<Vec<String>> = rows.iter().map(|r| r.render(columns)).collect();
    out.sort();
    out
}

/// Engine output batches rendered as [`Row::render`] does, sorted. Each
/// column is re-encoded at its dataset type, so a value an engine returned
/// at another unit, scale, or width compares only if it converts exactly.
pub fn rendered(batches: &[RecordBatch], columns: &[String]) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for b in batches {
        for r in 0..b.num_rows() {
            let row = columns
                .iter()
                .map(|c| {
                    let a = b
                        .column_by_name(c)
                        .unwrap_or_else(|| panic!("output lacks {c}: {:?}", b.schema()));
                    if c == "tags" {
                        if a.is_null(r) {
                            return "null".into();
                        }
                        let l = bellwether_core::list::ListElements::new(a).unwrap();
                        let e = l.encoder(&DataType::Utf8).unwrap();
                        let items: Vec<Option<Vec<u8>>> = l
                            .row(r)
                            .unwrap()
                            .map(|i| {
                                let mut out = Vec::new();
                                e.encode_into(i, &mut out).unwrap().then_some(out)
                            })
                            .collect();
                        render_list(Some(&items))
                    } else if a.is_null(r) {
                        "null".into()
                    } else {
                        let mut out = Vec::new();
                        encode_nullable_as(a.as_ref(), r, &dt(c), &mut out)
                            .unwrap_or_else(|e| panic!("{c}: {e}"));
                        hex(&out)
                    }
                })
                .collect();
            out.push(row);
        }
    }
    out.sort();
    out
}

/// The partition values of the corpus's files.
pub fn partitions(c: &Corpus) -> Partitions {
    Partitions {
        scheme: "hive".into(),
        columns: vec!["region".into()],
        files: c
            .files
            .iter()
            .map(|f| (f.path.clone(), vec![f.region.clone()]))
            .collect(),
    }
}

/// The corpus's row counts.
pub fn row_offsets(c: &Corpus) -> RowOffsets {
    RowOffsets(c.files.iter().map(|f| (f.path.clone(), f.rg_rows.clone())).collect())
}

/// The canonical tuples of `columns` over every row, nulls dropped.
pub fn tuples(c: &Corpus, columns: &[String]) -> BTreeSet<Vec<u8>> {
    c.rows.iter().filter_map(|r| r.tuple(columns)).collect()
}

/// A `locate` plan: the row groups holding any of `keys` (each a tuple
/// over `columns`, both float zeros of a zero listed), with an `In`.
pub fn locate(c: &Corpus, columns: &[&str], keys: &[Vec<u8>]) -> ScanPlan {
    let columns: Vec<String> = columns.iter().map(|s| (*s).to_string()).collect();
    let set: BTreeSet<Vec<u8>> = keys.iter().cloned().collect();
    let rgs = holding(c, |r| r.tuple(&columns).is_some_and(|t| set.contains(&t)));
    plan(
        c,
        &rgs,
        Some(Predicate::In {
            columns,
            values: set.into_iter().collect(),
        }),
    )
}

/// A `latest` (or `asof` at `at`) plan over `key` with timestamp `ts`, as
/// the planner writes it: per key its newest timestamp (at or before
/// `at`) over every row, the row groups holding a row at it (every tie),
/// `In` over keys with a version, `Between` for asof, `NotOlderThan`,
/// `LatestPerKey`, then `Live` on `void` when `live`.
pub fn latest(c: &Corpus, key: &str, ts: &str, keys: &[Vec<u8>], at: Option<&Vec<u8>>, live: bool) -> ScanPlan {
    let cols = vec![key.to_string()];
    // As the planner: a queried key's winner is over all its variants
    // (both signs of a float zero), and every variant gets its bound.
    let class = |k: &Vec<u8>| zero_variants(k).pop().unwrap();
    let mut newest: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for r in &c.rows {
        let (Some(k), Some(t)) = (r.tuple(&cols), r.get(ts)) else { continue };
        if !keys.contains(&k) || at.is_some_and(|a| t > a) {
            continue;
        }
        let e = newest.entry(class(&k)).or_insert_with(|| t.clone());
        if t > e {
            e.clone_from(t);
        }
    }
    let listed: BTreeMap<Vec<u8>, Vec<u8>> = keys
        .iter()
        .filter_map(|k| newest.get(&class(k)).map(|t| (k.clone(), t.clone())))
        .collect();
    let rgs = holding(c, |r| {
        r.tuple(&cols)
            .zip(r.get(ts))
            .is_some_and(|(k, t)| listed.get(&k) == Some(t))
    });
    let mut all = vec![Predicate::In {
        columns: cols.clone(),
        values: listed.keys().cloned().collect(),
    }];
    if let (Some(a), Some(lo)) = (at, listed.values().min()) {
        all.push(Predicate::Between {
            column: ts.into(),
            lo: lo.clone(),
            hi: a.clone(),
        });
    }
    all.push(Predicate::NotOlderThan {
        columns: cols.clone(),
        ts_column: ts.into(),
        bounds: listed.iter().map(|(k, t)| (k.clone(), t.clone())).collect(),
    });
    all.push(Predicate::LatestPerKey {
        columns: cols,
        ts_column: ts.into(),
    });
    if live {
        all.push(Predicate::Live {
            column: "void".into(),
            tombstone: enc(Value::Boolean(true), &DataType::Boolean),
        });
    }
    plan(c, &rgs, Some(Predicate::And { all }))
}

/// A text plan: row groups with a matching row, and the `Text` residual.
pub fn text(c: &Corpus, op: TextOp, args: &[&str]) -> ScanPlan {
    let values: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
    let rgs = holding(c, |r| {
        r.body
            .as_deref()
            .is_some_and(|v| values.iter().any(|a| bellwether_core::text::matches(op, a, v)))
    });
    plan(
        c,
        &rgs,
        Some(Predicate::Text {
            column: "body".into(),
            operator: op,
            values,
        }),
    )
}

/// Engine output for comparison: the columns of the dataset, in order.
pub fn all_columns() -> Vec<String> {
    dataset().into_iter().map(|c| c.name).collect()
}

/// A Parquet file of one Int64 key column `id`, holding `keys`.
pub fn keys_file(path: &Path, keys: &[i64]) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let b = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
}

/// Reads a Parquet file into batches.
pub fn read_parquet(path: &Path) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// Canonical Int64 keys.
pub fn ids(keys: &[i64]) -> Vec<Vec<u8>> {
    keys.iter().map(|k| enc(Value::Int(*k), &DataType::Int64)).collect()
}

/// `KeyColumns` over a batch's `columns` at the dataset types, for tests
/// that encode engine-side keys.
pub fn key_columns(b: &RecordBatch, columns: &[&str]) -> KeyColumns {
    let arrays: Vec<ArrayRef> = columns.iter().map(|c| Arc::clone(b.column_by_name(c).unwrap())).collect();
    let types: Vec<DataType> = columns.iter().map(|c| dt(c)).collect();
    KeyColumns::new(&arrays, &types).unwrap()
}

// --- DuckDB ---------------------------------------------------------------

/// The DuckDB CLI the bench uses (`$BELLWETHER_DUCKDB` or `duckdb`), if it
/// runs; otherwise the reason, for the skip message.
pub fn duckdb() -> Result<String, String> {
    let bin = std::env::var("BELLWETHER_DUCKDB").unwrap_or_else(|_| "duckdb".into());
    match std::process::Command::new(&bin)
        .args(["-no-agent", "--version"])
        .env_remove("AI_AGENT")
        .output()
    {
        Ok(o) if o.status.success() => Ok(bin),
        Ok(o) => Err(format!("`{bin} -no-agent --version` failed: {}", String::from_utf8_lossy(&o.stderr))),
        Err(e) => Err(format!("no `{bin}` CLI ({e}); install DuckDB or set BELLWETHER_DUCKDB")),
    }
}

/// Runs `script` in the DuckDB CLI exactly as the bench does
/// (`-no-agent -init /dev/null`, `AI_AGENT` removed); any stderr output or
/// a failure is an error.
pub fn duckdb_run(bin: &str, script: &str) -> Result<String, String> {
    use std::io::Write as _;
    let mut child = std::process::Command::new(bin)
        .args(["-no-agent", "-init", "/dev/null", "-csv", "-noheader"])
        .env_remove("AI_AGENT")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .map_err(|e| e.to_string())?;
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    if !out.status.success() || !stderr.is_empty() {
        return Err(format!("duckdb exited {} with stderr {stderr:?}", out.status));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

/// Runs the statement `sql` in DuckDB, writing its rows to a Parquet file,
/// and reads them back.
pub fn duckdb_rows(bin: &str, sql: &str, dir: &Path, setup: &str) -> Vec<RecordBatch> {
    let out = dir.join(format!("duckdb-out-{}.parquet", std::process::id()));
    let _ = std::fs::remove_file(&out);
    let script = format!(
        "{setup}COPY ({sql}\n) TO '{}' (FORMAT parquet);\n",
        out.display().to_string().replace('\'', "''")
    );
    duckdb_run(bin, &script).unwrap_or_else(|e| panic!("{e}\nSQL:\n{sql}"));
    let rows = read_parquet(&out);
    let _ = std::fs::remove_file(&out);
    rows
}

// --- Cases ----------------------------------------------------------------

/// A bitemporal plan (valid time `ts`, known time `ts_ms`) for `keys` on
/// `id`, valid at or before `valid` and known at or before `known`, as the
/// planner writes it: `In`, both clocks bounded, `NotOlderThan` on each,
/// then `LatestPerKey` on valid time. The plan reads every row group
/// holding a key (a superset, which the contract allows).
pub fn bitemporal(c: &Corpus, keys: &[Vec<u8>], valid: &Vec<u8>, known: &Vec<u8>) -> ScanPlan {
    let cols = vec!["id".to_string()];
    let mut winners: BTreeMap<Vec<u8>, (Vec<u8>, Vec<u8>)> = BTreeMap::new();
    for r in &c.rows {
        let (Some(k), Some(v), Some(kn)) = (r.tuple(&cols), r.get("ts"), r.get("ts_ms")) else {
            continue;
        };
        if !keys.contains(&k) || v > valid || kn > known {
            continue;
        }
        let e = winners.entry(k).or_insert_with(|| (v.clone(), kn.clone()));
        if (v, kn) > (&e.0, &e.1) {
            *e = (v.clone(), kn.clone());
        }
    }
    let rgs = holding(c, |r| r.tuple(&cols).is_some_and(|k| winners.contains_key(&k)));
    let (lo_v, lo_k) = (
        winners.values().map(|w| w.0.clone()).min(),
        winners.values().map(|w| w.1.clone()).min(),
    );
    let mut all = vec![Predicate::In {
        columns: cols.clone(),
        values: winners.keys().cloned().collect(),
    }];
    if let (Some(lv), Some(lk)) = (lo_v, lo_k) {
        all.push(Predicate::Between {
            column: "ts".into(),
            lo: lv,
            hi: valid.clone(),
        });
        all.push(Predicate::Between {
            column: "ts_ms".into(),
            lo: lk,
            hi: known.clone(),
        });
    }
    all.push(Predicate::NotOlderThan {
        columns: cols.clone(),
        ts_column: "ts".into(),
        bounds: winners.iter().map(|(k, w)| (k.clone(), w.0.clone())).collect(),
    });
    all.push(Predicate::NotOlderThan {
        columns: cols.clone(),
        ts_column: "ts_ms".into(),
        bounds: winners.iter().map(|(k, w)| (k.clone(), w.1.clone())).collect(),
    });
    all.push(Predicate::LatestPerKey {
        columns: cols,
        ts_column: "ts".into(),
    });
    plan(c, &rgs, Some(Predicate::And { all }))
}

/// Every lookup shape the planner emits, over the corpus, by name. Text
/// and expression residuals are `sql_exact == false`: only DataFusion
/// evaluates them; DuckDB and Spark refuse them by type.
pub struct Case {
    /// Name.
    pub name: &'static str,
    /// The plan.
    pub plan: ScanPlan,
    /// Whether SQL engines (DuckDB, Spark) can run it exactly.
    pub sql_exact: bool,
    /// Whether Spark reads every type it touches (no nanosecond, time,
    /// unsigned 64-bit, or float16 column).
    pub spark: bool,
}

/// The cases.
pub fn cases(c: &Corpus) -> Vec<Case> {
    let s = |v: &str| enc(Value::Utf8(v), &DataType::Utf8);
    let f = |v: f64| enc(Value::Float(v), &DataType::Float64);
    let d = |v: i128, sc: i8, t: &DataType| enc(Value::Decimal128 { value: v, scale: sc }, t);
    let ts = |us: i64| enc(Value::Timestamp { value: us, unit: TimeUnit::Microsecond }, &dt("ts"));
    let ms = |m: i64| enc(Value::Timestamp { value: m, unit: TimeUnit::Millisecond }, &dt("ts_ms"));
    let base = 1_704_067_200_000_000i64;
    let mut out = vec![
        Case { name: "locate int64", plan: locate(c, &["id"], &ids(&[0, 5, 22, 999])), sql_exact: true, spark: true },
        Case { name: "locate float zeros and NaN", plan: locate(c, &["f"], &[f(-0.0), f(0.0), f(f64::NAN), f(1.5)]), sql_exact: true, spark: true },
        Case { name: "locate decimal scale 2", plan: locate(c, &["d2"], &[d(-575, 2, &dt("d2")), d(-7, 0, &dt("d2"))]), sql_exact: true, spark: true },
        Case { name: "locate decimal scale 4", plan: locate(c, &["d4"], &[d(-61_725, 4, &dt("d4")), d(0, 0, &dt("d4"))]), sql_exact: true, spark: true },
        Case { name: "locate timestamp us utc", plan: locate(c, &["ts"], &[ts(base), ts(base + 3_000_003)]), sql_exact: true, spark: true },
        Case { name: "locate timestamp ms naive", plan: locate(c, &["ts_ms"], &[ms(1_700_000_000_997)]), sql_exact: true, spark: true },
        Case { name: "locate strings quoted unicode", plan: locate(c, &["sym"], &[s("o'k"), s("Straße"), s("\"q\"")]), sql_exact: true, spark: true },
        Case { name: "locate binary", plan: locate(c, &["bin"], &[enc(Value::Binary(&[0, 3, 0xff]), &DataType::Binary)]), sql_exact: true, spark: true },
        Case { name: "locate uint32", plan: locate(c, &["u"], &[enc(Value::UInt(4000), &DataType::UInt32)]), sql_exact: true, spark: true },
        Case { name: "locate date", plan: locate(c, &["day"], &[enc(Value::Date32(19_702), &DataType::Date32)]), sql_exact: true, spark: true },
        Case { name: "locate partition column (hive null matches nothing)", plan: locate(c, &["region"], &[s("us")]), sql_exact: true, spark: true },
        Case { name: "locate nothing found", plan: locate(c, &["id"], &ids(&[12_345])), sql_exact: true, spark: true },
        Case { name: "latest with ties and null timestamps", plan: latest(c, "id", "ts", &ids(&[1, 2, 3, 4, 5, 6, 7]), None, false), sql_exact: true, spark: true },
        Case { name: "asof", plan: latest(c, "id", "ts", &ids(&[1, 2, 3, 8, 9]), Some(&ts(base + 2_000_002)), false), sql_exact: true, spark: true },
        Case { name: "latest live (latest then live)", plan: latest(c, "id", "ts", &ids(&(0..23).collect::<Vec<_>>()), None, true), sql_exact: true, spark: true },
        Case { name: "latest on a float key keeps zero signs apart", plan: latest(c, "f", "ts", &[f(-0.0), f(0.0), f(f64::NAN)], None, false), sql_exact: true, spark: true },
        Case { name: "bitemporal", plan: bitemporal(c, &ids(&[1, 4, 9, 16]), &ts(base + 3_000_003), &ms(1_700_000_001_994)), sql_exact: true, spark: true },
        Case { name: "text has token (full case folding)", plan: text(c, TextOp::HasToken, &["strasse"]), sql_exact: false, spark: false },
        Case { name: "text contains", plan: text(c, TextOp::Contains, &["GUIDANCE r"]), sql_exact: false, spark: false },
    ];
    // Composite prefix: (sym, id) keyed, looked up by sym alone.
    out.push(Case { name: "composite prefix", plan: locate(c, &["sym"], &[s("AAPL")]), sql_exact: true, spark: true });
    let pairs: Vec<Vec<u8>> = tuples(c, &["sym".into(), "id".into()]).into_iter().step_by(5).collect();
    out.push(Case { name: "composite key", plan: locate(c, &["sym", "id"], &pairs), sql_exact: true, spark: true });
    // A window: keys whose rows lie in [lo, hi] on ts.
    let mut w = locate(c, &["id"], &ids(&[3, 6, 9]));
    if let Some(Predicate::In { columns, values }) = w.residual.take() {
        let (lo, hi) = (ts(base + 1_000_001), ts(base + 3_000_003));
        let set: BTreeSet<Vec<u8>> = values.iter().cloned().collect();
        let rgs = holding(c, |r| {
            r.tuple(&columns).is_some_and(|k| set.contains(&k))
                && r.get("ts").is_some_and(|t| *t >= lo && *t <= hi)
        });
        w = plan(c, &rgs, Some(Predicate::And { all: vec![Predicate::In { columns, values }, Predicate::Between { column: "ts".into(), lo, hi }] }));
    }
    out.push(Case { name: "window", plan: w, sql_exact: true, spark: true });
    // A lookback: In then a per-key lower bound, no ranking.
    let mut lb = latest(c, "id", "ts", &ids(&[2, 4, 6]), None, false);
    if let Some(Predicate::And { all }) = lb.residual.take() {
        let keep: Vec<Predicate> = all
            .into_iter()
            .filter_map(|p| match p {
                Predicate::NotOlderThan { columns, ts_column, bounds } => Some(Predicate::NotOlderThan {
                    columns,
                    ts_column,
                    bounds: bounds.into_iter().map(|(k, _)| (k, ts(base + 1_000_001))).collect(),
                }),
                p @ Predicate::In { .. } => Some(p),
                _ => None,
            })
            .collect();
        let set: BTreeSet<Vec<u8>> = ids(&[2, 4, 6]).into_iter().collect();
        let rgs = holding(c, |r| r.tuple(&["id".into()]).is_some_and(|k| set.contains(&k)));
        lb = plan(c, &rgs, Some(Predicate::And { all: keep }));
    }
    out.push(Case { name: "lookback (per-key lower bound)", plan: lb, sql_exact: true, spark: true });
    // Expression.
    let ev = vec![s("aapl"), s("straße"), s("été")];
    let set: BTreeSet<Vec<u8>> = ev.iter().cloned().collect();
    let rgs = holding(c, |r| r.expr.as_ref().is_some_and(|v| set.contains(v)));
    out.push(Case {
        name: "expression in",
        plan: plan(c, &rgs, Some(Predicate::ExprIn { expr: EXPR.into(), values: ev })),
        sql_exact: false,
        spark: false,
    });
    // List membership.
    let any = vec![s("t1"), s("t6")];
    let set: BTreeSet<Vec<u8>> = any.iter().cloned().collect();
    let rgs = holding(c, |r| r.tags.as_ref().is_some_and(|l| l.iter().flatten().any(|e| set.contains(e))));
    out.push(Case {
        name: "list contains any",
        plan: plan(c, &rgs, Some(Predicate::ContainsAny { column: "tags".into(), values: any })),
        sql_exact: true,
        spark: true,
    });
    let groups = vec![vec![s("t2")], vec![s("t3")]];
    let rgs = holding(c, |r| r.tags.as_ref().is_some_and(|l| groups.iter().all(|g| l.iter().flatten().any(|e| g.contains(e)))));
    out.push(Case {
        name: "list contains all",
        plan: plan(c, &rgs, Some(Predicate::ContainsAll { column: "tags".into(), values: groups })),
        sql_exact: true,
        spark: true,
    });
    out
}
