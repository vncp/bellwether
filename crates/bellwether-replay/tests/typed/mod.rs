//! A typed corpus (nulls, signed float zeros, NaN, decimals at two file
//! scales under one dataset scale, u64/u32 values above the signed range,
//! strings with NUL, embedded bloom filters on every key column of every
//! even file), an oracle that reads the Parquet files with the `parquet`
//! crate only and compares typed values with SQL equality (no bellwether
//! code, no canonical encoding), and footer byte sums read independently of
//! bellwether-source. Adopted from the PR 18 review's probes.

#![allow(
    dead_code,
    missing_docs,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::missing_panics_doc
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnPath;

pub struct Rng(pub u64);
impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

pub const I64S: [i64; 7] = [-5, 0, 7, 11, i64::MIN, i64::MAX, 42];
pub const STRS: [&str; 6] = ["a", "b", "a\0b", "", "zz", "\u{e9}"];
pub const U64S: [u64; 5] = [1, 2, 9, (1 << 63) + 5, u64::MAX];
pub const U32S: [u32; 4] = [1, 7, (1 << 31) + 3, u32::MAX];
/// Decimal domain in thousandths. Scale-2 files only hold multiples of 10.
pub const DEC_MILLI_S2: [i128; 5] = [1500, -1000, 0, 2250, 990_000];
pub const DEC_MILLI_S3: [i128; 7] = [1500, -1000, 0, 2250, 990_000, 1501, 123_457];

pub fn dataset_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k_i64", DataType::Int64, true),
        Field::new("k_f64", DataType::Float64, true),
        Field::new("k_dec", DataType::Decimal128(20, 3), true),
        Field::new("k_str", DataType::Utf8, true),
        Field::new("k_u64", DataType::UInt64, true),
        Field::new("k_u32", DataType::UInt32, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("pad", DataType::Utf8, false),
    ]))
}

fn file_schema(dec: (u8, i8)) -> SchemaRef {
    let mut fields: Vec<Field> = dataset_schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields[2] = Field::new("k_dec", DataType::Decimal128(dec.0, dec.1), true);
    Arc::new(Schema::new(fields))
}

pub struct Corpus {
    pub dir: tempfile::TempDir,
    pub data: String,
}

impl Corpus {
    pub fn files(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        collect(Path::new(&self.data), &mut out);
        out.sort();
        out
    }
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            collect(&p, out);
        } else if p.extension().is_some_and(|x| x == "parquet") {
            out.push(p);
        }
    }
}

/// `files` files of `rgs` row groups of `rows` rows. Even files store
/// `k_dec` as Decimal128(10,2), odd files as Decimal128(12,3). Bulk values
/// come from domains of about 300 values, so a value sits in few row groups;
/// special values (float zeros, NaN, u64 >= 2^63, u32 >= 2^31, decimals that
/// need scale 3, strings with NUL, extreme integers) are planted in chosen
/// row groups only (see `SPECIALS`). Embedded bloom filters on `k_i64`,
/// `k_u64`, `k_u32`, `k_str`, `k_f64`, `k_dec` in even files. About one value in nine is null
/// in every key column, and `(k_i64 = 0, k_str = null)` is planted.
pub fn write_corpus(files: usize, rgs: usize, rows: usize, seed: u64) -> Corpus {
    assert!(files >= 6 && rgs >= 3 && rows >= 8);
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let mut rng = Rng(seed);
    for f in 0..files {
        let dec = if f % 2 == 0 { (10u8, 2i8) } else { (12u8, 3i8) };
        let schema = file_schema(dec);
        let n = rgs * rows;
        let null = |rng: &mut Rng| rng.below(9) == 0;
        let mut i64s: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut f64s: Vec<Option<f64>> = Vec::with_capacity(n);
        let mut decs: Vec<Option<i128>> = Vec::with_capacity(n);
        let mut strs: Vec<Option<String>> = Vec::with_capacity(n);
        let mut u64s: Vec<Option<u64>> = Vec::with_capacity(n);
        let mut u32s: Vec<Option<u32>> = Vec::with_capacity(n);
        let mut ts = Vec::with_capacity(n);
        let mut pad = Vec::with_capacity(n);
        for _ in 0..n {
            i64s.push((!null(&mut rng)).then(|| 1000 + rng.below(300) as i64));
            f64s.push((!null(&mut rng)).then(|| rng.below(300) as f64 + 0.5));
            let d = if dec.1 == 2 {
                i128::from(rng.below(300)) * 10 // thousandths 10*k, stored at scale 2 as k
            } else {
                i128::from(rng.below(3000))
            };
            decs.push((!null(&mut rng)).then_some(d));
            strs.push((!null(&mut rng)).then(|| format!("s{}", rng.below(300))));
            u64s.push((!null(&mut rng)).then(|| 100 + rng.below(300)));
            u32s.push((!null(&mut rng)).then(|| 100 + rng.below(300) as u32));
            ts.push((!null(&mut rng)).then(|| {
                1_767_225_600_000_000_i64 + (f as i64) * 3_600_000_000 + rng.below(1_000_000) as i64
            }));
            pad.push(format!("p{:05}", rng.below(100_000)));
        }
        // Planted specials: (file, row group, row offset).
        let at = |g: usize, j: usize| g * rows + j;
        let put = |f0: usize, g: usize| f == f0 && g < rgs;
        for g in 0..rgs {
            if put(0, g) && g != 1 {
                f64s[at(g, 1)] = Some(0.0);
            }
            if put(1, g) && g == 1 {
                f64s[at(g, 1)] = Some(-0.0);
            }
            if put(2, g) && g == 2 {
                f64s[at(g, 1)] = Some(0.0);
                f64s[at(g, 2)] = Some(-0.0);
            }
            if put(3, g) && g == 0 {
                f64s[at(g, 3)] = Some(f64::NAN);
            }
            if put(5, g) && g == 1 {
                f64s[at(g, 3)] = Some(1.5);
            }
            // 1.500 at scale 2 (file 0) and at scale 3 (file 1); 1.501 only
            // at scale 3 (file 3); -1.000 at scale 2 (file 4).
            if put(0, g) && g == 2 {
                decs[at(g, 4)] = Some(150);
            }
            if put(1, g) && g == 0 {
                decs[at(g, 4)] = Some(1500);
            }
            if put(3, g) && g == 2 {
                decs[at(g, 4)] = Some(1501);
            }
            if put(4, g) && g == 1 {
                decs[at(g, 4)] = Some(-100);
            }
            if put(0, g) && g == 1 {
                u64s[at(g, 5)] = Some((1 << 63) + 5);
                u32s[at(g, 5)] = Some((1 << 31) + 3);
                strs[at(g, 5)] = Some("a\0b".to_string());
                i64s[at(g, 5)] = Some(i64::MIN);
            }
            if put(2, g) && g == 0 {
                u64s[at(g, 5)] = Some(u64::MAX);
                u32s[at(g, 5)] = Some(u32::MAX);
                strs[at(g, 5)] = Some(String::new());
                i64s[at(g, 5)] = Some(-5);
            }
            if put(4, g) && g == 2 {
                strs[at(g, 6)] = Some("a".to_string());
                i64s[at(g, 6)] = Some(7);
            }
            if put(5, g) && g == 0 {
                strs[at(g, 6)] = Some("\u{e9}".to_string());
                i64s[at(g, 6)] = Some(7);
            }
            // (0, null) in file 1 row group 2 and file 3 row group 1; (0, "a")
            // in file 2 row group 1.
            if (put(1, g) && g == 2) || (put(3, g) && g == 1) {
                i64s[at(g, 7)] = Some(0);
                strs[at(g, 7)] = None;
            }
            if put(2, g) && g == 1 {
                i64s[at(g, 7)] = Some(0);
                strs[at(g, 7)] = Some("a".to_string());
            }
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(i64s)) as ArrayRef,
                Arc::new(Float64Array::from(f64s)),
                Arc::new(
                    Decimal128Array::from(decs)
                        .with_precision_and_scale(dec.0, dec.1)
                        .unwrap(),
                ),
                Arc::new(StringArray::from(strs)),
                Arc::new(UInt64Array::from(u64s)),
                Arc::new(UInt32Array::from(u32s)),
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                Arc::new(StringArray::from(pad)),
            ],
        )
        .unwrap();
        let mut props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows))
            .set_created_by("review-probes".into());
        if f % 2 == 0 {
            for c in ["k_i64", "k_u64", "k_u32", "k_str", "k_f64", "k_dec"] {
                props = props
                    .set_column_bloom_filter_enabled(ColumnPath::from(c), true)
                    .set_column_bloom_filter_max_ndv(ColumnPath::from(c), rows as u64);
            }
        }
        let path = data.join(format!("part-{f:03}.parquet"));
        let mut w =
            ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(props.build())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    // Canonical, so paths agree with the build crate's canonical root.
    let data = std::fs::canonicalize(&data).unwrap();
    Corpus {
        data: data.to_str().unwrap().to_string(),
        dir,
    }
}

/// A typed key element for the oracle.
#[derive(Clone, Debug)]
pub enum V {
    I(i64),
    F(f64),
    /// Thousandths (dataset scale 3).
    D(i128),
    S(String),
    U(u64),
}

impl V {
    /// SQL equality, with NaN equal to NaN (the canonical NaN) and +0 == -0.
    pub fn eq_sql(&self, o: &V) -> bool {
        match (self, o) {
            (V::I(a), V::I(b)) => a == b,
            (V::F(a), V::F(b)) => a == b || (a.is_nan() && b.is_nan()),
            (V::D(a), V::D(b)) => a == b,
            (V::S(a), V::S(b)) => a == b,
            (V::U(a), V::U(b)) => a == b,
            _ => false,
        }
    }
}

fn value(arr: &ArrayRef, row: usize) -> Option<V> {
    if arr.is_null(row) {
        return None;
    }
    let any = arr.as_any();
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return Some(V::I(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        return Some(V::F(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<Decimal128Array>() {
        let s = a.scale();
        let v = a.value(row);
        let milli = match s {
            2 => v * 10,
            3 => v,
            other => panic!("unexpected scale {other}"),
        };
        return Some(V::D(milli));
    }
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return Some(V::S(a.value(row).to_string()));
    }
    if let Some(a) = any.downcast_ref::<UInt64Array>() {
        return Some(V::U(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<UInt32Array>() {
        return Some(V::U(u64::from(a.value(row))));
    }
    panic!("oracle: unsupported type {}", arr.data_type());
}

/// Calls `f(row_group, columns)` for each row group of `path`.
pub fn for_each_row_group(path: &Path, columns: &[&str], mut f: impl FnMut(u32, &[ArrayRef])) {
    let n = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .metadata()
        .num_row_groups();
    for rg in 0..n {
        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        let schema = b.schema().clone();
        let idx: Vec<usize> = columns
            .iter()
            .map(|c| schema.index_of(c).unwrap())
            .collect();
        let mask = parquet::arrow::ProjectionMask::roots(b.parquet_schema(), idx);
        let reader = b
            .with_row_groups(vec![rg])
            .with_projection(mask)
            .with_batch_size(1 << 20)
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let cols: Vec<ArrayRef> = columns
                .iter()
                .map(|c| Arc::clone(batch.column_by_name(c).unwrap()))
                .collect();
            f(rg as u32, &cols);
        }
    }
}

pub type Cell = (String, u32);

/// Every row group holding one of `keys` (tuples over `columns`). A row with
/// a null in any key column matches nothing (ADR 014); a key tuple with a
/// null (`None`) matches nothing.
pub fn oracle(corpus: &Corpus, columns: &[&str], keys: &[Vec<Option<V>>]) -> BTreeSet<Cell> {
    let keys: Vec<Vec<V>> = keys
        .iter()
        .filter_map(|t| t.iter().cloned().collect::<Option<Vec<V>>>())
        .collect();
    let mut out = BTreeSet::new();
    for path in corpus.files() {
        for_each_row_group(&path, columns, |rg, cols| {
            'rows: for row in 0..cols[0].len() {
                let t: Option<Vec<V>> = cols.iter().map(|c| value(c, row)).collect();
                let Some(t) = t else { continue };
                for k in &keys {
                    if k.iter().zip(&t).all(|(a, b)| a.eq_sql(b)) {
                        out.insert((path.to_str().unwrap().to_string(), rg));
                        break 'rows;
                    }
                }
            }
        });
    }
    out
}

/// Compressed bytes of every row group, read from the footers with the
/// `parquet` crate (`RowGroupMetaData::compressed_size`).
pub fn footer_bytes(corpus: &Corpus) -> BTreeMap<Cell, u64> {
    let mut out = BTreeMap::new();
    for path in corpus.files() {
        let r = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
        for i in 0..r.num_row_groups() {
            out.insert(
                (path.to_str().unwrap().to_string(), i as u32),
                r.metadata().row_group(i).compressed_size() as u64,
            );
        }
    }
    out
}

/// `(file path, row group)` cells of a plan.
pub fn cells(plan: &bellwether_types::ScanPlan) -> BTreeSet<Cell> {
    plan.entries
        .iter()
        .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
        .collect()
}

/// Files, row groups, and bytes of a set of cells, from the footers.
pub fn cost_of_cells(cells: &BTreeSet<Cell>, bytes: &BTreeMap<Cell, u64>) -> (u64, u64, u64) {
    let files: BTreeSet<&String> = cells.iter().map(|(f, _)| f).collect();
    let b: u64 = cells.iter().map(|c| bytes[c]).sum();
    (files.len() as u64, cells.len() as u64, b)
}
