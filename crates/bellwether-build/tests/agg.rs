//! Agg keys (B7) end to end through the build and the planner, against an
//! oracle that decodes every file itself and sums VALUES as `i128` at the
//! dataset scale, never the cells the code under test produced.
//!
//! - Exact decimal sums, counts, nulls (ADR 014: a null amount is counted,
//!   never summed or compared; a null account is in no key), minimum and
//!   maximum per key, over many files and row groups with skewed keys, at
//!   three segment caps (one key's cells spread over many segments), and
//!   again after a file is appended and another replaced (retired row
//!   groups never count).
//! - Invariant 1 for a finer lookup over a coarser agg key: per key, the
//!   answered aggregate plus the rows of the residual row groups the
//!   residual keeps is exactly the oracle's, and every row group holding
//!   the key is either answered or residual.
//! - Refusals that name the declaration: no agg key, no counted key, a
//!   function the agg key does not store, an agg key built alone.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

mod common;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Builder, Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Decimal128Type, Field, Int64Type, Schema, SchemaRef};
use bellwether_build::{AggLookup, BuildOptions, Builder, Planner, SourceKind};
use bellwether_core::agg::{AggCell, AggSum};
use bellwether_core::encoding::{encode_tuple_as, Value};
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{AggregateFn, KeySpec, Predicate};
use common::{memory_store, write_parquet, Scratch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const SCALE: i8 = 6;

fn dataset() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("account", DataType::Utf8, true),
        Field::new("day", DataType::Int64, true),
        Field::new("amount", DataType::Decimal128(18, SCALE), true),
    ]))
}

/// One row: account (None = null key), day, amount (unscaled, None = null).
type Row = (Option<String>, i64, Option<i128>);

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// Skewed: key `k` of `n` with weight about `1 / (k + 1)`.
    fn skewed(&mut self, n: u64) -> u64 {
        let a = self.below(n);
        self.below(a + 1)
    }
}

/// `n` rows from `seed`; every row on `day` when given (a file of one day,
/// so a row group's cells hold only that day's rows), else days 0..4.
fn gen_rows(seed: u64, n: usize, day: Option<i64>) -> Vec<Row> {
    let mut g = Lcg(seed);
    (0..n)
        .map(|_| {
            let account = (g.below(25) != 0).then(|| format!("acct{:02}", g.skewed(20)));
            let day = day.unwrap_or_else(|| g.below(4) as i64);
            let amount = (g.below(10) != 0)
                .then(|| i128::from(g.below(2_000_000_000) as i64 - 1_000_000_000));
            (account, day, amount)
        })
        .collect()
}

fn write_rows(path: &Path, rows: &[Row], rows_per_rg: usize) {
    let mut a = StringBuilder::new();
    let mut d = Int64Builder::new();
    let mut v = Decimal128Builder::new()
        .with_precision_and_scale(18, SCALE)
        .unwrap();
    for (acct, day, amount) in rows {
        a.append_option(acct.as_deref());
        d.append_value(*day);
        v.append_option(*amount);
    }
    let cols: Vec<ArrayRef> = vec![
        Arc::new(a.finish()),
        Arc::new(d.finish()),
        Arc::new(v.finish()),
    ];
    write_parquet(
        path,
        &RecordBatch::try_new(dataset(), cols).unwrap(),
        rows_per_rg,
    );
}

/// Every row of every file under `root`, decoded independently, with its
/// file and row group: (path, row group, row).
fn decode(root: &Path) -> Vec<(String, u32, Row)> {
    let mut paths: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    paths.sort();
    let mut out = Vec::new();
    for p in paths {
        let n = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
            .unwrap()
            .metadata()
            .num_row_groups();
        for rg in 0..n {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            for b in reader {
                let b = b.unwrap();
                let a = b.column(0).as_string::<i32>();
                let d = b.column(1).as_primitive::<Int64Type>();
                let v = b.column(2).as_primitive::<Decimal128Type>();
                for i in 0..b.num_rows() {
                    out.push((
                        p.to_str().unwrap().to_string(),
                        rg as u32,
                        (
                            a.is_valid(i).then(|| a.value(i).to_string()),
                            d.value(i),
                            v.is_valid(i).then(|| v.value(i)),
                        ),
                    ));
                }
            }
        }
    }
    out
}

/// The oracle's aggregate of the rows `keep` selects: rows, nulls, exact
/// sum, min, max.
#[derive(Debug, PartialEq, Eq)]
struct Agg {
    rows: u64,
    nulls: u64,
    sum: i128,
    min: Option<i128>,
    max: Option<i128>,
}

fn oracle<'a>(rows: impl Iterator<Item = &'a Row>) -> Agg {
    let mut a = Agg {
        rows: 0,
        nulls: 0,
        sum: 0,
        min: None,
        max: None,
    };
    for (_, _, v) in rows {
        a.rows += 1;
        match v {
            None => a.nulls += 1,
            Some(x) => {
                a.sum += x;
                a.min = Some(a.min.map_or(*x, |m| m.min(*x)));
                a.max = Some(a.max.map_or(*x, |m| m.max(*x)));
            }
        }
    }
    a
}

fn dec(v: i128) -> Vec<u8> {
    encode_tuple_as(
        &[Value::Decimal128 {
            value: v,
            scale: SCALE,
        }],
        &[DataType::Decimal128(18, SCALE)],
    )
    .unwrap()
}

fn agg_of(cell: Option<&AggCell>) -> Agg {
    let Some(c) = cell else {
        return Agg {
            rows: 0,
            nulls: 0,
            sum: 0,
            min: None,
            max: None,
        };
    };
    let decode = |b: &Option<Vec<u8>>| b.as_deref().map(decode_dec);
    Agg {
        rows: c.rows,
        nulls: c.nulls,
        sum: match c.sum {
            Some(AggSum::Int(v)) => v,
            other => panic!("not an exact sum: {other:?}"),
        },
        min: decode(&c.min),
        max: decode(&c.max),
    }
}

/// A canonical Decimal128 encoding's unscaled value (tag, width, then 16
/// big-endian bytes with the sign bit flipped).
fn decode_dec(b: &[u8]) -> i128 {
    let mut raw: [u8; 16] = b[2..18].try_into().unwrap();
    raw[0] ^= 0x80;
    let v = i128::from_be_bytes(raw);
    assert_eq!(dec(v), b, "a canonical decimal at the dataset scale");
    v
}

fn account(s: &str) -> Vec<u8> {
    encode_tuple_as(&[Value::Utf8(s)], &[DataType::Utf8]).unwrap()
}

fn account_day(s: &str, d: i64) -> Vec<u8> {
    encode_tuple_as(
        &[Value::Utf8(s), Value::Int(d)],
        &[DataType::Utf8, DataType::Int64],
    )
    .unwrap()
}

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

fn counted(c: &[&str]) -> KeySpec {
    KeySpec::Exact {
        columns: cols(c),
        counts: true,
        bounds: None,
    }
}

fn agg(c: &[&str]) -> KeySpec {
    KeySpec::Agg {
        columns: cols(c),
        value: "amount".into(),
        functions: AggregateFn::CANONICAL.to_vec(),
    }
}

fn opts(max_keys: u64, max_rgs: usize) -> BuildOptions {
    BuildOptions {
        schema: Some(dataset()),
        max_segment_keys: max_keys,
        max_segment_row_groups: max_rgs,
        ..BuildOptions::new(SourceKind::Parquet)
    }
}

fn accounts() -> Vec<String> {
    (0..20).map(|k| format!("acct{k:02}")).collect()
}

/// Checks every account's aggregate, then every (account, day)'s through
/// the coarser agg key, against the decoded files.
fn check(planner: &Planner<ObjectStoreBackend>, specs: &[KeySpec], root: &Path) {
    let rows = decode(root);
    // Live row groups only: the manifest's files are the folder's.
    let lookup = AggLookup {
        columns: cols(&["account"]),
        keys: accounts()
            .iter()
            .map(|a| account(a))
            .chain([account("nobody")])
            .collect(),
        value: "amount".into(),
        functions: AggregateFn::CANONICAL.to_vec(),
    };
    let a = planner.aggregate(specs, &lookup).unwrap();
    assert_eq!(a.agg_spec, agg(&["account"]));
    assert_eq!(a.counted_by, counted(&["account"]));
    // The agg key's columns are the lookup's: nothing to read.
    assert_eq!(a.residual.row_groups_selected, 0);
    assert_eq!(a.residual.entries.len(), 0);
    for k in &a.keys {
        let name = accounts()
            .into_iter()
            .chain(["nobody".to_string()])
            .find(|n| account(n) == k.key)
            .unwrap();
        let want = oracle(
            rows.iter()
                .filter(|(_, _, r)| r.0.as_deref() == Some(name.as_str()))
                .map(|(_, _, r)| r),
        );
        assert_eq!(k.cover.rows, want.rows, "{name}");
        assert!(k.cover.residual.is_empty());
        assert_eq!(agg_of(k.cover.covered.as_ref()), want, "{name}");
    }

    // A finer lookup (account, day) over the agg key on account, beside
    // the counted key on (account, day): a row group where the account's
    // cell holds rows of other days is residual.
    let keys: Vec<(String, i64)> = accounts()
        .into_iter()
        .flat_map(|a| (0..4).map(move |d| (a.clone(), d)))
        .collect();
    let lookup = AggLookup {
        columns: cols(&["account", "day"]),
        keys: keys.iter().map(|(a, d)| account_day(a, *d)).collect(),
        value: "amount".into(),
        functions: AggregateFn::CANONICAL.to_vec(),
    };
    let a = planner.aggregate(specs, &lookup).unwrap();
    assert_eq!(a.counted_by, counted(&["account", "day"]));
    // Map the residual plan's (file, row group) back to row-group ids is
    // not needed: the residual's files and indexes in file name the rows.
    let residual: std::collections::BTreeSet<(String, u32)> = a
        .residual
        .entries
        .iter()
        .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
        .collect();
    let Some(Predicate::In { columns, values }) = &a.residual.residual else {
        panic!("an In residual");
    };
    assert_eq!(columns, &cols(&["account", "day"]));
    let mut answered_some = false;
    let mut read_some = false;
    for k in &a.keys {
        let (name, day) = keys
            .iter()
            .find(|(n, d)| account_day(n, *d) == k.key)
            .unwrap();
        let mine = |r: &Row| r.0.as_deref() == Some(name.as_str()) && r.1 == *day;
        let want = oracle(rows.iter().filter(|(_, _, r)| mine(r)).map(|(_, _, r)| r));
        assert_eq!(k.cover.rows, want.rows);
        // Every row group holding the key is answered or read (invariant
        // 1), and its residual rows are those of the read row groups.
        let read: Vec<&Row> = rows
            .iter()
            .filter(|(p, rg, r)| mine(r) && residual.contains(&(p.clone(), *rg)))
            .map(|(_, _, r)| r)
            .collect();
        assert_eq!(k.cover.residual_rows, read.len() as u64);
        if !read.is_empty() {
            assert!(values.contains(&k.key), "the residual keeps the key's rows");
            read_some = true;
        }
        let covered = agg_of(k.cover.covered.as_ref());
        answered_some |= covered.rows > 0;
        let rest = oracle(read.into_iter());
        assert_eq!(covered.rows + rest.rows, want.rows);
        assert_eq!(covered.nulls + rest.nulls, want.nulls);
        assert_eq!(covered.sum + rest.sum, want.sum, "{name} day {day}");
        let both = |a: Option<i128>, b: Option<i128>, f: fn(i128, i128) -> i128| match (a, b) {
            (Some(x), Some(y)) => Some(f(x, y)),
            (x, None) => x,
            (None, y) => y,
        };
        assert_eq!(both(covered.min, rest.min, i128::min), want.min);
        assert_eq!(both(covered.max, rest.max, i128::max), want.max);
    }
    assert!(answered_some && read_some, "both paths exercised");
}

#[test]
fn aggregates_equal_decoded_files_at_every_segment_cap_and_after_changes() {
    let specs = vec![
        counted(&["account"]),
        counted(&["account", "day"]),
        agg(&["account"]),
    ];
    for (max_keys, max_rgs) in [(1_000_000, 1_000), (5, 3), (2, 1)] {
        let s = Scratch::new();
        for f in 0..4u64 {
            write_rows(
                &s.path(&format!("part-{f}.parquet")),
                &gen_rows(f + 7, 300, (f % 2 == 0).then_some(f as i64)),
                40,
            );
        }
        let store = memory_store();
        let b = Builder::new(Arc::clone(&store), "idx");
        b.build(&s.root_str(), &specs, &opts(max_keys, max_rgs))
            .unwrap();
        let planner = Planner::open(Arc::clone(&store), "idx").unwrap();
        let segs = planner
            .manifest()
            .segments
            .iter()
            .filter(|x| x.key_spec == agg(&["account"]).identity())
            .count();
        if max_keys == 2 {
            assert!(segs > 4, "one key's cells spread over {segs} segments");
        }
        check(&planner, &specs, s.root());
        // Append a file and replace another: the new rows count, the
        // retired row groups never do.
        write_rows(&s.path("part-9.parquet"), &gen_rows(99, 200, None), 30);
        write_rows(&s.path("part-1.parquet"), &gen_rows(55, 120, Some(3)), 25);
        b.build(&s.root_str(), &specs, &opts(max_keys, max_rgs))
            .unwrap();
        let planner = Planner::open(Arc::clone(&store), "idx").unwrap();
        assert_ne!(planner.manifest().rowgroup_space.tombstones.len(), 0);
        check(&planner, &specs, s.root());
    }
}

#[test]
fn refusals_name_the_declaration_that_answers() {
    let s = Scratch::new();
    write_rows(&s.path("a.parquet"), &gen_rows(1, 50, None), 10);
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    // An agg key alone is refused at the build, naming the counted key.
    let e = b
        .build(&s.root_str(), &[agg(&["account"])], &opts(1_000_000, 1_000))
        .unwrap_err()
        .to_string();
    assert!(e.contains("--key 'exact:account,counts'"), "{e}");
    let specs = vec![counted(&["account"]), agg(&["account"])];
    b.build(&s.root_str(), &specs, &opts(1_000_000, 1_000))
        .unwrap();
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    let lookup = |c: &[&str], value: &str, f: &[AggregateFn]| AggLookup {
        columns: cols(c),
        keys: Vec::new(),
        value: value.into(),
        functions: f.to_vec(),
    };
    // No agg key on that value: the declaration to add.
    let e = p
        .aggregate(&specs, &lookup(&["account"], "day", &[AggregateFn::Sum]))
        .unwrap_err()
        .to_string();
    assert!(e.contains("agg:account@day{sum,count}"), "{e}");
    // A finer lookup without a counted key on its columns.
    let e = p
        .aggregate(
            &specs,
            &lookup(&["account", "day"], "amount", &[AggregateFn::Sum]),
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("exact:account,day,counts"), "{e}");
    // An agg key on a superset of the lookup's columns does not serve it.
    let e = p
        .aggregate(
            &[counted(&["day"]), agg(&["account"])],
            &lookup(&["day"], "amount", &[AggregateFn::Sum]),
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("agg:day@amount"), "{e}");
    // A summed string column is refused at the build.
    let text = KeySpec::Agg {
        columns: cols(&["day"]),
        value: "account".into(),
        functions: AggregateFn::canonical(&[AggregateFn::Sum]),
    };
    let e = b
        .build(
            &s.root_str(),
            &[counted(&["day"]), text],
            &opts(1_000_000, 1_000),
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("{count,min,max}"), "{e}");
}
