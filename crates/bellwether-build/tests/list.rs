//! List keys (B4) end to end through the build and the planner, against an
//! oracle that decodes every file itself and compares VALUES (SQL
//! equality), never the canonical bytes the code under test produces.
//!
//! Invariant 1 (no false negatives) for `contains_any` and `contains_all`
//! over `List<Utf8>`, `LargeList<Int64>`, `List<Float64>` (signed zero,
//! NaN) and `Map<Utf8, Int64>` (by its keys), with duplicate elements in a
//! list, null lists, empty lists, null elements, a row whose distinct
//! elements alone exceed the segment key cap, and elements spread over
//! many row groups and files; at three segment caps, so one element's row
//! groups are spread over many segments. The residual over the planned
//! rows is exactly the matching rows.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::float_cmp,
    clippy::type_complexity,
    clippy::many_single_char_names,
    clippy::unusual_byte_groupings
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Float64Builder, Int64Array, Int64Builder, LargeListBuilder,
    ListBuilder, MapBuilder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, SchemaRef};
use bellwether_build::{BuildOptions, Builder, Lookup, Planner, SourceKind};
use bellwether_core::encoding::Value;
use bellwether_store::ObjectStoreBackend;
use bellwether_types::{KeySpec, ScanPlan};
use common::{apply_residual, enc, memory_store, Row, Scratch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// A decoded element, compared with SQL equality.
#[derive(Clone, Debug)]
enum V {
    I(i64),
    F(f64),
    S(String),
}

fn sql_eq(a: &V, b: &V) -> bool {
    match (a, b) {
        (V::I(x), V::I(y)) => x == y,
        // SQL: 0.0 = -0.0; the canonical encoding has one NaN, and a NaN
        // probe matches NaN elements
        (V::F(x), V::F(y)) => (x.is_nan() && y.is_nan()) || x == y,
        (V::S(x), V::S(y)) => x == y,
        _ => false,
    }
}

/// One list cell: `None` for a null list; `None` elements are nulls.
type Cell = Option<Vec<Option<V>>>;

/// (path, row group, row ordinal in the file, column -> cell).
type ORow = (String, u32, usize, BTreeMap<String, Cell>);

const LIST_COLUMNS: [&str; 4] = ["tags", "nums", "vals", "attrs"];

fn item(dt: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", dt, true))
}

fn dataset() -> SchemaRef {
    let tags = ListBuilder::new(StringBuilder::new()).finish();
    let nums = LargeListBuilder::new(Int64Builder::new()).finish();
    let attrs = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new()).finish();
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("tags", tags.data_type().clone(), true),
        Field::new("nums", nums.data_type().clone(), true),
        Field::new("vals", DataType::List(item(DataType::Float64)), true),
        Field::new("attrs", attrs.data_type().clone(), true),
    ]))
}

/// A deterministic generator (no dependency on a seeded crate's stream).
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

    /// Skewed: element `k` of `n` with weight about `1 / (k + 1)`.
    fn skewed(&mut self, n: u64) -> u64 {
        let a = self.below(n);
        self.below(a + 1)
    }
}

const TAGS: u64 = 12;
const FLOATS: [f64; 6] = [0.0, -0.0, 1.5, -2.25, f64::NAN, 7.0];

/// One row's lists: tags, nums, vals, attrs (keys); `wide` gives the row
/// `tags` of 40 distinct values, more than any segment cap below holds.
fn gen_row(
    g: &mut Lcg,
    wide: bool,
) -> (
    Option<Vec<Option<String>>>,
    Option<Vec<Option<i64>>>,
    Option<Vec<Option<f64>>>,
    Option<Vec<String>>,
) {
    let shape = |g: &mut Lcg| match g.below(10) {
        0 => None,
        1 => Some(0),
        _ => Some(1 + g.below(5) as usize),
    };
    let tags = if wide {
        Some((0..40).map(|i| Some(format!("w{i:02}"))).collect())
    } else {
        shape(g).map(|n| {
            (0..n)
                .map(|_| {
                    // duplicates come from the skew; one in nine is null
                    (g.below(9) != 0).then(|| format!("t{}", g.skewed(TAGS)))
                })
                .collect()
        })
    };
    let nums = shape(g).map(|n| {
        (0..n)
            .map(|_| (g.below(9) != 0).then(|| g.skewed(30) as i64 - 3))
            .collect()
    });
    let vals = shape(g).map(|n| {
        (0..n)
            .map(|_| (g.below(9) != 0).then(|| FLOATS[g.below(FLOATS.len() as u64) as usize]))
            .collect()
    });
    // map keys are unique within a map and never null
    let attrs = shape(g).map(|n| {
        let set: BTreeSet<String> = (0..n).map(|_| format!("k{}", g.skewed(8))).collect();
        set.into_iter().collect()
    });
    (tags, nums, vals, attrs)
}

fn batch(rows: &[(i64, bool)], g: &mut Lcg) -> RecordBatch {
    let mut ids = Vec::new();
    let mut tags = ListBuilder::new(StringBuilder::new());
    let mut nums = LargeListBuilder::new(Int64Builder::new());
    let mut vals = ListBuilder::new(Float64Builder::new());
    let mut attrs = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    for &(id, wide) in rows {
        ids.push(id);
        let (t, n, v, a) = gen_row(g, wide);
        match t {
            None => tags.append(false),
            Some(items) => {
                for i in items {
                    tags.values().append_option(i);
                }
                tags.append(true);
            }
        }
        match n {
            None => nums.append(false),
            Some(items) => {
                for i in items {
                    nums.values().append_option(i);
                }
                nums.append(true);
            }
        }
        match v {
            None => vals.append(false),
            Some(items) => {
                for i in items {
                    vals.values().append_option(i);
                }
                vals.append(true);
            }
        }
        match a {
            None => attrs.append(false).unwrap(),
            Some(keys) => {
                for (j, k) in keys.iter().enumerate() {
                    attrs.keys().append_value(k);
                    attrs.values().append_value(j as i64);
                }
                attrs.append(true).unwrap();
            }
        }
    }
    let schema = dataset();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(tags.finish()),
        Arc::new(nums.finish()),
        Arc::new(vals.with_field(item(DataType::Float64)).finish()),
        Arc::new(attrs.finish()),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

/// Writes one file of `groups` row groups, `rows` rows each; the row
/// `wide` (file row ordinal), if any, holds 40 distinct tags.
fn write_file(path: &Path, groups: usize, rows: usize, wide: Option<usize>, g: &mut Lcg) {
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), dataset(), Some(props)).unwrap();
    for rg in 0..groups {
        let r: Vec<(i64, bool)> = (0..rows)
            .map(|i| {
                let n = rg * rows + i;
                (n as i64, Some(n) == wide)
            })
            .collect();
        w.write(&batch(&r, g)).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

fn write_corpus(s: &Scratch) -> Vec<PathBuf> {
    let mut g = Lcg(0x5eed_b4);
    let a = s.path("a.parquet");
    write_file(&a, 6, 5, Some(7), &mut g);
    let b = s.path("b.parquet");
    write_file(&b, 4, 3, None, &mut g);
    let c = s.path("c.parquet");
    // one row group, one row: the wide list alone
    write_file(&c, 1, 1, Some(0), &mut g);
    vec![a, b, c]
}

fn cell(a: &ArrayRef, i: usize) -> Cell {
    if a.is_null(i) {
        return None;
    }
    let elems: ArrayRef = match a.data_type() {
        DataType::List(_) => a.as_list::<i32>().value(i),
        DataType::LargeList(_) => a.as_list::<i64>().value(i),
        DataType::Map(_, _) => Arc::clone(a.as_map().value(i).column(0)),
        other => panic!("oracle does not decode {other}"),
    };
    Some(
        (0..elems.len())
            .map(|j| {
                if elems.is_null(j) {
                    return None;
                }
                Some(match elems.data_type() {
                    DataType::Int64 => V::I(elems.as_primitive::<Int64Type>().value(j)),
                    DataType::Float64 => V::F(elems.as_primitive::<Float64Type>().value(j)),
                    DataType::Utf8 => V::S(elems.as_string::<i32>().value(j).to_string()),
                    other => panic!("oracle does not decode element {other}"),
                })
            })
            .collect(),
    )
}

fn decode(path: &Path) -> Vec<ORow> {
    let n = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .metadata()
        .num_row_groups();
    let p = path.to_str().unwrap().to_string();
    let mut out = Vec::new();
    let mut ordinal = 0;
    for rg in 0..n {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .with_row_groups(vec![rg])
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            for r in 0..batch.num_rows() {
                let m = LIST_COLUMNS
                    .iter()
                    .map(|c| ((*c).to_string(), cell(batch.column_by_name(c).unwrap(), r)))
                    .collect();
                out.push((p.clone(), rg as u32, ordinal, m));
                ordinal += 1;
            }
        }
    }
    out
}

fn holds(c: &Cell, v: &V) -> bool {
    c.as_ref()
        .is_some_and(|items| items.iter().flatten().any(|e| sql_eq(e, v)))
}

fn matches(c: &Cell, probe: &[V], all: bool) -> bool {
    if all {
        // ADR 014: a null or empty list holds no element, so it contains no
        // value; the lookups below always ask for at least one value
        probe.iter().all(|v| holds(c, v))
    } else {
        probe.iter().any(|v| holds(c, v))
    }
}

fn planned(plan: &ScanPlan) -> BTreeSet<(String, u32)> {
    plan.entries
        .iter()
        .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
        .collect()
}

fn element_type(column: &str) -> DataType {
    match column {
        "tags" | "attrs" => DataType::Utf8,
        "nums" => DataType::Int64,
        "vals" => DataType::Float64,
        _ => unreachable!(),
    }
}

fn encode(column: &str, v: &V) -> Vec<u8> {
    let dt = element_type(column);
    match v {
        V::I(i) => enc(&Value::Int(*i), &dt),
        V::F(f) => enc(&Value::Float(*f), &dt),
        V::S(s) => enc(&Value::Utf8(s), &dt),
    }
}

struct Counts {
    checks: u64,
    nonempty: u64,
    pruned: u64,
}

#[allow(clippy::too_many_arguments)]
fn check(
    p: &Planner<ObjectStoreBackend>,
    rows: &[ORow],
    crow: &[Row],
    column: &str,
    probe: &[V],
    all: bool,
    counts: &mut Counts,
) {
    let spec = KeySpec::List {
        column: column.into(),
    };
    let values: Vec<Vec<u8>> = probe.iter().map(|v| encode(column, v)).collect();
    let lookup = if all {
        Lookup::ContainsAll {
            column: column.into(),
            values,
        }
    } else {
        Lookup::ContainsAny {
            column: column.into(),
            values,
        }
    };
    let plan = p
        .plan(std::slice::from_ref(&spec), &lookup)
        .unwrap_or_else(|e| panic!("{column} {probe:?} all={all}: {e}"));
    let want_rows: BTreeSet<(String, u32, usize)> = rows
        .iter()
        .filter(|(_, _, _, m)| matches(&m[column], probe, all))
        .map(|(path, rg, o, _)| (path.clone(), *rg, *o))
        .collect();
    let want: BTreeSet<(String, u32)> = want_rows
        .iter()
        .map(|(path, rg, _)| (path.clone(), *rg))
        .collect();
    let got = planned(&plan);
    counts.checks += 1;
    if !want.is_empty() {
        counts.nonempty += 1;
    }
    if got.len() < plan.row_groups_total as usize {
        counts.pruned += 1;
    }
    assert!(
        want.is_subset(&got),
        "INVARIANT 1: list:{column} all={all} probe {probe:?} misses {:?}",
        want.difference(&got).collect::<Vec<_>>()
    );
    // residual exactness, over the planned rows only
    let res = apply_residual(
        plan.residual.as_ref().unwrap(),
        crow.iter()
            .filter(|r| got.contains(&(r.path.clone(), r.rg)))
            .cloned()
            .collect(),
    );
    let res_rows: BTreeSet<(String, u32, usize)> =
        res.iter().map(|r| (r.path.clone(), r.rg, r.row)).collect();
    assert_eq!(
        res_rows, want_rows,
        "residual of list:{column} all={all} for {probe:?} is not the matching rows"
    );
}

fn universe(column: &str) -> Vec<V> {
    match column {
        "tags" => (0..TAGS)
            .map(|i| V::S(format!("t{i}")))
            .chain([
                V::S("w05".into()),
                V::S("w39".into()),
                V::S("absent".into()),
            ])
            .collect(),
        "nums" => (-4..30).map(V::I).collect(),
        "vals" => FLOATS
            .iter()
            .copied()
            .chain([3.0, f64::INFINITY])
            .map(V::F)
            .collect(),
        "attrs" => (0..10).map(|i| V::S(format!("k{i}"))).collect(),
        _ => unreachable!(),
    }
}

fn run_all(store: &Arc<ObjectStoreBackend>, paths: &[PathBuf], counts: &mut Counts) {
    let rows: Vec<ORow> = paths.iter().flat_map(|p| decode(p)).collect();
    let crow: Vec<Row> = paths
        .iter()
        .flat_map(|p| common::parquet_rows(p, &dataset()))
        .collect();
    let p = Planner::open(Arc::clone(store), "idx").unwrap();
    let mut g = Lcg(7);
    for column in LIST_COLUMNS {
        let u = universe(column);
        for v in &u {
            check(
                &p,
                &rows,
                &crow,
                column,
                std::slice::from_ref(v),
                false,
                counts,
            );
            check(
                &p,
                &rows,
                &crow,
                column,
                std::slice::from_ref(v),
                true,
                counts,
            );
        }
        for _ in 0..60 {
            let n = 1 + g.below(4) as usize;
            // repeats allowed: a value asked twice is still one value
            let probe: Vec<V> = (0..n)
                .map(|_| u[g.below(u.len() as u64) as usize].clone())
                .collect();
            check(&p, &rows, &crow, column, &probe, false, counts);
            check(&p, &rows, &crow, column, &probe, true, counts);
        }
    }
}

fn specs() -> Vec<KeySpec> {
    LIST_COLUMNS
        .iter()
        .map(|c| KeySpec::List {
            column: (*c).to_string(),
        })
        .chain([KeySpec::Exact {
            columns: vec!["id".into()],
            counts: false,
            bounds: None,
        }])
        .collect()
}

#[test]
fn invariant1_for_list_keys_against_decoded_files() {
    let s = Scratch::new();
    let paths = write_corpus(&s);
    let mut counts = Counts {
        checks: 0,
        nonempty: 0,
        pruned: 0,
    };
    // (max keys per segment, row groups per unit of work): one unbounded,
    // then caps the wide row alone exceeds, so an element's row groups and
    // one row group's elements are split over many segments
    for (max_keys, item) in [(1u64 << 20, 64usize), (1, 1), (3, 2)] {
        let store = memory_store();
        let b = Builder::new(Arc::clone(&store), "idx");
        let opts = BuildOptions {
            schema: Some(dataset()),
            max_segment_keys: max_keys,
            max_segment_row_groups: item,
            threads: Some(2),
            ..BuildOptions::new(SourceKind::Parquet)
        };
        b.build(&s.root_str(), &specs(), &opts)
            .unwrap_or_else(|e| panic!("build at cap {max_keys}/{item}: {e}"));
        let m = b.manifest().unwrap().unwrap();
        let list_segments = m
            .segments
            .iter()
            .filter(|seg| seg.key_spec == "list:tags")
            .count();
        if max_keys == 1 {
            // one segment per row group at least: 11 row groups
            assert!(list_segments >= 11, "{list_segments} list:tags segments");
        }
        run_all(&store, &paths, &mut counts);
    }
    let Counts {
        checks,
        nonempty,
        pruned,
    } = counts;
    eprintln!("checks {checks}, non-empty truth {nonempty}, pruned {pruned}");
    assert!(
        checks > 1000 && nonempty > 500 && pruned > 300,
        "checks {checks}, non-empty {nonempty}, pruned {pruned}"
    );
}

/// The segment caps count a list key's distinct elements (ADR 018): with
/// work units as large as a file and a small key cap, every list segment
/// over more than one row group holds at most the cap's distinct elements,
/// summed per row group as the build accounts them; a row group whose
/// elements alone exceed the cap gets a segment of its own.
#[test]
fn segment_key_cap_counts_list_elements() {
    const CAP: u64 = 8;
    let s = Scratch::new();
    let paths = write_corpus(&s);
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    b.build(
        &s.root_str(),
        &specs(),
        &BuildOptions {
            schema: Some(dataset()),
            max_segment_keys: CAP,
            max_segment_row_groups: 64,
            threads: Some(1),
            ..BuildOptions::new(SourceKind::Parquet)
        },
    )
    .unwrap();
    let m = b.manifest().unwrap().unwrap();
    // Distinct `tags` elements per (path, row group), decoded by value.
    let rows: Vec<ORow> = paths.iter().flat_map(|p| decode(p)).collect();
    let mut per: BTreeMap<(String, u32), BTreeSet<String>> = BTreeMap::new();
    for (path, rg, _, cells) in &rows {
        let name = Path::new(path)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let e = per.entry((name, *rg)).or_default();
        for v in cells["tags"].iter().flatten().flatten() {
            if let V::S(x) = v {
                e.insert(x.clone());
            }
        }
    }
    let mut by_id: BTreeMap<u32, u64> = BTreeMap::new();
    for f in &m.rowgroup_space.files {
        for (i, id) in f.rowgroup_ids.iter().enumerate() {
            let name = Path::new(&f.fingerprint.path)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            let n = per.get(&(name, i as u32)).map_or(0, |e| e.len() as u64);
            by_id.insert(id.0, n);
        }
    }
    let segs: Vec<_> = m
        .segments
        .iter()
        .filter(|seg| seg.key_spec == "list:tags")
        .collect();
    let mut multi = 0;
    for seg in &segs {
        let (lo, hi) = (seg.rowgroup_range.start.0, seg.rowgroup_range.end.0);
        let ids: Vec<u32> = (lo..hi).filter(|i| by_id.contains_key(i)).collect();
        let keys: u64 = ids.iter().map(|i| by_id[i]).sum();
        if ids.len() > 1 {
            multi += 1;
            assert!(
                keys <= CAP,
                "segment {lo}..{hi} accounts {keys} > {CAP} elements"
            );
        }
    }
    // 11 row groups in 3 files: without the cap a unit is a whole file.
    assert!(segs.len() > 3, "{} list:tags segments", segs.len());
    assert!(multi > 0, "some segment spans several row groups");
}

/// The planner's refusals and edges: a list key never serves a point lookup
/// on its column, another kind never serves a membership lookup, no spec
/// reads every live row group, `contains_all` of nothing is refused, and
/// `contains_any` of nothing reads nothing.
#[test]
fn list_key_serves_membership_only() {
    let s = Scratch::new();
    write_corpus(&s);
    let store = memory_store();
    let b = Builder::new(Arc::clone(&store), "idx");
    b.build(
        &s.root_str(),
        &specs(),
        &BuildOptions {
            schema: Some(dataset()),
            ..BuildOptions::new(SourceKind::Parquet)
        },
    )
    .unwrap();
    let p = Planner::open(Arc::clone(&store), "idx").unwrap();
    let list = KeySpec::List {
        column: "tags".into(),
    };
    let exact = KeySpec::Exact {
        columns: vec!["id".into()],
        counts: false,
        bounds: None,
    };
    let t0 = vec![enc(&Value::Utf8("t0"), &DataType::Utf8)];
    let any = Lookup::ContainsAny {
        column: "tags".into(),
        values: t0.clone(),
    };
    // another kind: refused, naming the list key to declare
    let e = p
        .plan(std::slice::from_ref(&exact), &any)
        .unwrap_err()
        .to_string();
    assert!(e.contains("declare list:tags"), "{e}");
    // no spec: every live row group
    let plan = p.plan(&[], &any).unwrap();
    assert_eq!(plan.row_groups_selected, plan.row_groups_total);
    // the list key never serves a point lookup on the list column
    let point = Lookup::Keys {
        columns: vec!["tags".into()],
        keys: t0.clone(),
    };
    assert!(p.plan(std::slice::from_ref(&list), &point).is_err());
    // a list key on another column does not serve
    let nums = KeySpec::List {
        column: "nums".into(),
    };
    let e = p
        .plan(std::slice::from_ref(&nums), &any)
        .unwrap_err()
        .to_string();
    assert!(e.contains("list:tags"), "{e}");
    // contains_all of nothing: refused with the reason
    let e = p
        .plan(
            std::slice::from_ref(&list),
            &Lookup::ContainsAll {
                column: "tags".into(),
                values: Vec::new(),
            },
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("at least one value"), "{e}");
    // contains_all with a null value: no list holds a null (ADR 014), so
    // nothing matches; never planned as if the null were dropped
    let hot = enc(&Value::Utf8("t0"), &DataType::Utf8);
    let with_null = p
        .plan(
            std::slice::from_ref(&list),
            &Lookup::ContainsAll {
                column: "tags".into(),
                values: vec![hot.clone(), vec![0x00]],
            },
        )
        .unwrap();
    assert_eq!(with_null.row_groups_selected, 0);
    assert_eq!(
        with_null.residual,
        Some(bellwether_types::Predicate::ContainsAny {
            column: "tags".into(),
            values: Vec::new(),
        })
    );
    // ...while contains_any drops the null and keeps the other value
    let any_null = p
        .plan(
            std::slice::from_ref(&list),
            &Lookup::ContainsAny {
                column: "tags".into(),
                values: vec![hot, vec![0x00]],
            },
        )
        .unwrap();
    assert!(any_null.row_groups_selected > 0);
    // contains_any of nothing: no row group
    let plan = p
        .plan(
            std::slice::from_ref(&list),
            &Lookup::ContainsAny {
                column: "tags".into(),
                values: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(plan.row_groups_selected, 0);
    // a membership lookup on a column that is not a list: refused with why
    let e = p
        .plan(
            &[],
            &Lookup::ContainsAny {
                column: "id".into(),
                values: t0,
            },
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("id"), "{e}");
}
