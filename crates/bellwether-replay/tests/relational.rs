//! Relational replay (WP15): consumer plans priced from the index and the
//! footers, checked against an independent oracle over the decoded rows.
//!
//! Two datasets beside each other: `events` (the main dataset: `k` with
//! nulls and skew, a version `t` with ties, a group `g`, a decimal at scale
//! 2, `x`) and `repos` (`k` restated one to three times, a version `ts`
//! with ties, a decimal at scale 4), the second indexed by the build crate
//! with a counted exact key and a temporal key. The oracle decodes every
//! row with the `parquet` crate, evaluates each plan node on the rows
//! (SQL semantics: a null key matches nothing, a null version never wins,
//! a null group is one group), and reads each column chunk's compressed
//! size from the footers. Every `Exact` quantity the replay states must
//! equal the oracle's; every `Bounded` interval must contain it; the bytes
//! read must cover every row group holding a row the plan needs (no false
//! negatives), and equal them where the plan's selection is by whole keys.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::type_complexity
)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_build::{BuildOptions, Builder};
use bellwether_core::encoding::{encode_value, Value};
use bellwether_ledger::Ledger;
use bellwether_replay::{
    apply_alternative, check_shape, load_plan, store_plan, write_replay, Alternative, KeyInput,
    MemoryKeySets, ReplayError, Replayer, RunKeys, RunQuery, WorkloadRecorder,
};
use bellwether_source::ParquetSource;
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::{
    AggregateFn, ConsumerPlan, Error, Exactness, Hash, JoinKind, KeySpec, KeysFrom, Measure,
    OperatorCost, PlanNode, Predicate, Proposal, ProposalBody, Quantity, Record, RewriteShape,
};
use common::Rng;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};

// ---------------------------------------------------------------- corpus

fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("t", DataType::Int64, true),
        Field::new("g", DataType::Utf8, true),
        Field::new("d2", DataType::Decimal128(18, 2), true),
        Field::new("x", DataType::Int64, false),
    ]))
}

fn repos_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("d4", DataType::Decimal128(18, 4), false),
    ]))
}

/// Writes `events` (3 files, row groups of 50) and `repos` (2 files, row
/// groups of 40).
fn write_corpus(events: &Path, repos: &Path) {
    let mut g = Rng(0x0015_c0de);
    std::fs::create_dir_all(events).unwrap();
    std::fs::create_dir_all(repos).unwrap();
    for f in 0..3 {
        let n = 400;
        let mut k = Vec::with_capacity(n);
        let mut t = Vec::with_capacity(n);
        let mut gs = Vec::with_capacity(n);
        let mut d = Vec::with_capacity(n);
        let mut x = Vec::with_capacity(n);
        for i in 0..n {
            let u = g.below(1000);
            k.push(if u < 30 {
                None
            } else {
                // Skewed: the low keys are busy.
                let v = g.below(100) * g.below(100) / 100;
                Some(v as i64)
            });
            // Versions with ties: a few values per key.
            t.push(if g.below(50) == 0 {
                None
            } else {
                Some(g.below(12) as i64)
            });
            gs.push(if g.below(40) == 0 {
                None
            } else {
                Some(format!("g{}", g.below(6)))
            });
            d.push(if g.below(30) == 0 {
                None
            } else {
                Some(g.below(100_000) as i128 - 20_000)
            });
            x.push((f * n + i) as i64);
        }
        let batch = RecordBatch::try_new(
            events_schema(),
            vec![
                Arc::new(Int64Array::from(k)) as ArrayRef,
                Arc::new(Int64Array::from(t)),
                Arc::new(StringArray::from(gs)),
                Arc::new(
                    Decimal128Array::from(d)
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                ),
                Arc::new(Int64Array::from(x)),
            ],
        )
        .unwrap();
        write(
            &events.join(format!("e-{f}.parquet")),
            &events_schema(),
            &batch,
            50,
        );
    }
    let mut rk = Vec::new();
    let mut rts = Vec::new();
    let mut rd = Vec::new();
    for key in 30..120i64 {
        let versions = 1 + g.below(3);
        let base = g.below(20) as i64;
        for v in 0..versions {
            rk.push(key);
            // The second version sometimes ties the first.
            rts.push(if v == 1 && g.below(3) == 0 {
                base
            } else {
                base + v as i64 * 7
            });
            rd.push(g.below(1_000_000) as i128);
        }
    }
    let half = rk.len() / 2;
    for (i, (lo, hi)) in [(0, half), (half, rk.len())].into_iter().enumerate() {
        let batch = RecordBatch::try_new(
            repos_schema(),
            vec![
                Arc::new(Int64Array::from(rk[lo..hi].to_vec())) as ArrayRef,
                Arc::new(Int64Array::from(rts[lo..hi].to_vec())),
                Arc::new(
                    Decimal128Array::from(rd[lo..hi].to_vec())
                        .with_precision_and_scale(18, 4)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        write(
            &repos.join(format!("r-{i}.parquet")),
            &repos_schema(),
            &batch,
            40,
        );
    }
}

fn write(path: &Path, schema: &SchemaRef, batch: &RecordBatch, per_rg: usize) {
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(per_rg))
        .build();
    let mut w = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        Arc::clone(schema),
        Some(props),
    )
    .unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
}

// ---------------------------------------------------------------- oracle

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Val {
    Null,
    I(i64),
    S(String),
    D(i128),
}

impl Val {
    fn key(&self) -> Option<i64> {
        match self {
            Val::I(v) => Some(*v),
            _ => None,
        }
    }
}

/// Every row of one dataset with its row group id, and every
/// (row group, column) chunk's compressed size from the footers.
struct Decoded {
    rows: Vec<(u32, BTreeMap<String, Val>)>,
    chunk: BTreeMap<(u32, String), u64>,
    rg_rows: BTreeMap<u32, u64>,
}

/// Decodes a dataset, naming each row group by the id `replayer`'s catalog
/// gives its (file, position).
fn decode(replayer: &Replayer<'_>) -> Decoded {
    let mut rows = Vec::new();
    let mut chunk = BTreeMap::new();
    let mut rg_rows = BTreeMap::new();
    let cat = replayer.catalog();
    let mut ids: BTreeMap<(String, u32), u32> = BTreeMap::new();
    for rg in cat.row_groups() {
        ids.insert(
            (cat.files()[rg.file].path.clone(), rg.meta.index_in_file),
            rg.id.0,
        );
    }
    let paths: BTreeSet<String> = cat.files().iter().map(|f| f.path.clone()).collect();
    for path in paths {
        let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
        let md = reader.metadata();
        for i in 0..md.num_row_groups() {
            let id = ids[&(path.clone(), i as u32)];
            let rgm = md.row_group(i);
            rg_rows.insert(id, rgm.num_rows() as u64);
            for c in 0..rgm.num_columns() {
                let col = rgm.column(c);
                chunk.insert(
                    (id, col.column_path().string()),
                    col.compressed_size() as u64,
                );
            }
        }
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap()).unwrap();
        let mut rg = 0usize;
        let mut left = md.row_group(0).num_rows() as usize;
        for batch in builder.with_batch_size(10_000).build().unwrap() {
            let batch = batch.unwrap();
            for r in 0..batch.num_rows() {
                while left == 0 {
                    rg += 1;
                    left = md.row_group(rg).num_rows() as usize;
                }
                left -= 1;
                let mut row = BTreeMap::new();
                for (f, a) in batch.schema().fields().iter().zip(batch.columns()) {
                    row.insert(f.name().clone(), value(a, r));
                }
                rows.push((ids[&(path.clone(), rg as u32)], row));
            }
        }
    }
    Decoded {
        rows,
        chunk,
        rg_rows,
    }
}

fn value(a: &ArrayRef, r: usize) -> Val {
    if a.is_null(r) {
        return Val::Null;
    }
    if let Some(x) = a.as_any().downcast_ref::<Int64Array>() {
        return Val::I(x.value(r));
    }
    if let Some(x) = a.as_any().downcast_ref::<StringArray>() {
        return Val::S(x.value(r).to_string());
    }
    if let Some(x) = a.as_any().downcast_ref::<Decimal128Array>() {
        return Val::D(x.value(r));
    }
    panic!("unexpected type {}", a.data_type());
}

/// One output tuple: per scan node, a row of its dataset.
type Tuple = BTreeMap<u32, usize>;

/// What the oracle derives for one node: its output tuples, its output
/// columns (name, scan, dataset column), the tuples it needs (an
/// aggregate's input; any other node's output), and its row count.
#[derive(Clone)]
struct Truth {
    out: Vec<Tuple>,
    cols: Vec<(String, u32, String)>,
    needs: Vec<Tuple>,
    rows: u64,
}

struct Oracle<'d> {
    data: BTreeMap<&'static str, &'d Decoded>,
    keys: BTreeSet<i64>,
    asof: i64,
}

impl Oracle<'_> {
    fn dataset_of(plan: &ConsumerPlan, scan: u32) -> &'static str {
        match &plan.nodes[scan as usize] {
            PlanNode::Scan { dataset, .. } if dataset == "main" => "main",
            _ => "repos",
        }
    }

    fn get<'a>(
        &'a self,
        plan: &ConsumerPlan,
        t: &Tuple,
        cols: &[(String, u32, String)],
        name: &str,
    ) -> &'a Val {
        let (_, scan, col) = cols.iter().find(|(n, _, _)| n == name).unwrap();
        let row = t[scan];
        &self.data[Self::dataset_of(plan, *scan)].rows[row].1[col]
    }

    fn run(&self, plan: &ConsumerPlan, order: &[u32]) -> Vec<Truth> {
        let mut out: Vec<Option<Truth>> = vec![None; plan.nodes.len()];
        for &v in order {
            let t = match &plan.nodes[v as usize] {
                PlanNode::Scan { dataset, columns } => {
                    let d = if dataset == "main" { "main" } else { "repos" };
                    let tuples: Vec<Tuple> = (0..self.data[d].rows.len())
                        .map(|i| BTreeMap::from([(v, i)]))
                        .collect();
                    Truth {
                        rows: tuples.len() as u64,
                        needs: tuples.clone(),
                        out: tuples,
                        cols: columns.iter().map(|c| (c.clone(), v, c.clone())).collect(),
                    }
                }
                PlanNode::SemiJoin {
                    input,
                    columns,
                    keys: KeysFrom::Run,
                } => {
                    let i = out[*input as usize].clone().unwrap();
                    self.keep(plan, i, |o, t, c| {
                        o.get(plan, t, c, &columns[0])
                            .key()
                            .is_some_and(|k| o.keys.contains(&k))
                    })
                }
                PlanNode::Window {
                    input,
                    partition_by,
                    keys: Some(KeysFrom::Run),
                } => {
                    let i = out[*input as usize].clone().unwrap();
                    self.keep(plan, i, |o, t, c| {
                        o.get(plan, t, c, &partition_by[0])
                            .key()
                            .is_some_and(|k| o.keys.contains(&k))
                    })
                }
                PlanNode::Window { input, .. } | PlanNode::Exchange { input, .. } => {
                    out[*input as usize].clone().unwrap()
                }
                PlanNode::Project { input, columns } => {
                    let mut i = out[*input as usize].clone().unwrap();
                    i.cols = columns
                        .iter()
                        .map(|c| i.cols.iter().find(|(n, _, _)| n == c).unwrap().clone())
                        .collect();
                    i
                }
                PlanNode::Filter { input, predicate } => {
                    let i = out[*input as usize].clone().unwrap();
                    match predicate {
                        Predicate::In { columns, values } => {
                            let set: BTreeSet<i64> = values.iter().map(|b| decode_int(b)).collect();
                            self.keep(plan, i, |o, t, c| {
                                o.get(plan, t, c, &columns[0])
                                    .key()
                                    .is_some_and(|k| set.contains(&k))
                            })
                        }
                        Predicate::Between { column, lo, hi } => {
                            // Canonical encodings are memcomparable: the
                            // row's value, encoded at its type, between the
                            // bounds' bytes.
                            self.keep(plan, i, |o, t, c| {
                                let enc = match o.get(plan, t, c, column) {
                                    Val::I(v) => int(*v),
                                    Val::D(v) => dec(*v, 2),
                                    _ => return false,
                                };
                                lo.as_slice() <= enc.as_slice() && enc.as_slice() <= hi.as_slice()
                            })
                        }
                        p => panic!("oracle does not evaluate {p:?}"),
                    }
                }
                PlanNode::Latest { input, columns, ts } | PlanNode::AsOf { input, columns, ts } => {
                    let asof = matches!(plan.nodes[v as usize], PlanNode::AsOf { .. })
                        .then_some(self.asof);
                    let i = out[*input as usize].clone().unwrap();
                    let mut best: BTreeMap<Vec<Val>, i64> = BTreeMap::new();
                    for t in &i.out {
                        let key: Vec<Val> = columns
                            .iter()
                            .map(|c| self.get(plan, t, &i.cols, c).clone())
                            .collect();
                        if let Some(at) = self.get(plan, t, &i.cols, ts).key() {
                            if asof.is_none_or(|a| at <= a) {
                                let e = best.entry(key).or_insert(at);
                                *e = (*e).max(at);
                            }
                        }
                    }
                    self.keep(plan, i, |o, t, c| {
                        let key: Vec<Val> = columns
                            .iter()
                            .map(|x| o.get(plan, t, c, x).clone())
                            .collect();
                        let at = o.get(plan, t, c, ts).key();
                        at.is_some() && best.get(&key).copied() == at
                    })
                }
                PlanNode::Join {
                    left,
                    right,
                    on,
                    kind,
                } => {
                    let (l, r) = (
                        out[*left as usize].clone().unwrap(),
                        out[*right as usize].clone().unwrap(),
                    );
                    let mut by: BTreeMap<i64, Vec<&Tuple>> = BTreeMap::new();
                    for t in &r.out {
                        if let Some(k) = self.get(plan, t, &r.cols, &on[0].1).key() {
                            by.entry(k).or_default().push(t);
                        }
                    }
                    let mut tuples = Vec::new();
                    for t in &l.out {
                        let matches = self
                            .get(plan, t, &l.cols, &on[0].0)
                            .key()
                            .and_then(|k| by.get(&k));
                        match (matches, kind) {
                            (Some(ms), _) => {
                                for m in ms {
                                    let mut j = t.clone();
                                    j.extend(m.iter().map(|(a, b)| (*a, *b)));
                                    tuples.push(j);
                                }
                            }
                            (None, JoinKind::Left) => tuples.push(t.clone()),
                            (None, JoinKind::Inner) => {}
                        }
                    }
                    let mut cols = l.cols.clone();
                    for c in r.cols {
                        if !on.iter().any(|(a, b)| a == b && *b == c.0) {
                            cols.push(c);
                        }
                    }
                    Truth {
                        rows: tuples.len() as u64,
                        needs: tuples.clone(),
                        out: tuples,
                        cols,
                    }
                }
                PlanNode::Aggregate {
                    input, group_by, ..
                } => {
                    let i = out[*input as usize].clone().unwrap();
                    let groups: BTreeSet<Vec<Val>> = i
                        .out
                        .iter()
                        .map(|t| {
                            group_by
                                .iter()
                                .map(|c| self.get(plan, t, &i.cols, c).clone())
                                .collect()
                        })
                        .collect();
                    let rows = if group_by.is_empty() {
                        1
                    } else {
                        groups.len() as u64
                    };
                    Truth {
                        out: Vec::new(),
                        needs: i.out.clone(),
                        cols: group_by
                            .iter()
                            .map(|c| i.cols.iter().find(|(n, _, _)| n == c).unwrap().clone())
                            .collect(),
                        rows,
                    }
                }
                n => panic!("oracle does not evaluate {n:?}"),
            };
            out[v as usize] = Some(t);
        }
        out.into_iter().map(Option::unwrap).collect()
    }

    fn keep(
        &self,
        plan: &ConsumerPlan,
        i: Truth,
        f: impl Fn(&Self, &Tuple, &[(String, u32, String)]) -> bool,
    ) -> Truth {
        let _ = plan;
        let out: Vec<Tuple> = i
            .out
            .iter()
            .filter(|t| f(self, t, &i.cols))
            .cloned()
            .collect();
        Truth {
            rows: out.len() as u64,
            needs: out.clone(),
            out,
            cols: i.cols,
        }
    }
}

fn int(v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value(&Value::Int(v), &mut out).unwrap();
    out
}

fn decode_int(b: &[u8]) -> i64 {
    (-5000..5000).find(|v| int(*v) == b).unwrap()
}

fn dec(v: i128, scale: i8) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value(&Value::Decimal128 { value: v, scale }, &mut out).unwrap();
    out
}

/// Whether `q` holds `truth`: equal when exact, inside when bounded.
fn holds(q: &Quantity, truth: u64) -> bool {
    match &q.exactness {
        Exactness::Exact => q.value == truth,
        Exactness::Bounded { low, high } => *low <= truth && truth <= *high && q.value == *high,
        Exactness::Measured { .. } => false,
    }
}

// ---------------------------------------------------------------- setup

struct World {
    _dir: tempfile::TempDir,
    events: String,
    esrc: ParquetSource,
    rsrc: ParquetSource,
    rstore: Arc<ObjectStoreBackend>,
}

fn world() -> World {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let (events, repos) = (root.join("events"), root.join("repos"));
    write_corpus(&events, &repos);
    let rstore = Arc::new(ObjectStoreBackend::memory().unwrap());
    Builder::new(Arc::clone(&rstore), "idx")
        .build(
            repos.to_str().unwrap(),
            &[exact("k", true), temporal("k", "ts", true)],
            &BuildOptions {
                schema: Some(repos_schema()),
                max_segment_row_groups: 2,
                ..BuildOptions::default()
            },
        )
        .unwrap();
    World {
        _dir: dir,
        events: events.to_str().unwrap().to_string(),
        esrc: ParquetSource::new().with_schema(events_schema()),
        rsrc: ParquetSource::new().with_schema(repos_schema()),
        rstore,
    }
}

impl World {
    fn replayer(&self) -> Replayer<'_> {
        let snap = Snapshot::open(Arc::clone(&self.rstore), "idx").unwrap();
        let repos = Replayer::for_snapshot(&self.rsrc, &snap).unwrap();
        Replayer::unindexed(
            &self.esrc,
            &self.events,
            &bellwether_source::Partitioning::None,
        )
        .unwrap()
        .with_datasets(BTreeMap::from([("repos".to_string(), repos)]))
        .unwrap()
    }
}

fn exact(c: &str, counts: bool) -> KeySpec {
    KeySpec::Exact {
        columns: vec![c.into()],
        counts,
        bounds: None,
    }
}

fn temporal(c: &str, ts: &str, counts: bool) -> KeySpec {
    KeySpec::Temporal {
        columns: vec![c.into()],
        ts_column: ts.into(),
        counts,
        tombstone: None,
    }
}

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

fn run_keys(r: &Replayer<'_>, keys: &BTreeSet<i64>) -> RunKeys {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(keys.iter().copied().collect::<Vec<_>>())) as ArrayRef],
    )
    .unwrap();
    let mut b = bellwether_replay::RunKeysBuilder::new(&cols(&["k"]), r.schema()).unwrap();
    b.push_batch(&batch).unwrap();
    b.finish()
}

fn scan_events() -> PlanNode {
    PlanNode::Scan {
        dataset: "main".into(),
        columns: cols(&["k", "t", "g", "d2", "x"]),
    }
}

fn scan_repos() -> PlanNode {
    PlanNode::Scan {
        dataset: "repos".into(),
        columns: cols(&["k", "ts", "d4"]),
    }
}

fn semi(input: u32) -> PlanNode {
    PlanNode::SemiJoin {
        input,
        columns: cols(&["k"]),
        keys: KeysFrom::Run,
    }
}

/// Checks every node of `plan` priced under `specs` against the oracle.
/// Returns the priced plan, or the refusal.
fn check(
    r: &Replayer<'_>,
    main: &Decoded,
    repos: &Decoded,
    plan: &ConsumerPlan,
    specs: &[KeySpec],
    keys: &BTreeSet<i64>,
    asof: i64,
) -> Result<bellwether_replay::PricedPlan, ReplayError> {
    let rk = run_keys(r, keys);
    let priced = r.price_plan(plan, specs, "run", &rk, Some(asof))?;
    let order = check_shape(plan).unwrap();
    let oracle = Oracle {
        data: BTreeMap::from([("main", main), ("repos", repos)]),
        keys: keys.clone(),
        asof,
    };
    let truth = oracle.run(plan, &order);
    for op in &priced.operators {
        let t = &truth[op.node as usize];
        assert!(
            holds(&op.rows_out, t.rows),
            "node {} ({}) rows {:?} do not hold the truth {} under {specs:?} with keys \
             {keys:?}\nplan {plan:?}",
            op.node,
            op.op,
            op.rows_out,
            t.rows
        );
        if op.op != "exchange" {
            assert_eq!(op.bytes.exactness, Exactness::Exact, "node {}", op.node);
        }
    }
    // No false negatives: the run reads every row group holding a row the
    // root needs, at the projection the plan checked.
    let checked = r.check_plan(plan).unwrap();
    let root = &truth[plan.root as usize];
    let mut minimal = 0u64;
    let mut cells: BTreeSet<(u32, u32)> = BTreeSet::new();
    for t in &root.needs {
        for (scan, row) in t {
            let d = if Oracle::dataset_of(plan, *scan) == "main" {
                main
            } else {
                repos
            };
            cells.insert((*scan, d.rows[*row].0));
        }
    }
    for (scan, rg) in &cells {
        let d = if Oracle::dataset_of(plan, *scan) == "main" {
            main
        } else {
            repos
        };
        for c in checked.projection(*scan).unwrap() {
            minimal += d.chunk[&(*rg, c.clone())];
        }
    }
    assert!(
        priced.cost.bytes >= minimal,
        "the run reads {} bytes, below the {minimal} its needed rows sit in\nplan {plan:?}",
        priced.cost.bytes
    );
    assert!(priced.cost.row_groups >= cells.len() as u64);
    assert!(holds(priced.cost.rows.as_ref().unwrap(), root.rows));
    Ok(priced)
}

// ---------------------------------------------------------------- tests

/// The property: random plans of depth up to 4 over both datasets, random
/// key sets (absent keys and the null-key rows included), under every
/// configuration: each Exact quantity equals the oracle, each Bounded
/// interval contains it, reads cover the needed rows, and a join whose main
/// side has no exact or temporal key on `k` is refused, never priced.
#[test]
fn random_plans_are_exact_or_bounded_against_the_oracle() {
    let w = world();
    let r = w.replayer();
    let main = decode(&r);
    let repos = decode(r.dataset("repos").unwrap());
    let configs: Vec<Vec<KeySpec>> = vec![
        vec![],
        vec![exact("k", true)],
        vec![exact("k", false)],
        vec![KeySpec::Bloom {
            columns: cols(&["k"]),
            fpr: 0.05,
            bounds: None,
        }],
        vec![temporal("k", "t", true)],
        vec![exact("k", true), KeySpec::Range { column: "t".into() }],
        vec![temporal("k", "t", false), exact("g", true)],
    ];
    let mut g = Rng(0x00ab_cdef);
    let mut priced = 0;
    let mut refused = 0;
    // Exact and bounded claims both occur, on joins and elsewhere.
    let (mut exact_joins, mut bounded_nodes) = (0, 0);
    for case in 0..160 {
        let keys: BTreeSet<i64> = (0..1 + g.below(25)).map(|_| g.below(140) as i64).collect();
        let plan = random_plan(&mut g);
        let specs = &configs[(case as usize) % configs.len()];
        let asof = g.below(25) as i64;
        match check(&r, &main, &repos, &plan, specs, &keys, asof) {
            Ok(p) => {
                priced += 1;
                for o in &p.operators {
                    match o.rows_out.exactness {
                        Exactness::Exact if o.op == "join" => exact_joins += 1,
                        Exactness::Bounded { .. } => bounded_nodes += 1,
                        _ => {}
                    }
                }
            }
            Err(ReplayError::JoinNotIndexed { side, .. }) => {
                assert!(
                    !specs.iter().any(|s| matches!(s,
                        KeySpec::Exact { columns, .. } | KeySpec::Temporal { columns, .. }
                            if columns == &cols(&["k"]))),
                    "a join with a keyed main side was refused ({side})\nplan {plan:?}"
                );
                refused += 1;
            }
            Err(e) => panic!("case {case}: {e}\nplan {plan:?}"),
        }
    }
    assert!(
        priced > 100 && refused > 0,
        "{priced} priced, {refused} refused"
    );
    assert!(
        exact_joins > 0 && bounded_nodes > 0,
        "{exact_joins} exact joins, {bounded_nodes} bounded nodes"
    );
}

/// A plan of up to four steps over `events`, possibly joined with `repos`
/// and aggregated.
fn random_plan(g: &mut Rng) -> ConsumerPlan {
    let mut nodes = vec![scan_events()];
    let mut top = 0u32;
    let mut have_g = true;
    let steps = g.below(4);
    for _ in 0..steps {
        let node = match g.below(8) {
            0 => semi(top),
            1 => PlanNode::Filter {
                input: top,
                predicate: Predicate::In {
                    columns: cols(&["k"]),
                    values: (0..1 + g.below(6))
                        .map(|_| int(g.below(120) as i64))
                        .collect(),
                },
            },
            2 if g.below(2) == 0 => {
                let lo = g.below(10) as i64;
                PlanNode::Filter {
                    input: top,
                    predicate: Predicate::Between {
                        column: "t".into(),
                        lo: int(lo),
                        hi: int(lo + g.below(5) as i64),
                    },
                }
            }
            2 => {
                // A decimal range at the column's scale (2).
                let lo = g.below(60_000) as i128 - 20_000;
                PlanNode::Filter {
                    input: top,
                    predicate: Predicate::Between {
                        column: "d2".into(),
                        lo: dec(lo, 2),
                        hi: dec(lo + g.below(20_000) as i128, 2),
                    },
                }
            }
            3 => PlanNode::Latest {
                input: top,
                columns: cols(&["k"]),
                ts: "t".into(),
            },
            4 => PlanNode::AsOf {
                input: top,
                columns: cols(&["k"]),
                ts: "t".into(),
            },
            5 => PlanNode::Window {
                input: top,
                partition_by: cols(&["k"]),
                keys: Some(KeysFrom::Run),
            },
            6 if have_g => {
                have_g = false;
                PlanNode::Project {
                    input: top,
                    columns: cols(&["k", "t", "d2"]),
                }
            }
            _ => PlanNode::Exchange {
                input: top,
                by: cols(&["k"]),
            },
        };
        nodes.push(node);
        top = nodes.len() as u32 - 1;
    }
    if g.below(2) == 0 {
        nodes.push(scan_repos());
        let mut right = nodes.len() as u32 - 1;
        if g.below(2) == 0 {
            nodes.push(semi(right));
            right = nodes.len() as u32 - 1;
        }
        nodes.push(PlanNode::Join {
            left: top,
            right,
            on: vec![("k".into(), "k".into())],
            kind: if g.below(3) == 0 {
                JoinKind::Left
            } else {
                JoinKind::Inner
            },
        });
        top = nodes.len() as u32 - 1;
    }
    match g.below(3) {
        0 => {
            let by = if have_g && g.below(2) == 0 { "g" } else { "k" };
            nodes.push(PlanNode::Aggregate {
                input: top,
                group_by: if g.below(5) == 0 {
                    Vec::new()
                } else {
                    cols(&[by])
                },
                measures: vec![
                    Measure {
                        function: AggregateFn::Count,
                        column: None,
                    },
                    Measure {
                        function: AggregateFn::Sum,
                        column: Some("d2".into()),
                    },
                ],
            });
            top = nodes.len() as u32 - 1;
        }
        _ => {}
    }
    ConsumerPlan {
        version: 1,
        nodes,
        root: top,
    }
}

fn plan(nodes: Vec<PlanNode>) -> ConsumerPlan {
    ConsumerPlan {
        version: 1,
        root: nodes.len() as u32 - 1,
        nodes,
    }
}

fn join_plan(left_semi: bool, right_semi: bool) -> ConsumerPlan {
    let mut nodes = vec![scan_events()];
    let mut left = 0;
    if left_semi {
        nodes.push(semi(0));
        left = 1;
    }
    nodes.push(scan_repos());
    let mut right = nodes.len() as u32 - 1;
    if right_semi {
        nodes.push(semi(right));
        right += 1;
    }
    nodes.push(PlanNode::Join {
        left,
        right,
        on: vec![("k".into(), "k".into())],
        kind: JoinKind::Inner,
    });
    plan(nodes)
}

fn root_rows(p: &bellwether_replay::PricedPlan) -> &Quantity {
    p.cost.rows.as_ref().unwrap()
}

fn keys_some() -> BTreeSet<i64> {
    [2, 3, 31, 33, 40, 55, 77, 119, 130].into_iter().collect()
}

/// An inner join of two scans' rows selected by whole keys, both counted,
/// is exact: Σ count_L × count_R over the common keys. With counts on one
/// side only it is bounded (deliberate break: price it Exact).
#[test]
fn join_cardinality_is_exact_only_with_counts_on_both_sides() {
    let w = world();
    let r = w.replayer();
    let (main, repos) = (decode(&r), decode(r.dataset("repos").unwrap()));
    let p = join_plan(true, false);
    let both = check(&r, &main, &repos, &p, &[exact("k", true)], &keys_some(), 0).unwrap();
    assert_eq!(root_rows(&both).exactness, Exactness::Exact);
    let one = check(&r, &main, &repos, &p, &[exact("k", false)], &keys_some(), 0).unwrap();
    assert!(
        matches!(root_rows(&one).exactness, Exactness::Bounded { .. }),
        "counts on one side only: {:?}",
        root_rows(&one)
    );
    // Both are the same plan's reads.
    assert_eq!(both.cost.bytes, one.cost.bytes);
}

/// A join fed by a semi-join of the run's keys counts only the common keys
/// among them (deliberate break: drop the intersection with the run keys).
#[test]
fn a_semijoin_fed_join_counts_only_the_run_keys() {
    let w = world();
    let r = w.replayer();
    let (main, repos) = (decode(&r), decode(r.dataset("repos").unwrap()));
    let p = join_plan(true, false);
    let narrow: BTreeSet<i64> = [31, 32].into_iter().collect();
    let got = check(&r, &main, &repos, &p, &[exact("k", true)], &narrow, 0).unwrap();
    let wide = check(
        &r,
        &main,
        &repos,
        &join_plan(false, false),
        &[exact("k", true)],
        &narrow,
        0,
    )
    .unwrap();
    assert_eq!(root_rows(&got).exactness, Exactness::Exact);
    assert!(root_rows(&got).value < root_rows(&wide).value);
    assert!(got.cost.bytes < wide.cost.bytes);
}

/// Without counts the `in` rule's lower end is the (key, row group) pairs
/// the exact key holds, never the rows of the planned row groups
/// (deliberate break: take rows(rg) as the lower bound).
#[test]
fn the_rows_of_planned_row_groups_are_never_a_lower_bound() {
    let w = world();
    let r = w.replayer();
    let (main, repos) = (decode(&r), decode(r.dataset("repos").unwrap()));
    let p = plan(vec![scan_events(), semi(0)]);
    let keys: BTreeSet<i64> = [60, 61].into_iter().collect();
    let got = check(&r, &main, &repos, &p, &[exact("k", false)], &keys, 0).unwrap();
    let Exactness::Bounded { low, high } = root_rows(&got).exactness else {
        panic!("{:?}", root_rows(&got))
    };
    let in_rgs: u64 = main
        .rows
        .iter()
        .filter(|(_, row)| row["k"].key().is_some_and(|k| keys.contains(&k)))
        .map(|(rg, _)| *rg)
        .collect::<BTreeSet<_>>()
        .iter()
        .map(|rg| main.rg_rows[rg])
        .sum();
    assert_eq!(high, in_rgs);
    assert!(
        low < in_rgs,
        "low {low} must be the pairs, not the rows {in_rgs}"
    );
    let truth = main
        .rows
        .iter()
        .filter(|(_, row)| row["k"].key().is_some_and(|k| keys.contains(&k)))
        .count() as u64;
    assert!(low <= truth && truth < in_rgs);
}

/// A scan reads the columns the nodes above it reference: an aggregate's
/// measure column is read, a column nothing references is not (deliberate
/// break: forget a measure column in the projection).
#[test]
fn the_scan_projection_holds_every_referenced_column_and_no_other() {
    let w = world();
    let r = w.replayer();
    let (main, repos) = (decode(&r), decode(r.dataset("repos").unwrap()));
    let p = plan(vec![
        scan_events(),
        semi(0),
        PlanNode::Aggregate {
            input: 1,
            group_by: cols(&["k"]),
            measures: vec![Measure {
                function: AggregateFn::Sum,
                column: Some("d2".into()),
            }],
        },
    ]);
    let checked = r.check_plan(&p).unwrap();
    assert_eq!(
        checked.projection(0).unwrap(),
        cols(&["k", "d2"]).as_slice()
    );
    let keys = keys_some();
    let got = check(&r, &main, &repos, &p, &[exact("k", true)], &keys, 0).unwrap();
    // Exact: the bytes of k and d2 over the row groups holding a key.
    let rgs: BTreeSet<u32> = main
        .rows
        .iter()
        .filter(|(_, row)| row["k"].key().is_some_and(|k| keys.contains(&k)))
        .map(|(rg, _)| *rg)
        .collect();
    let want: u64 = rgs
        .iter()
        .map(|rg| main.chunk[&(*rg, "k".to_string())] + main.chunk[&(*rg, "d2".to_string())])
        .sum();
    assert_eq!(got.cost.bytes, want);
    assert_eq!(got.cost.row_groups, rgs.len() as u64);
    // Groups: the keys present, exact from the exact key.
    assert_eq!(got.operators[2].rows_out.exactness, Exactness::Exact);
}

/// The window prefilter reads exactly the row groups holding the
/// partitions of interest, and a range filter the row groups whose bounds
/// overlap, with a range key equal to the footers' answer here.
#[test]
fn window_and_range_read_what_the_oracle_needs() {
    let w = world();
    let r = w.replayer();
    let (main, repos) = (decode(&r), decode(r.dataset("repos").unwrap()));
    let keys = keys_some();
    let p = plan(vec![
        scan_events(),
        PlanNode::Window {
            input: 0,
            partition_by: cols(&["k"]),
            keys: Some(KeysFrom::Run),
        },
    ]);
    let got = check(&r, &main, &repos, &p, &[exact("k", true)], &keys, 0).unwrap();
    let rgs: BTreeSet<u32> = main
        .rows
        .iter()
        .filter(|(_, row)| row["k"].key().is_some_and(|k| keys.contains(&k)))
        .map(|(rg, _)| *rg)
        .collect();
    assert_eq!(got.cost.row_groups, rgs.len() as u64);
    assert_eq!(root_rows(&got).exactness, Exactness::Exact);
    let between = plan(vec![
        scan_events(),
        PlanNode::Filter {
            input: 0,
            predicate: Predicate::Between {
                column: "x".into(),
                lo: int(120),
                hi: int(260),
            },
        },
    ]);
    let stats = check(&r, &main, &repos, &between, &[], &keys, 0).unwrap();
    let ranged = check(
        &r,
        &main,
        &repos,
        &between,
        &[KeySpec::Range { column: "x".into() }],
        &keys,
        0,
    )
    .unwrap();
    // x is clustered: rows 120..=260 sit in four row groups of 50.
    assert_eq!(stats.cost.row_groups, 4);
    assert_eq!(ranged.cost.row_groups, 4);
    assert!(matches!(
        root_rows(&stats).exactness,
        Exactness::Bounded { low: 0, .. }
    ));
}

/// `latest` through the temporal key reads the winners' row groups and
/// bounds its rows by the keys with a live version and their counted rows.
#[test]
fn latest_through_a_temporal_key_is_bounded_by_keys_and_counts() {
    let w = world();
    let r = w.replayer();
    let (main, repos) = (decode(&r), decode(r.dataset("repos").unwrap()));
    let p = plan(vec![
        scan_repos(),
        semi(0),
        PlanNode::Latest {
            input: 1,
            columns: cols(&["k"]),
            ts: "ts".into(),
        },
    ]);
    let keys: BTreeSet<i64> = (30..120).collect();
    let got = check(&r, &main, &repos, &p, &[], &keys, 0).unwrap();
    let Exactness::Bounded { low, .. } = got.operators[2].rows_out.exactness else {
        // Exact only when no key ties at its newest version.
        return;
    };
    assert_eq!(low, 90, "every repository has a version");
    // Fewer row groups than the semi-join reads: the winners only.
    assert!(got.operators[2].bytes.value <= got.operators[1].bytes.value);
}

/// A key committed by the build crate (read by range through the pinned
/// snapshot) and the same key as transient segments price a plan alike;
/// a dataset with no key on the join column refuses the join, naming it.
#[test]
fn committed_and_transient_indexes_price_alike() {
    let w = world();
    let r = w.replayer();
    let p = join_plan(true, false);
    let rk = run_keys(&r, &keys_some());
    let transient = r
        .price_plan(&p, &[exact("k", true)], "run", &rk, None)
        .unwrap();
    let estore = Arc::new(ObjectStoreBackend::memory().unwrap());
    Builder::new(Arc::clone(&estore), "idx")
        .build(
            &w.events,
            &[exact("k", true)],
            &BuildOptions {
                schema: Some(events_schema()),
                max_segment_row_groups: 4,
                ..BuildOptions::default()
            },
        )
        .unwrap();
    let esnap = Snapshot::open(Arc::clone(&estore), "idx").unwrap();
    let rsnap = Snapshot::open(Arc::clone(&w.rstore), "idx").unwrap();
    let committed = Replayer::for_snapshot(&w.esrc, &esnap)
        .unwrap()
        .with_datasets(BTreeMap::from([(
            "repos".to_string(),
            Replayer::for_snapshot(&w.rsrc, &rsnap).unwrap(),
        )]))
        .unwrap();
    let got = committed
        .price_plan(&p, committed.current_specs(), "run", &rk, None)
        .unwrap();
    assert_eq!(got.cost, transient.cost);
    assert_eq!(got.operators, transient.operators);
    // The same repos as an unindexed folder: no key on k, so refused.
    let folder = std::path::Path::new(&r.dataset("repos").unwrap().catalog().files()[0].path)
        .parent()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let bare =
        Replayer::unindexed(&w.rsrc, &folder, &bellwether_source::Partitioning::None).unwrap();
    let main = Replayer::unindexed(&w.esrc, &w.events, &bellwether_source::Partitioning::None)
        .unwrap()
        .with_datasets(BTreeMap::from([("repos".to_string(), bare)]))
        .unwrap();
    let e = main
        .price_plan(&p, &[exact("k", true)], "run", &rk, None)
        .unwrap_err();
    assert!(
        matches!(e, ReplayError::JoinNotIndexed { side: "right", .. }),
        "{e}"
    );
    assert!(e.to_string().contains("exact:k"), "{e}");
}

/// Every malformed plan is refused before anything is priced, naming the
/// node.
#[test]
fn malformed_plans_are_refused_by_node() {
    let w = world();
    let r = w.replayer();
    let rk = run_keys(&r, &keys_some());
    let refused = |p: ConsumerPlan, want: &str| {
        let e = r.price_plan(&p, &[], "run", &rk, None).unwrap_err();
        assert!(e.to_string().contains(want), "{e} (wanted {want:?})");
    };
    refused(
        ConsumerPlan {
            version: 0,
            nodes: vec![scan_events()],
            root: 0,
        },
        "not a plan version",
    );
    assert!(matches!(
        r.price_plan(
            &ConsumerPlan {
                version: 2,
                nodes: vec![scan_events()],
                root: 0
            },
            &[],
            "run",
            &rk,
            None
        ),
        Err(ReplayError::Engine(Error::UnsupportedVersion { .. }))
    ));
    refused(
        ConsumerPlan {
            version: 1,
            nodes: vec![scan_events()],
            root: 3,
        },
        "is not a node",
    );
    refused(plan(vec![scan_events(), semi(5)]), "reads node 5");
    // A cycle: 1 reads 2 reads 1.
    refused(
        ConsumerPlan {
            version: 1,
            nodes: vec![scan_events(), semi(2), semi(1)],
            root: 1,
        },
        "cycle",
    );
    // A shared input.
    refused(
        plan(vec![
            scan_events(),
            semi(0),
            PlanNode::Join {
                left: 0,
                right: 1,
                on: vec![("k".into(), "k".into())],
                kind: JoinKind::Inner,
            },
        ]),
        "feeds two nodes",
    );
    refused(
        ConsumerPlan {
            version: 1,
            nodes: vec![scan_events(), scan_repos()],
            root: 0,
        },
        "is not read by the root",
    );
    refused(
        plan(vec![PlanNode::Scan {
            dataset: "users".into(),
            columns: cols(&["k"]),
        }]),
        "--dataset users=",
    );
    refused(
        plan(vec![PlanNode::Scan {
            dataset: "main".into(),
            columns: cols(&["nope"]),
        }]),
        "nope",
    );
    refused(
        plan(vec![
            scan_events(),
            PlanNode::Project {
                input: 0,
                columns: cols(&["k"]),
            },
            semi(1),
            PlanNode::Filter {
                input: 2,
                predicate: Predicate::Between {
                    column: "t".into(),
                    lo: int(1),
                    hi: int(2),
                },
            },
        ]),
        "is not an output of node 2",
    );
    // Both sides name `t`/`ts` differently, but `d2` and `d4` are both
    // decimals at different scales: they never encode alike.
    refused(
        plan(vec![
            scan_events(),
            scan_repos(),
            PlanNode::Join {
                left: 0,
                right: 1,
                on: vec![("d2".into(), "d4".into())],
                kind: JoinKind::Inner,
            },
        ]),
        "never encode",
    );
    refused(
        plan(vec![
            scan_events(),
            PlanNode::Filter {
                input: 0,
                predicate: Predicate::Text {
                    column: "g".into(),
                    operator: bellwether_types::TextOp::Contains,
                    values: vec!["x".into()],
                },
            },
        ]),
        "text",
    );
    // An as_of node with no instant on the run.
    refused(
        plan(vec![
            scan_events(),
            PlanNode::AsOf {
                input: 0,
                columns: cols(&["k"]),
                ts: "t".into(),
            },
        ]),
        "records no instant",
    );
    // A join of a join's output is not priced as a scan.
    let nested = plan(vec![
        scan_events(),
        scan_repos(),
        PlanNode::Join {
            left: 0,
            right: 1,
            on: vec![("k".into(), "k".into())],
            kind: JoinKind::Inner,
        },
        scan_repos(),
        PlanNode::Join {
            left: 2,
            right: 3,
            on: vec![("k".into(), "k".into())],
            kind: JoinKind::Inner,
        },
    ]);
    assert!(matches!(
        r.price_plan(&nested, &[exact("k", true)], "run", &rk, None),
        Err(ReplayError::JoinNotIndexed {
            side: "left",
            derived: Some(_),
            ..
        })
    ));
}

/// A plan over a dataset whose costs a model measures is measured
/// throughout, names the model, and a replay that marks its cost exact is
/// refused by the ledger (rule 7b; deliberate break: mark it Exact).
#[test]
fn a_plan_over_a_measured_dataset_is_measured_and_the_ledger_holds_it_so() {
    let w = world();
    let snap = Snapshot::open(Arc::clone(&w.rstore), "idx").unwrap();
    let repos = Replayer::for_snapshot(&w.rsrc, &snap)
        .unwrap()
        .with_measured("measured: test model");
    let r = Replayer::unindexed(&w.esrc, &w.events, &bellwether_source::Partitioning::None)
        .unwrap()
        .with_datasets(BTreeMap::from([("repos".to_string(), repos)]))
        .unwrap();
    let p = join_plan(true, false);
    let rk = run_keys(&r, &keys_some());
    let got = r
        .price_plan(&p, &[exact("k", true)], "run", &rk, None)
        .unwrap();
    assert_eq!(got.model.as_deref(), Some("measured: test model"));
    assert!(!got.cost.exact);
    assert!(matches!(got.cost.exactness, Exactness::Measured { .. }));
    // The events-only scan below the join stays exact.
    assert_eq!(got.operators[0].bytes.exactness, Exactness::Exact);
    assert!(matches!(
        got.operators[3].rows_out.exactness,
        Exactness::Measured { .. }
    ));

    // A rewrite between two plans over the measured dataset, scored and
    // written; then the same record with its measured costs relabelled
    // exact is refused.
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let ledger = Ledger::new(Arc::clone(&store));
    let keys = MemoryKeySets::new();
    let workload = record(&r, &keys, &[("a", &keys_some())], None);
    let before = plan(vec![scan_repos()]);
    let after = plan(vec![scan_repos(), semi(0)]);
    let body = ProposalBody::Rewrite {
        shape: shape(before, after),
    };
    let proposal = put_proposal(&ledger, body.clone());
    let scored = r
        .score(
            &proposal,
            &body,
            &workload,
            &keys,
            "2026-10-26T00:00:00.000Z",
        )
        .unwrap();
    assert!(scored.replay.model.is_some());
    write_replay(&ledger, &scored).unwrap();
    let mut forged = scored.replay.clone();
    for c in forged.current.iter_mut().chain(forged.proposed.iter_mut()) {
        c.exact = true;
        c.exactness = Exactness::Exact;
    }
    forged.model = None;
    let e = ledger.put(&Record::Replay(forged)).unwrap_err();
    assert!(e.to_string().contains("rule 7b"), "{e}");
}

fn shape(before: ConsumerPlan, after: ConsumerPlan) -> RewriteShape {
    RewriteShape {
        before: "scan then join".into(),
        after: "semi-join first".into(),
        invariant: "the joined rows of the run's keys are unchanged".into(),
        pairs: Vec::new(),
        key: Vec::new(),
        keys: None,
        before_plan: Some(before),
        after_plan: Some(after),
    }
}

fn put_proposal(ledger: &Ledger<Arc<ObjectStoreBackend>>, body: ProposalBody) -> Hash {
    // A proposal cites an Evidence; any recorded evidence does.
    let evidence = ledger
        .put(&Record::Evidence(bellwether_types::Evidence {
            record_version: bellwether_types::format::RECORD_VERSION,
            provenance: bellwether_types::Provenance {
                inputs: vec![Hash::of(b"data")],
                oldest_input_age: std::time::Duration::ZERO,
                manifest_version: Some("0".into()),
                created_at: "2026-10-26T00:00:00.000Z".into(),
            },
            body: bellwether_types::EvidenceBody::Plan {
                plan: bellwether_types::ScanPlan {
                    manifest_version: "0".into(),
                    entries: Vec::new(),
                    residual: None,
                    bytes: 0,
                    row_groups_total: 0,
                    row_groups_selected: 0,
                    projected_bytes: None,
                    order: None,
                    rows: None,
                },
            },
        }))
        .unwrap();
    ledger
        .put(&Record::Proposal(Proposal {
            record_version: bellwether_types::format::RECORD_VERSION,
            evidence: vec![evidence],
            proposed_by: "test".into(),
            created_at: "2026-10-26T00:00:00.000Z".into(),
            body,
        }))
        .unwrap()
}

fn record(
    r: &Replayer<'_>,
    keys: &MemoryKeySets,
    runs: &[(&str, &BTreeSet<i64>)],
    with: Option<&ConsumerPlan>,
) -> bellwether_replay::Workload {
    let mut rec = WorkloadRecorder::new(keys, Arc::clone(r.schema()));
    if let Some(p) = with {
        rec = rec.with_plan(p).unwrap();
    }
    for (id, ks) in runs {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(ks.iter().copied().collect::<Vec<_>>())) as ArrayRef],
        )
        .unwrap();
        rec.record_query(id, KeyInput::Batch(&batch), RunQuery::Locate, None)
            .unwrap();
    }
    rec.finish().unwrap()
}

/// A rewrite with typed plans is replayed: current is the before plan per
/// run, proposed the after plan, both exact over files; its per-node costs
/// are listed per run (before then after); the ledger keeps it. A rewrite
/// in prose has no replay.
#[test]
fn a_rewrite_with_plans_is_replayed_and_one_in_prose_is_not() {
    let w = world();
    let r = w.replayer();
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let ledger = Ledger::new(Arc::clone(&store));
    let keys = MemoryKeySets::new();
    let a: BTreeSet<i64> = [31, 40].into_iter().collect();
    let b: BTreeSet<i64> = [55, 56, 57, 130].into_iter().collect();
    let workload = record(&r, &keys, &[("a", &a), ("b", &b)], None);
    let before = join_plan(false, true);
    let after = join_plan(true, true);
    let body = ProposalBody::Rewrite {
        shape: shape(before.clone(), after.clone()),
    };
    let proposal = put_proposal(&ledger, body.clone());
    // Under a counted exact key on events: the replay's main configuration
    // is the current one, so index events by a key first (transient: the
    // unindexed replayer has none, so price the plans as they stand).
    let scored = r
        .score(
            &proposal,
            &body,
            &workload,
            &keys,
            "2026-10-26T00:00:00.000Z",
        )
        .unwrap_err();
    // The current configuration of events has no key on k: the join is
    // refused, never priced as a scan.
    assert!(matches!(
        scored,
        ReplayError::JoinNotIndexed { side: "left", .. }
    ));
    // Index events (committed) and score again.
    let estore = Arc::new(ObjectStoreBackend::memory().unwrap());
    Builder::new(Arc::clone(&estore), "idx")
        .build(
            &w.events,
            &[exact("k", true)],
            &BuildOptions {
                schema: Some(events_schema()),
                max_segment_row_groups: 3,
                ..BuildOptions::default()
            },
        )
        .unwrap();
    let esnap = Snapshot::open(Arc::clone(&estore), "idx").unwrap();
    let rsnap = Snapshot::open(Arc::clone(&w.rstore), "idx").unwrap();
    let r = Replayer::for_snapshot(&w.esrc, &esnap)
        .unwrap()
        .with_datasets(BTreeMap::from([(
            "repos".to_string(),
            Replayer::for_snapshot(&w.rsrc, &rsnap).unwrap(),
        )]))
        .unwrap();
    let workload = record(&r, &keys, &[("a", &a), ("b", &b)], None);
    let scored = r
        .score(
            &proposal,
            &body,
            &workload,
            &keys,
            "2026-10-26T00:00:00.000Z",
        )
        .unwrap();
    let rep = &scored.replay;
    assert!(rep.model.is_none());
    assert!(rep.current.iter().chain(&rep.proposed).all(|c| c.exact));
    let per_run = before.nodes.len() + after.nodes.len();
    assert_eq!(rep.operators.len(), 2 * per_run);
    for (i, run) in [&a, &b].into_iter().enumerate() {
        let rk = run_keys(&r, run);
        let id = ["a", "b"][i];
        let pb = r
            .price_plan(&before, r.current_specs(), id, &rk, None)
            .unwrap();
        let pa = r
            .price_plan(&after, r.current_specs(), id, &rk, None)
            .unwrap();
        assert_eq!(rep.current[i], pb.cost);
        assert_eq!(rep.proposed[i], pa.cost);
        let block: Vec<OperatorCost> = pb.operators.into_iter().chain(pa.operators).collect();
        assert_eq!(
            &rep.operators[i * per_run..(i + 1) * per_run],
            block.as_slice()
        );
        // The semi-join pushed below the join reads less on the left.
        assert!(pa.cost.bytes <= pb.cost.bytes);
    }
    assert_eq!(rep.inputs.len(), 2, "both datasets are cited");
    write_replay(&ledger, &scored).unwrap();
    // Determinism: the same inputs give the same record.
    let again = r
        .score(
            &proposal,
            &body,
            &workload,
            &keys,
            "2026-10-26T00:00:00.000Z",
        )
        .unwrap();
    assert_eq!(again.replay, scored.replay);

    let prose = ProposalBody::Rewrite {
        shape: RewriteShape {
            before_plan: None,
            after_plan: None,
            ..shape(before, after)
        },
    };
    let h = put_proposal(&ledger, prose.clone());
    assert!(matches!(
        r.score(&h, &prose, &workload, &keys, "2026-10-26T00:00:00.000Z"),
        Err(ReplayError::RewriteNotScorable)
    ));
}

/// A plan workload: runs name the plan's hash, a key proposal on a column
/// the plan probes is scored through the plan with every run's per-node
/// costs, the plan is loaded back canonically, and a stored plan with a
/// field this build does not read is refused.
#[test]
fn plan_workloads_record_score_and_load() {
    let w = world();
    let r = w.replayer();
    let keys = MemoryKeySets::new();
    let p = join_plan(true, false);
    let a: BTreeSet<i64> = [31, 40, 41].into_iter().collect();
    let workload = record(&r, &keys, &[("a", &a)], Some(&p));
    let h = workload.runs()[0].plan.unwrap();
    assert_eq!(load_plan(&keys, &h).unwrap(), p);
    // A recorded run with a window and a plan is refused.
    let mut bad = workload.manifest().runs[0].clone();
    bad.query_kind = "window".into();
    bad.window = Some(bellwether_replay::RunWindow {
        ts_column: "t".into(),
        lo: 0,
        hi: 1,
    });
    assert!(bad.query().is_err());
    // A plan whose stored bytes carry an unknown field: refused.
    let mut json: serde_json::Value = serde_json::to_value(&p).unwrap();
    json["hint"] = serde_json::json!("x");
    let bytes = serde_json::to_vec(&json).unwrap();
    let forged = Hash::of(&bytes);
    bellwether_replay::KeySetSink::put(&keys, &forged, bytes::Bytes::from(bytes)).unwrap();
    assert!(load_plan(&keys, &forged)
        .unwrap_err()
        .to_string()
        .contains("canonical"));
    assert_eq!(store_plan(&keys, &p).unwrap(), h);

    // A key proposal on a column the plan probes, over a plan the current
    // configuration (no key on events) can price: a semi-join and an
    // aggregate of events.
    let agg = plan(vec![
        scan_events(),
        semi(0),
        PlanNode::Aggregate {
            input: 1,
            group_by: cols(&["k"]),
            measures: vec![Measure {
                function: AggregateFn::Count,
                column: None,
            }],
        },
    ]);
    let workload = record(&r, &keys, &[("a", &a)], Some(&agg));
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let ledger = Ledger::new(Arc::clone(&store));
    let body = ProposalBody::Key {
        spec: exact("k", true),
        sidecars: Vec::new(),
    };
    let proposal = put_proposal(&ledger, body.clone());
    let scored = r
        .score(
            &proposal,
            &body,
            &workload,
            &keys,
            "2026-10-26T00:00:00.000Z",
        )
        .unwrap();
    let rep = &scored.replay;
    assert_eq!(rep.operators.len(), 2 * agg.nodes.len());
    assert!(rep.proposed[0].bytes < rep.current[0].bytes);
    // Counted: the proposed rows are exact, the current ones bounded.
    assert_eq!(
        rep.proposed[0].rows.as_ref().unwrap().exactness,
        Exactness::Exact
    );
    write_replay(&ledger, &scored).unwrap();
    // A key on a column the plan never probes serves no run.
    let other = ProposalBody::Key {
        spec: exact("x", false),
        sidecars: Vec::new(),
    };
    let h = put_proposal(&ledger, other.clone());
    assert!(matches!(
        r.score(&h, &other, &workload, &keys, "2026-10-26T00:00:00.000Z"),
        Err(ReplayError::SpecServesNoRun { .. })
    ));
}

/// Counterfactuals over plan nodes: ablating the semi-join below a join
/// gives the delta to the unfiltered plan, and the interaction of two
/// ablations is the inclusion-exclusion of four exact replays.
#[test]
fn plan_node_counterfactuals_are_exact_replays() {
    let w = world();
    let estore = Arc::new(ObjectStoreBackend::memory().unwrap());
    Builder::new(Arc::clone(&estore), "idx")
        .build(
            &w.events,
            &[exact("k", true)],
            &BuildOptions {
                schema: Some(events_schema()),
                ..BuildOptions::default()
            },
        )
        .unwrap();
    let esnap = Snapshot::open(Arc::clone(&estore), "idx").unwrap();
    let rsnap = Snapshot::open(Arc::clone(&w.rstore), "idx").unwrap();
    let r = Replayer::for_snapshot(&w.esrc, &esnap)
        .unwrap()
        .with_datasets(BTreeMap::from([(
            "repos".to_string(),
            Replayer::for_snapshot(&w.rsrc, &rsnap).unwrap(),
        )]))
        .unwrap();
    let keys = MemoryKeySets::new();
    let a: BTreeSet<i64> = [31, 40].into_iter().collect();
    let b: BTreeSet<i64> = [2, 90].into_iter().collect();
    let workload = record(&r, &keys, &[("a", &a), ("b", &b)], None);
    let p = join_plan(true, true); // 0 scan, 1 semi, 2 scan, 3 semi, 4 join
    let d = r
        .replace_node(&p, 1, &Alternative::Ablate, &workload, &keys)
        .unwrap();
    let without = apply_alternative(&p, 1, &Alternative::Ablate).unwrap();
    assert_eq!(without.nodes.len(), 4);
    let cf = r.replay_plan(&without, &workload, &keys).unwrap();
    for (x, c) in d.iter().zip(&cf) {
        assert_eq!(&x.counterfactual, c);
        assert!(
            x.delta.bytes <= 0,
            "removing the semi-join never reads less"
        );
    }
    let inter = r
        .interaction(
            &p,
            (1, &Alternative::Ablate),
            (3, &Alternative::Ablate),
            &workload,
            &keys,
        )
        .unwrap();
    let base = r.replay_plan(&p, &workload, &keys).unwrap();
    let u = r
        .replay_plan(
            &apply_alternative(&p, 1, &Alternative::Ablate).unwrap(),
            &workload,
            &keys,
        )
        .unwrap();
    let v = r
        .replay_plan(
            &apply_alternative(&p, 3, &Alternative::Ablate).unwrap(),
            &workload,
            &keys,
        )
        .unwrap();
    let uv = r
        .replay_plan(&join_plan(false, false), &workload, &keys)
        .unwrap();
    for (i, x) in inter.iter().enumerate() {
        let want = i128::from(uv[i].bytes) - i128::from(u[i].bytes) - i128::from(v[i].bytes)
            + i128::from(base[i].bytes);
        assert_eq!(x.interaction.bytes, want);
    }
    // A scan cannot be ablated; a replacement node is priced.
    assert!(r
        .replace_node(&p, 0, &Alternative::Ablate, &workload, &keys)
        .is_err());
    let replaced = r
        .replace_node(
            &p,
            4,
            &Alternative::Replace(PlanNode::Join {
                left: 1,
                right: 3,
                on: vec![("k".into(), "k".into())],
                kind: JoinKind::Left,
            }),
            &workload,
            &keys,
        )
        .unwrap();
    assert_eq!(replaced.len(), 2);
}
