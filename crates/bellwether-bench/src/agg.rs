//! `agg`: the ledger corpus for agg keys (B7): decimal amounts at scale 6
//! over skewed account keys, with null amounts, per-key sum workloads of
//! 10, 10^3, and 10^5 keys, and exact ground truth.
//!
//! # Layout
//!
//! `<out>/data/part-NNNN.parquet`, flat files, columns `account: Int64`
//! (never null), `version: Int64` (the row's position in the corpus) and
//! `amount: Decimal128(18, 6)` (nullable). The preset writes 100 files × 10
//! row groups × 10,000 rows: 10^7 rows.
//!
//! # Skewed keys
//!
//! Each row's account is drawn from a log-uniform rank over `accounts`
//! values: rank `r = floor((accounts + 1)^u) - 1` for uniform `u`, so
//! `P(r) ≈ 1 / ((r + 1) ln(accounts + 1))`. Account `r + 1` is the row's
//! key. The first accounts sit in every row group, the long tail in one or
//! none: the shape of a ledger where a few accounts carry most entries and
//! entries are not clustered by account. `null_fraction` of amounts are null
//! (ADR 014: counted, never summed). Non-null amounts are uniform in
//! `[-10,000, 10,000)` at scale 6.
//!
//! # Workloads and truth
//!
//! `workload/keys-<n>.parquet` for each `n` in `keys` holds `n` distinct
//! accounts drawn from the same skewed distribution as the rows (a
//! workload asks about the accounts that carry entries), in one `account`
//! column; a few may hold no row. `workload/agg.json` lists them;
//! `workload/truth.json` gives, per run, the rows of its accounts, how many
//! amounts are null, the exact sum of the rest at scale 6, and how many row
//! groups hold any of its accounts, computed while the rows are generated,
//! never from an index.
//!
//! # Memory (ADR 018)
//!
//! One row group's rows at a time, plus one entry per account named by a
//! run (at most the sum of `keys`).

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Builder, Int64Builder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::stats::chacha;
use crate::{io_err, write_json, BenchError, Result};

/// Format tag of `workload/agg.json`.
pub const AGG_WORKLOAD_FORMAT: &str = "bellwether-bench-agg-workload/1";
/// Format tag of `workload/truth.json`.
pub const AGG_TRUTH_FORMAT: &str = "bellwether-bench-agg-truth/1";
/// The amount column's decimal precision and scale.
pub const AGG_AMOUNT_TYPE: (u8, i8) = (18, 6);
/// Non-null amounts lie in `[-AGG_AMOUNT_BOUND, AGG_AMOUNT_BOUND)`, unscaled
/// (10,000 at scale 6).
pub const AGG_AMOUNT_BOUND: i128 = 10_000_000_000;

/// Parameters of an agg corpus. Everything written is a deterministic
/// function of them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggParams {
    /// RNG seed (ChaCha12 via [`crate::stats::chacha`]).
    pub seed: u64,
    /// Files.
    pub files: u32,
    /// Row groups per file.
    pub row_groups_per_file: u32,
    /// Rows per row group.
    pub rows_per_row_group: u32,
    /// Distinct accounts the rows draw from.
    pub accounts: u32,
    /// Fraction of null amounts, in `[0, 1]`.
    pub null_fraction: f64,
    /// Accounts per per-key sum run (each at most half of `accounts`).
    pub keys: Vec<u32>,
}

impl AggParams {
    /// The preset: 100 files × 10 row groups × 10,000 rows (10^7 rows) over
    /// 10^6 accounts, 2% null amounts, and runs of 10, 10^3, and 10^5
    /// accounts.
    #[must_use]
    pub fn preset() -> Self {
        Self {
            seed: 7,
            files: 100,
            row_groups_per_file: 10,
            rows_per_row_group: 10_000,
            accounts: 1_000_000,
            null_fraction: 0.02,
            keys: vec![10, 1_000, 100_000],
        }
    }

    /// Refuses parameters that make no sense, saying which.
    pub fn validate(&self) -> Result<()> {
        let bad = |m: String| Err(BenchError::Invalid(m));
        if self.files == 0 || self.row_groups_per_file == 0 || self.rows_per_row_group == 0 {
            return bad(
                "files, row groups per file, and rows per row group must be positive".into(),
            );
        }
        if self.accounts < 2 {
            return bad(format!(
                "accounts must be at least 2, not {}",
                self.accounts
            ));
        }
        if !(0.0..=1.0).contains(&self.null_fraction) {
            return bad(format!(
                "null_fraction must lie in [0, 1], not {}",
                self.null_fraction
            ));
        }
        if let Some(n) = self
            .keys
            .iter()
            .find(|n| **n == 0 || **n > self.accounts / 2)
        {
            return bad(format!(
                "a run of {n} accounts needs 1..={} (half the accounts)",
                self.accounts / 2
            ));
        }
        if self.keys.len() > 8 {
            return bad(format!("at most 8 runs, not {}", self.keys.len()));
        }
        Ok(())
    }
}

/// One recorded per-key sum run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggRun {
    /// Run id: `keys-<n>`.
    pub id: String,
    /// The key file, relative to the corpus directory.
    pub file: String,
    /// Distinct accounts in it.
    pub keys: u64,
}

/// `workload/agg.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggWorkload {
    /// [`AGG_WORKLOAD_FORMAT`].
    pub format: String,
    /// The corpus parameters.
    pub params: AggParams,
    /// The key column (`account`).
    pub key: String,
    /// The aggregated column (`amount`).
    pub value: String,
    /// The runs.
    pub runs: Vec<AggRun>,
}

/// The truth of one run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggRunTruth {
    /// The run id.
    pub id: String,
    /// Accounts of the run holding at least one row.
    pub present: u64,
    /// Rows of the run's accounts.
    pub rows: u64,
    /// Among them, rows whose amount is null.
    pub null_amounts: u64,
    /// The exact sum of the non-null amounts, at scale 6 (`-12.345678`).
    pub sum: String,
    /// Row groups holding a row of the run's accounts.
    pub row_groups: u64,
}

/// Summary of a generated corpus.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggSummary {
    /// Files written.
    pub files: u64,
    /// Row groups written.
    pub row_groups: u64,
    /// Rows written.
    pub rows: u64,
    /// Rows whose amount is null.
    pub null_amounts: u64,
    /// On-disk bytes of the data files.
    pub bytes: u64,
    /// Rows of the most frequent account (the skew's head).
    pub head_rows: u64,
}

/// `workload/truth.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggTruth {
    /// [`AGG_TRUTH_FORMAT`].
    pub format: String,
    /// The corpus.
    pub summary: AggSummary,
    /// Per run.
    pub runs: Vec<AggRunTruth>,
}

/// An unscaled amount at scale 6 as decimal text: `-12.345678`.
#[must_use]
pub fn decimal6(unscaled: i128) -> String {
    let sign = if unscaled < 0 { "-" } else { "" };
    let a = unscaled.unsigned_abs();
    format!("{sign}{}.{:06}", a / 1_000_000, a % 1_000_000)
}

/// The skewed account draw (module docs).
fn account(rng: &mut impl Rng, accounts: u32) -> i64 {
    let u: f64 = rng.gen();
    let r =
        ((f64::from(accounts) + 1.0).powf(u).floor() as i64 - 1).clamp(0, i64::from(accounts) - 1);
    r + 1
}

fn writer_properties(rows: u32) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(rows as usize + 1))
        .set_created_by("bellwether-bench agg".to_string())
        .build()
}

/// The corpus schema: `account`, `version`, `amount`.
#[must_use]
pub fn agg_schema() -> SchemaRef {
    let (p, s) = AGG_AMOUNT_TYPE;
    Arc::new(Schema::new(vec![
        Field::new("account", DataType::Int64, false),
        Field::new("version", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(p, s), true),
    ]))
}

#[derive(Default)]
struct RunAcc {
    rows: u64,
    nulls: u64,
    sum: i128,
    row_groups: u64,
    last_rg: Option<u64>,
}

/// Generates the corpus, its workload, and its truth under `out` (which
/// must not hold a corpus).
pub fn generate_agg(out: &Path, p: &AggParams) -> Result<AggTruth> {
    p.validate()?;
    if out.join("data").exists() {
        return Err(BenchError::Invalid(format!(
            "{} already holds a corpus; pass an empty --out",
            out.display()
        )));
    }
    // The runs first: their accounts depend on the parameters only. Each
    // account maps to the bit set of the runs naming it.
    let mut rng = chacha(&[p.seed, 0x4147_4752_0001]);
    let mut runs: Vec<(AggRun, Vec<i64>)> = Vec::new();
    let mut member: HashMap<i64, u8> = HashMap::new();
    for (i, &n) in p.keys.iter().enumerate() {
        let mut picked: HashSet<i64> = HashSet::with_capacity(n as usize);
        let budget = 1_000 * u64::from(n);
        let mut draws = 0u64;
        while picked.len() < n as usize {
            if draws == budget {
                return Err(BenchError::Invalid(format!(
                    "{budget} skewed draws found only {} distinct of {n} accounts; raise the \
                     accounts or lower the run",
                    picked.len()
                )));
            }
            draws += 1;
            picked.insert(account(&mut rng, p.accounts));
        }
        let mut keys: Vec<i64> = picked.into_iter().collect();
        keys.sort_unstable();
        for k in &keys {
            *member.entry(*k).or_default() |= 1 << i;
        }
        runs.push((
            AggRun {
                id: format!("keys-{n}"),
                file: format!("workload/keys-{n}.parquet"),
                keys: u64::from(n),
            },
            keys,
        ));
    }
    let mut acc: Vec<RunAcc> = runs.iter().map(|_| RunAcc::default()).collect();
    let mut seen: HashMap<i64, u8> = HashMap::new();
    let mut head_rows = 0u64;
    let schema = agg_schema();
    let props = writer_properties(p.rows_per_row_group);
    let mut summary = AggSummary {
        files: 0,
        row_groups: 0,
        rows: 0,
        null_amounts: 0,
        bytes: 0,
        head_rows: 0,
    };
    let (prec, scale) = AGG_AMOUNT_TYPE;
    for f in 0..p.files {
        let path = out.join("data").join(format!("part-{f:04}.parquet"));
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(io_err(dir))?;
        }
        let sink = BufWriter::new(File::create(&path).map_err(io_err(&path))?);
        let mut w = ArrowWriter::try_new(sink, Arc::clone(&schema), Some(props.clone()))?;
        for rg in 0..p.row_groups_per_file {
            let rg_id = u64::from(f * p.row_groups_per_file + rg);
            let first = rg_id * u64::from(p.rows_per_row_group);
            let mut rng = chacha(&[p.seed, 0x4147_4752_0002, u64::from(f), u64::from(rg)]);
            let n = p.rows_per_row_group as usize;
            let mut accounts = Int64Builder::with_capacity(n);
            let mut version = Int64Builder::with_capacity(n);
            let mut amount = Decimal128Builder::with_capacity(n);
            for i in 0..n {
                let a = account(&mut rng, p.accounts);
                let v = if rng.gen_bool(p.null_fraction) {
                    None
                } else {
                    Some(rng.gen_range(-AGG_AMOUNT_BOUND..AGG_AMOUNT_BOUND))
                };
                accounts.append_value(a);
                version.append_value((first + i as u64) as i64);
                amount.append_option(v);
                if a == 1 {
                    head_rows += 1;
                }
                if v.is_none() {
                    summary.null_amounts += 1;
                }
                if let Some(bits) = member.get(&a) {
                    *seen.entry(a).or_default() |= bits;
                    for (j, r) in acc.iter_mut().enumerate() {
                        if bits & (1 << j) == 0 {
                            continue;
                        }
                        r.rows += 1;
                        match v {
                            None => r.nulls += 1,
                            Some(x) => r.sum += x,
                        }
                        if r.last_rg != Some(rg_id) {
                            r.last_rg = Some(rg_id);
                            r.row_groups += 1;
                        }
                    }
                }
            }
            let columns: Vec<ArrayRef> = vec![
                Arc::new(accounts.finish()),
                Arc::new(version.finish()),
                Arc::new(amount.finish().with_precision_and_scale(prec, scale)?),
            ];
            w.write(&RecordBatch::try_new(Arc::clone(&schema), columns)?)?;
            w.flush()?;
            summary.rows += n as u64;
            summary.row_groups += 1;
        }
        w.close()?;
        summary.bytes += std::fs::metadata(&path).map_err(io_err(&path))?.len();
        summary.files += 1;
    }
    summary.head_rows = head_rows;
    for (run, keys) in &runs {
        write_accounts(&out.join(&run.file), keys)?;
    }
    let truth = AggTruth {
        format: AGG_TRUTH_FORMAT.into(),
        summary,
        runs: runs
            .iter()
            .zip(&acc)
            .enumerate()
            .map(|(j, ((run, keys), r))| AggRunTruth {
                id: run.id.clone(),
                present: keys
                    .iter()
                    .filter(|k| seen.get(k).is_some_and(|b| b & (1 << j) != 0))
                    .count() as u64,
                rows: r.rows,
                null_amounts: r.nulls,
                sum: decimal6(r.sum),
                row_groups: r.row_groups,
            })
            .collect(),
    };
    let workload = AggWorkload {
        format: AGG_WORKLOAD_FORMAT.into(),
        params: p.clone(),
        key: "account".into(),
        value: "amount".into(),
        runs: runs.into_iter().map(|(r, _)| r).collect(),
    };
    write_json(&out.join("workload").join("agg.json"), &workload)?;
    write_json(&out.join("workload").join("truth.json"), &truth)?;
    Ok(truth)
}

/// Writes a run's accounts: one `account: Int64` column.
fn write_accounts(path: &Path, keys: &[i64]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(io_err(dir))?;
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "account",
        DataType::Int64,
        false,
    )]));
    let mut b = Int64Builder::with_capacity(keys.len());
    b.append_slice(keys);
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(b.finish())])?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by("bellwether-bench agg".to_string())
        .build();
    let mut w = ArrowWriter::try_new(
        File::create(path).map_err(io_err(path))?,
        schema,
        Some(props),
    )?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Decimal128Array, Int64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    fn small() -> AggParams {
        AggParams {
            seed: 3,
            files: 3,
            row_groups_per_file: 4,
            rows_per_row_group: 500,
            accounts: 2_000,
            null_fraction: 0.05,
            keys: vec![10, 100],
        }
    }

    fn read_i64s(path: &Path) -> Vec<i64> {
        let r = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        r.flat_map(|b| {
            let b = b.unwrap();
            let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            c.values().to_vec()
        })
        .collect()
    }

    /// The truth matches the rows decoded back from the files: rows, null
    /// amounts, the exact sum and the row groups, per run.
    #[test]
    fn truth_matches_the_decoded_rows() {
        let dir = tempfile::tempdir().unwrap();
        let p = small();
        let truth = generate_agg(dir.path(), &p).unwrap();
        assert_eq!(truth.summary.rows, 6_000);
        assert_eq!(truth.summary.row_groups, 12);
        assert!(truth.summary.null_amounts > 0);
        for run in &truth.runs {
            let keys: HashSet<i64> =
                read_i64s(&dir.path().join(format!("workload/{}.parquet", run.id)))
                    .into_iter()
                    .collect();
            let (mut rows, mut nulls, mut sum, mut rgs) = (0u64, 0u64, 0i128, 0u64);
            let mut present = HashSet::new();
            for f in 0..p.files {
                let path = dir.path().join(format!("data/part-{f:04}.parquet"));
                let meta = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
                    .unwrap()
                    .metadata()
                    .clone();
                assert_eq!(meta.num_row_groups(), 4);
                for rg in 0..meta.num_row_groups() {
                    let reader =
                        ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
                            .unwrap()
                            .with_row_groups(vec![rg])
                            .with_batch_size(1 << 20)
                            .build()
                            .unwrap();
                    let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
                    assert_eq!(batches.len(), 1, "one batch per row group");
                    let b = &batches[0];
                    assert_eq!(b.num_rows(), 500);
                    let acc = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    let amt = b
                        .column(2)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap();
                    assert_eq!(amt.scale(), 6);
                    let mut hit = false;
                    for i in 0..b.num_rows() {
                        if keys.contains(&acc.value(i)) {
                            hit = true;
                            rows += 1;
                            present.insert(acc.value(i));
                            if amt.is_null(i) {
                                nulls += 1;
                            } else {
                                sum += amt.value(i);
                            }
                        }
                    }
                    rgs += u64::from(hit);
                }
            }
            assert_eq!(run.rows, rows, "{}", run.id);
            assert_eq!(run.null_amounts, nulls, "{}", run.id);
            assert_eq!(run.sum, decimal6(sum), "{}", run.id);
            assert_eq!(run.row_groups, rgs, "{}", run.id);
            assert_eq!(run.present, present.len() as u64, "{}", run.id);
        }
    }

    #[test]
    fn same_parameters_same_bytes_and_skewed_keys() {
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let p = small();
        let ta = generate_agg(a.path(), &p).unwrap();
        let tb = generate_agg(b.path(), &p).unwrap();
        assert_eq!(ta, tb);
        for rel in [
            "data/part-0001.parquet",
            "workload/keys-100.parquet",
            "workload/truth.json",
        ] {
            assert_eq!(
                std::fs::read(a.path().join(rel)).unwrap(),
                std::fs::read(b.path().join(rel)).unwrap(),
                "{rel}"
            );
        }
        // The head account carries far more than a uniform share.
        assert!(ta.summary.head_rows * u64::from(p.accounts) > 20 * ta.summary.rows);
    }

    #[test]
    fn refusals_say_what_is_wrong() {
        let mut p = small();
        p.keys = vec![1_001];
        assert!(p
            .validate()
            .unwrap_err()
            .to_string()
            .contains("half the accounts"));
        let mut p = small();
        p.null_fraction = 1.5;
        assert!(p
            .validate()
            .unwrap_err()
            .to_string()
            .contains("null_fraction"));
        let dir = tempfile::tempdir().unwrap();
        generate_agg(dir.path(), &small()).unwrap();
        let e = generate_agg(dir.path(), &small()).unwrap_err();
        assert!(e.to_string().contains("already holds a corpus"), "{e}");
    }

    #[test]
    fn decimal_text_at_scale_six() {
        assert_eq!(decimal6(0), "0.000000");
        assert_eq!(decimal6(-1), "-0.000001");
        assert_eq!(decimal6(12_345_678), "12.345678");
        assert_eq!(decimal6(-12_345_678), "-12.345678");
    }
}
