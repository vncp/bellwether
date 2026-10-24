//! `workload`: recorded incremental key sets.
//!
//! Each run models one incremental job: the entities re-versioned on a
//! window of consecutive days, starting at a random day and offset. Run sizes
//! are fixed so that the nearest-rank p50, p95, and max of the size list hit
//! the requested targets exactly (see [`run_sizes`]); the list is then
//! shuffled with the seed.
//!
//! Hashes: a run's hash is BLAKE3 over its sorted keys, each as 8 big-endian
//! bytes. The workload hash is BLAKE3 over the concatenated 32-byte run
//! hashes in run-id order. Neither depends on Parquet encoding.

use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_types::Hash;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use rand::seq::SliceRandom;
use rand::Rng;
use rand_chacha::ChaCha12Rng;
use serde::{Deserialize, Serialize};

use crate::corpus::{Corpus, GenParams};
use crate::stats::{chacha, nearest_rank_index};
use crate::{io_err, write_json, BenchError, Key, Result};

/// Manifest format tag written into `workload.json`.
pub const WORKLOAD_FORMAT: &str = "bellwether-bench-workload/1";

/// Workload parameters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadParams {
    /// RNG seed.
    pub seed: u64,
    /// Number of recorded runs.
    pub runs: u32,
    /// Target nearest-rank median keys per run.
    pub p50: u64,
    /// Target nearest-rank p95 keys per run.
    pub p95: u64,
    /// Target largest run.
    pub max: u64,
}

impl WorkloadParams {
    /// The pre-registered workload (docs/benchmark.md): 200 runs with
    /// key-set sizes at p50 / p95 / max of 50 / 10,000 / 100,000, seed 11.
    #[must_use]
    pub fn preregistered() -> Self {
        Self {
            seed: 11,
            runs: 200,
            p50: 50,
            p95: 10_000,
            max: 100_000,
        }
    }
}

impl Default for WorkloadParams {
    fn default() -> Self {
        Self {
            seed: 7,
            runs: 48,
            p50: 40,
            p95: 120_000,
            max: 415_000,
        }
    }
}

/// One recorded run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEntry {
    /// `run-NNNN`.
    pub run_id: String,
    /// Parquet file of keys, relative to the corpus root.
    pub file: String,
    /// Keys in the run.
    pub keys: u64,
    /// BLAKE3 of the sorted keys (8 big-endian bytes each).
    pub hash: Hash,
}

/// `workload.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadManifest {
    /// [`WORKLOAD_FORMAT`].
    pub format: String,
    /// The corpus the keys were drawn from.
    pub corpus_id: Hash,
    /// Parameters as requested.
    pub params: WorkloadParams,
    /// Distinct entities with at least one version in the corpus.
    pub available_entities: u64,
    /// Whether any requested size exceeded `available_entities` and was cut.
    pub clamped: bool,
    /// Achieved nearest-rank `[p50, p95, max]` of run sizes.
    pub sizes_p50_p95_max: [u64; 3],
    /// Runs in id order.
    pub runs: Vec<RunEntry>,
    /// BLAKE3 over the run hashes in id order.
    pub hash: Hash,
}

/// Run sizes in ascending order whose nearest-rank p50, p95, and max equal the
/// targets (after clamping to `available`). Values between anchors are
/// log-linearly interpolated from a smallest run of one key.
pub fn run_sizes(params: &WorkloadParams, available: u64) -> Result<Vec<u64>> {
    if params.runs == 0 {
        return Err(BenchError::Invalid("runs must be positive".into()));
    }
    if params.p50 == 0 || params.p50 > params.p95 || params.p95 > params.max {
        return Err(BenchError::Invalid(
            "need 1 <= p50 <= p95 <= max for run sizes".into(),
        ));
    }
    let n = params.runs as usize;
    let cap = |v: u64| v.min(available).max(1);
    // Later anchors win when indexes collide (tiny run counts).
    let mut anchors: Vec<(usize, u64)> = vec![(0, 1)];
    for (q, v) in [(0.5, params.p50), (0.95, params.p95), (1.0, params.max)] {
        let idx = nearest_rank_index(q, n);
        anchors.retain(|&(i, _)| i != idx);
        anchors.push((idx, cap(v)));
    }
    anchors.sort_unstable();
    let mut sizes = vec![0u64; n];
    for w in anchors.windows(2) {
        let (ia, va) = w[0];
        let (ib, vb) = w[1];
        let (la, lb) = ((va as f64).ln(), (vb as f64).ln());
        for (t, slot) in sizes.iter_mut().enumerate().take(ib + 1).skip(ia) {
            let frac = (t - ia) as f64 / (ib - ia) as f64;
            *slot = ((la + (lb - la) * frac).exp().round() as u64).clamp(va, vb);
        }
    }
    for &(i, v) in &anchors {
        sizes[i] = v;
    }
    if anchors.len() == 1 {
        sizes.fill(anchors[0].1);
    }
    Ok(sizes)
}

/// Distinct entities that have at least one version, computed from the row
/// model with a bitset of `entities` bits.
#[must_use]
pub fn available_entities(params: &GenParams) -> u64 {
    let mut bits = vec![0u64; params.entities.div_ceil(64) as usize];
    for day in params.layout() {
        let perm = day.permutation(params);
        for t in 0..day.active {
            let e = perm.apply(t);
            bits[(e / 64) as usize] |= 1 << (e % 64);
        }
    }
    bits.iter().map(|w| u64::from(w.count_ones())).sum()
}

/// BLAKE3 of sorted keys as 8 big-endian bytes each.
#[must_use]
pub fn hash_keys(sorted: &[Key]) -> Hash {
    let mut h = blake3::Hasher::new();
    for k in sorted {
        h.update(&k.to_be_bytes());
    }
    Hash(*h.finalize().as_bytes())
}

/// BLAKE3 over run hashes in order.
#[must_use]
pub fn hash_workload(runs: &[RunEntry]) -> Hash {
    let mut h = blake3::Hasher::new();
    for r in runs {
        h.update(&r.hash.0);
    }
    Hash(*h.finalize().as_bytes())
}

/// Draws `n` distinct keys: entities re-versioned on consecutive days from a
/// random start day and offset. Holds only this run's key set.
fn draw_keys(params: &GenParams, n: u64, rng: &mut ChaCha12Rng) -> Vec<Key> {
    let layout = params.layout();
    let days = layout.len();
    let start_day = rng.gen_range(0..days);
    let mut keys: HashSet<Key> = HashSet::with_capacity(n as usize);
    for step in 0..days {
        let day = &layout[(start_day + step) % days];
        let perm = day.permutation(params);
        let offset = if step == 0 {
            rng.gen_range(0..day.active)
        } else {
            0
        };
        for t in 0..day.active {
            if keys.len() as u64 >= n {
                break;
            }
            keys.insert(perm.apply((offset + t) % day.active));
        }
        if keys.len() as u64 >= n {
            break;
        }
    }
    let mut out: Vec<Key> = keys.into_iter().collect();
    out.sort_unstable();
    out
}

/// The key columns every recorded run supplies: run files hold `entity_id`
/// key sets, and nothing else.
pub const WORKLOAD_KEY_COLUMNS: &[&str] = &["entity_id"];

fn write_keys(path: &Path, keys: &[Key]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        WORKLOAD_KEY_COLUMNS[0],
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(keys.to_vec()))],
    )?;
    let file = File::create(path).map_err(io_err(path))?;
    let props = parquet::file::properties::WriterProperties::builder()
        .set_created_by(format!("bellwether-bench {}", env!("CARGO_PKG_VERSION")))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Reads a run's keys back, sorted and deduplicated.
pub fn read_keys(path: &Path) -> Result<Vec<Key>> {
    let file = File::open(path).map_err(io_err(path))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut keys = Vec::new();
    for batch in reader {
        let batch = batch?;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                BenchError::Invalid(format!("{}: keys are not uint64", path.display()))
            })?;
        keys.extend(col.iter().flatten());
        debug_assert_eq!(col.null_count(), 0);
    }
    keys.sort_unstable();
    keys.dedup();
    Ok(keys)
}

/// Writes `corpus/workload/run-NNNN.parquet` and `corpus/workload.json`.
pub fn generate_workload(corpus_root: &Path, params: &WorkloadParams) -> Result<WorkloadManifest> {
    let corpus = Corpus::open(corpus_root)?;
    let gen = &corpus.manifest.params;
    let available = available_entities(gen);
    let mut sizes = run_sizes(params, available)?;
    let clamped = [params.p50, params.p95, params.max]
        .iter()
        .any(|&v| v > available);
    let mut sorted = sizes.clone();
    sorted.sort_unstable();
    let n = sorted.len();
    let achieved = [
        sorted[nearest_rank_index(0.5, n)],
        sorted[nearest_rank_index(0.95, n)],
        sorted[n - 1],
    ];
    let mut rng = chacha(&[params.seed, gen.seed, 0x0005_70AD]);
    sizes.shuffle(&mut rng);

    let dir = corpus_root.join("workload");
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(io_err(&dir))?;
    }
    std::fs::create_dir_all(&dir).map_err(io_err(&dir))?;

    let mut runs = Vec::with_capacity(n);
    for (i, &size) in sizes.iter().enumerate() {
        let keys = draw_keys(gen, size, &mut rng);
        let run_id = format!("run-{i:04}");
        let file = format!("workload/{run_id}.parquet");
        write_keys(&corpus_root.join(&file), &keys)?;
        runs.push(RunEntry {
            run_id,
            file,
            keys: keys.len() as u64,
            hash: hash_keys(&keys),
        });
    }
    let manifest = WorkloadManifest {
        format: WORKLOAD_FORMAT.to_string(),
        corpus_id: corpus.id,
        params: params.clone(),
        available_entities: available,
        clamped,
        sizes_p50_p95_max: achieved,
        hash: hash_workload(&runs),
        runs,
    };
    write_json(&corpus_root.join("workload.json"), &manifest)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::quantile_sorted;

    #[test]
    fn run_sizes_hit_targets_exactly() {
        for runs in [20u32, 48, 100, 1000] {
            let p = WorkloadParams {
                runs,
                ..WorkloadParams::default()
            };
            let s = run_sizes(&p, u64::MAX).unwrap();
            assert!(s.windows(2).all(|w| w[0] <= w[1]));
            assert_eq!(quantile_sorted(&s, 0.5), Some(40));
            assert_eq!(quantile_sorted(&s, 0.95), Some(120_000));
            assert_eq!(s.last().copied(), Some(415_000));
        }
    }

    #[test]
    fn preregistered_workload_is_pinned() {
        let p = WorkloadParams::preregistered();
        assert_eq!(
            p,
            WorkloadParams {
                seed: 11,
                runs: 200,
                p50: 50,
                p95: 10_000,
                max: 100_000,
            }
        );
        let s = run_sizes(&p, u64::MAX).unwrap();
        assert_eq!(s.len(), 200);
        assert_eq!(quantile_sorted(&s, 0.5), Some(50));
        assert_eq!(quantile_sorted(&s, 0.95), Some(10_000));
        assert_eq!(s.last().copied(), Some(100_000));
        // The bound the harness states for its one-pass oracle.
        assert_eq!(s.iter().sum::<u64>(), 612_898);
    }

    #[test]
    fn run_sizes_clamp_to_available() {
        let s = run_sizes(&WorkloadParams::default(), 1000).unwrap();
        assert_eq!(s.last().copied(), Some(1000));
        assert_eq!(quantile_sorted(&s, 0.5), Some(40));
    }
}
