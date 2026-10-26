//! Pricing semi-join runs (B3): the two-step plan of a run whose keys are
//! values of the left dataset's join columns, read on both datasets.
//!
//! A [`Replayer`] over the left dataset prices semi-join runs only with
//! its right dataset attached ([`Replayer::with_pair`]): the right
//! dataset's own replayer (its catalog, current keys, and unserved
//! baseline) and, per join key being priced, its **transient pair index**,
//! built by the build crate from both datasets' complete exact keys on the
//! join columns with the same core function the index build uses
//! (`bellwether_core::join::build_pair`), so the price is the plan.
//!
//! Under a configuration holding the join key on the run's pair, a run
//! reads, on each side, the row groups holding a key both datasets hold
//! (with an `In` residual over those keys): what `semijoin(keys)` plans.
//! Under any other configuration each side is planned as a `locate` of
//! the keys by that side's keys (the left by the configuration, the right
//! by its current keys), and a side no key serves is priced as its files'
//! min/max statistics read it under [`crate::Baseline::ParquetStats`]: the
//! lookup is on a stored column value, so the statistics apply. A run's
//! cost is the sum of its two steps.

use std::collections::BTreeMap;

use bellwether_core::segment::{ColumnType, SegmentReader};
use bellwether_types::{Error, KeySpec, Predicate, Quantity, Result, RunCost, ScanPlan};
use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::baseline::{Baseline, BaselinePlanner};
use crate::keys::RunKeys;
use crate::plan::{run_cost, PlanSource};
use crate::replay::Replayer;
use crate::workload::{RunJoin, RunQuery};

/// The right dataset a left replayer prices semi-join runs with (B3).
pub struct PairInput<'a> {
    /// The right dataset's name: the folder of that name beside the data
    /// root.
    pub right: String,
    /// The right dataset's replayer.
    pub replayer: Replayer<'a>,
    /// Per join key priced (by [`KeySpec::identity`]), the bytes of its
    /// transient pair index.
    pub pairs: BTreeMap<String, Bytes>,
}

/// One run's cost over its two steps: files, row groups, and bytes summed;
/// rows summed when both steps count them.
#[must_use]
pub fn pair_cost(run_id: &str, keys: u64, left: &ScanPlan, right: &ScanPlan) -> RunCost {
    let (a, b) = (run_cost(run_id, keys, left), run_cost(run_id, keys, right));
    RunCost {
        files: a.files + b.files,
        row_groups: a.row_groups + b.row_groups,
        bytes: a.bytes + b.bytes,
        rows: match (&a.rows, &b.rows) {
            (Some(x), Some(y)) => Some(Quantity::exact(x.value + y.value)),
            _ => None,
        },
        ..a
    }
}

/// The join key of `specs` that answers a semi-join run keyed on `columns`
/// with right side `j`.
fn serving_join<'s>(specs: &'s [KeySpec], columns: &[String], j: &RunJoin) -> Option<&'s KeySpec> {
    specs
        .iter()
        .find(|s| crate::plan::serves_query(s, columns, &RunQuery::SemiJoin(j.clone())))
}

impl<'a> Replayer<'a> {
    /// Attaches the right dataset semi-join runs are priced with.
    #[must_use]
    pub fn with_pair(mut self, pair: PairInput<'a>) -> Self {
        self.pair = Some(Box::new(pair));
        self
    }

    /// The right dataset attached, or the refusal naming what is missing.
    fn pair_for(&self, j: &RunJoin) -> Result<&PairInput<'a>> {
        let pair = self.pair.as_deref().ok_or_else(|| Error::InvalidArgument {
            field: "join",
            reason: format!(
                "a semijoin run with {} is priced on both datasets, and no right dataset was \
                     given; name it (the folder {} beside the data root)",
                j.right, j.right
            ),
        })?;
        if pair.right != j.right {
            return Err(Error::InvalidArgument {
                field: "join",
                reason: format!(
                    "a semijoin run with {} cannot be priced against {}; name {} as the right \
                     dataset",
                    j.right, pair.right, j.right
                ),
            });
        }
        Ok(pair)
    }

    /// `keys` as the right dataset's join columns name them.
    fn right_keys(pair: &PairInput<'_>, keys: &RunKeys, j: &RunJoin) -> Result<RunKeys> {
        let schema = pair.replayer.schema();
        let types = j
            .right_columns
            .iter()
            .map(|c| {
                let f = schema.field_with_name(c).map_err(|_| {
                    Error::KeySpec(format!("join column {c:?} is not in {}", pair.right))
                })?;
                ColumnType::of(f.data_type())
            })
            .collect::<Result<Vec<_>>>()?;
        keys.relabel(&j.right_columns, &types)
    }

    /// The two plans of a semi-join run keyed on `keys` with right side `j`
    /// under the left configuration `specs` (module docs), and whether a
    /// side was priced by file statistics as unserved.
    pub(crate) fn semijoin_steps(
        &self,
        planner: &dyn PlanSource,
        specs: &[KeySpec],
        keys: &RunKeys,
        j: &RunJoin,
    ) -> Result<(ScanPlan, ScanPlan, bool)> {
        let pair = self.pair_for(j)?;
        if let Some(spec) = serving_join(specs, keys.columns(), j) {
            let (l, r) = self.pair_steps(pair, spec, keys)?;
            return Ok((l, r, false));
        }
        let (left, lu) = self.verified_plan(planner, specs, keys, &RunQuery::Locate)?;
        let rk = Self::right_keys(pair, keys, j)?;
        let rr = &pair.replayer;
        let rplanner = rr.planner_for(rr.current_specs())?;
        let (right, ru) = rr.verified_plan(
            rplanner.as_ref(),
            rr.current_specs(),
            &rk,
            &RunQuery::Locate,
        )?;
        Ok((left, right, lu || ru))
    }

    /// The two plans `semijoin(keys)` makes with the join key `spec`'s
    /// transient pair index: each side's row groups holding a key both
    /// datasets hold, with an `In` residual over those keys; exact rows per
    /// side when the key counts.
    fn pair_steps(
        &self,
        pair: &PairInput<'_>,
        spec: &KeySpec,
        keys: &RunKeys,
    ) -> Result<(ScanPlan, ScanPlan)> {
        let KeySpec::Join { right_columns, .. } = spec else {
            return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
        };
        let bytes = pair.pairs.get(&spec.identity()).ok_or_else(|| {
            Error::Other(format!(
                "no pair index was built for {}; a join key is priced from both datasets' \
                 complete exact keys",
                spec.name()
            ))
        })?;
        let reader = SegmentReader::open(bytes.clone())?;
        if reader.key_spec() != spec {
            return Err(Error::Other(format!(
                "the pair index given for {} indexes {}",
                spec.identity(),
                reader.key_spec().identity()
            )));
        }
        let all: Vec<usize> = (0..keys.columns().len()).collect();
        let probes = keys.all_probes(&all);
        let hits = bellwether_core::join::semijoin(std::slice::from_ref(&reader), &probes)?;
        let (mut lsel, mut rsel) = (RoaringBitmap::new(), RoaringBitmap::new());
        let (mut lrows, mut rrows) = (Some(0u64), Some(0u64));
        let mut matched = Vec::with_capacity(hits.len());
        for (k, v) in hits {
            lsel |= &v.left.bitmap;
            rsel |= &v.right.bitmap;
            lrows = lrows
                .zip(v.left.counts.as_ref())
                .map(|(a, c)| a + c.iter().sum::<u64>());
            rrows = rrows
                .zip(v.right.counts.as_ref())
                .map(|(a, c)| a + c.iter().sum::<u64>());
            matched.push(k);
        }
        let mut left = self.catalog().plan_for(&lsel)?;
        left.residual = Some(Predicate::In {
            columns: keys.columns().to_vec(),
            values: matched.clone(),
        });
        let mut right = pair.replayer.catalog().plan_for(&rsel)?;
        right.residual = Some(Predicate::In {
            columns: right_columns.clone(),
            values: matched,
        });
        if spec.counts() {
            left.rows = lrows.map(Quantity::exact);
            right.rows = rrows.map(Quantity::exact);
        }
        self.catalog().verify_plan(&left)?;
        pair.replayer.catalog().verify_plan(&right)?;
        Ok((left, right))
    }

    /// A semi-join run's cost under `specs`, and whether a side was priced
    /// by file statistics as unserved.
    pub(crate) fn semijoin_cost(
        &self,
        planner: &dyn PlanSource,
        specs: &[KeySpec],
        run_id: &str,
        keys: &RunKeys,
        j: &RunJoin,
    ) -> Result<(RunCost, bool)> {
        let (l, r, unserved) = self.semijoin_steps(planner, specs, keys, j)?;
        Ok((pair_cost(run_id, keys.len() as u64, &l, &r), unserved))
    }

    /// The two plans of a semi-join run keyed on `keys` with right side
    /// `j`, under `specs` (the left configuration), as costed: the left
    /// step, then the right.
    pub fn semijoin_plans(
        &self,
        specs: &[KeySpec],
        keys: &RunKeys,
        j: &RunJoin,
    ) -> Result<(ScanPlan, ScanPlan)> {
        let planner = self.planner_for(specs)?;
        let (l, r, _) = self.semijoin_steps(planner.as_ref(), specs, keys, j)?;
        Ok((l, r))
    }

    /// A semi-join run's two steps under a baseline without an index: the
    /// left through `left` (the run's baseline planner), the right through
    /// the same baseline over the right dataset.
    pub(crate) fn semijoin_baseline(
        &self,
        left: &BaselinePlanner<'_>,
        baseline: Baseline,
        keys: &RunKeys,
        j: &RunJoin,
    ) -> crate::ReplayResult<(ScanPlan, ScanPlan)> {
        let pair = self.pair_for(j)?;
        let l = left.plan(keys, None)?;
        let rk = Self::right_keys(pair, keys, j)?;
        let rc = pair.replayer.catalog();
        let right = match baseline {
            Baseline::RawScan => BaselinePlanner::raw_scan(rc),
            Baseline::ParquetStats => BaselinePlanner::parquet_stats(
                rc,
                &j.right_columns,
                pair.replayer.fingerprint_policy(),
            )?,
        };
        let r = right.plan(&rk, None)?;
        Ok((l, r))
    }

    /// The identities of the data a replay of `workload` reads: the left
    /// catalog digest, and the right's when a run is a semi-join priced
    /// with the right dataset.
    pub(crate) fn inputs(
        &self,
        workload: &crate::workload::Workload,
    ) -> Vec<bellwether_types::Hash> {
        let mut out = vec![self.catalog().digest()];
        let semijoin = workload
            .runs()
            .iter()
            .any(|r| r.query().is_ok_and(|q| q.join().is_some()));
        if let (true, Some(pair)) = (semijoin, self.pair.as_deref()) {
            out.push(pair.replayer.catalog().digest());
        }
        out
    }
}
