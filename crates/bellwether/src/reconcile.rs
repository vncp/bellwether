//! `reconcile`: compare two folders by key (WP8) and write the Certificate.

use bellwether_types::{AggregateFn, Error, KeySpec, Result};
use bellwether_verify::{
    CertificateContext, CoverageRequest, IndexPlanner, ManifestAnchor, Options, ReconcileSpec,
    Reconciliation, Side, TotalColumn,
};
use serde::{Deserialize, Serialize};

use std::time::Duration;

use bellwether_source_sql::{is_sql_root, SqlUrl, TableRun};
use bellwether_types::Source;
use bellwether_verify::AgedInput;

use std::sync::Arc;

use bellwether_source_sql::SqlSource;

use crate::engine::{folder_changes, guard, Engine, EngineOptions, Pinned, TableSource};
use crate::results::Written;

/// How to reconcile two folders.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReconcileRequest {
    /// Key columns, in tuple order.
    pub key: Vec<String>,
    /// Compare the newest version per key by this column (a null never
    /// wins, ADR 014).
    pub latest_by: Option<String>,
    /// Columns left out of the row hash; listed on the certificate.
    pub ignore: Vec<String>,
    /// Columns totalled exactly: `col` for a decimal column, or
    /// `col:dec(S)` for a float column quantized at scale `S` (each value's
    /// exact binary value rounded half to even, then summed as an `i128`;
    /// NaN or infinity is an error naming the key). Quantized totals are
    /// labeled `col:dec(S)` in the outcome and the certificate.
    pub totals: Vec<String>,
    /// Columns grouping the totals.
    pub group_by: Vec<String>,
    /// Compare a deterministic sample of keys: `(fraction, seed)`.
    pub sample: Option<(f64, u64)>,
    /// Bytes the comparison state may occupy (the verifier's
    /// `Options::memory_budget`); `None` keeps its default, 256 MiB. A
    /// larger budget means fewer passes over both sides: the result's
    /// `outcome.memory` reports the passes and the rows read per side.
    #[serde(default)]
    pub memory_budget: Option<u64>,
}

/// What [`Engine::reconcile`] found.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReconcileResult {
    /// The Certificate (written even when it fails).
    pub certificate: Written,
    /// The typed outcome: counts, the first mismatches, totals, sides.
    pub outcome: Reconciliation,
    /// Each side that is a table (ADR 027): its units and rows, and the
    /// snapshot the run held or the re-read of every unit's identity that
    /// stood in for one. Empty (and omitted) when both sides are folders.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<TableRun>,
    /// `R-agg-1` (B7): each folder side read to compare per-key rows and
    /// one exact decimal total, where an agg key on the key and total
    /// would have answered it from its index. Empty (and omitted)
    /// otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lints: Vec<bellwether_types::Lint>,
}

/// A reconcile request the engine would refuse for what it asks, not for
/// what the data holds: a usage problem a caller can fix by changing the
/// request. Found by [`Engine::reconcile_problems`] before any row is read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReconcileProblem {
    /// A `totals` column is not a decimal, and was not given a quantization
    /// scale. Totals are exact; a float column is totalled only as
    /// `col:dec(S)`, each value rounded half to even at scale `S`.
    TotalNotDecimal {
        /// The column.
        column: String,
        /// Its dataset type, as Arrow prints it.
        data_type: String,
        /// Whether it is a floating-point column (the usual case: total it
        /// as `col:dec(S)`).
        float: bool,
    },
    /// A `totals` entry asks to quantize (`col:dec(S)`) a column that is not
    /// a float.
    QuantizeNotFloat {
        /// The column.
        column: String,
        /// Its dataset type, as Arrow prints it.
        data_type: String,
        /// The scale asked for.
        scale: u8,
    },
    /// A `totals` entry is neither `col` nor `col:dec(S)` with `S` in
    /// `0..=38`.
    TotalSpecInvalid {
        /// The entry as given.
        given: String,
        /// One sentence saying what is wrong and the accepted form.
        reason: String,
    },
    /// A `totals` column is not in the left side's dataset schema.
    TotalNotFound {
        /// The column.
        column: String,
    },
    /// The memory budget is zero.
    ZeroMemoryBudget,
}

impl Engine {
    /// The usage problems in `request`, checked against the data root's
    /// dataset schema before any row is read: every `totals` entry must
    /// parse (`col` or `col:dec(S)`), name a column of the left side, and be
    /// a decimal column, or a float column quantized with `:dec(S)`; a
    /// memory budget must be positive. Empty when the request is well
    /// formed. `reconcile` refuses the same requests (as engine errors); a
    /// caller that tells usage from engine failures (the CLI's exit 2
    /// against 4) asks here first.
    pub fn reconcile_problems(&self, request: &ReconcileRequest) -> Result<Vec<ReconcileProblem>> {
        guard(|| {
            let mut out = Vec::new();
            if request.memory_budget == Some(0) {
                out.push(ReconcileProblem::ZeroMemoryBudget);
            }
            if request.totals.is_empty() {
                return Ok(out);
            }
            let pinned = self.pin()?;
            let schema = self.source(pinned.as_ref())?.schema(self.root())?;
            for given in &request.totals {
                let t = match TotalColumn::parse(given) {
                    Ok(t) => t,
                    Err(reason) => {
                        out.push(ReconcileProblem::TotalSpecInvalid {
                            given: given.clone(),
                            reason,
                        });
                        continue;
                    }
                };
                let Ok(f) = schema.field_with_name(&t.column) else {
                    out.push(ReconcileProblem::TotalNotFound { column: t.column });
                    continue;
                };
                let dt = f.data_type();
                match t.quantize {
                    None if !totalable(dt) => out.push(ReconcileProblem::TotalNotDecimal {
                        column: t.column,
                        data_type: dt.to_string(),
                        float: floating(dt),
                    }),
                    Some(scale) if !floating(dt) => {
                        out.push(ReconcileProblem::QuantizeNotFloat {
                            column: t.column,
                            data_type: dt.to_string(),
                            scale,
                        });
                    }
                    _ => {}
                }
            }
            Ok(out)
        })
    }

    /// Reconciles the engine's data root (left) against `right`, a folder
    /// of the same format, and writes the Certificate to this ledger. The
    /// certificate is anchored to this index's current manifest when one
    /// exists; otherwise the verifier records manifest version "0" and
    /// cites both sides' catalog digests (contract v3 entry 29).
    pub fn reconcile(&self, right: &str, request: &ReconcileRequest) -> Result<ReconcileResult> {
        guard(|| self.reconcile_inner(right, request))
    }

    fn reconcile_inner(&self, right: &str, req: &ReconcileRequest) -> Result<ReconcileResult> {
        if req.key.is_empty() {
            return Err(Error::KeySpec(
                "a reconcile compares rows by key; pass --key <columns>".into(),
            ));
        }
        let right = if is_sql_root(right) {
            right.to_string()
        } else {
            std::fs::canonicalize(right)
                .map_err(|e| {
                    Error::Source(format!(
                        "cannot open the right side {right:?} ({e}); pass an existing folder or \
                         a sql:// table"
                    ))
                })?
                .to_string_lossy()
                .into_owned()
        };
        let right_root = if is_sql_root(&right) {
            SqlUrl::parse(&right)?.root()
        } else {
            right.clone()
        };
        let pinned = self.pin()?;
        let (left_source, right_source, identities) =
            self.reconcile_sides(pinned.as_ref(), &right, &right_root)?;
        let mut spec = ReconcileSpec::new(req.key.clone())
            .ignore(req.ignore.clone())
            .with_totals(total_columns(&req.totals)?)
            .group_by(req.group_by.clone());
        if let Some(l) = &req.latest_by {
            spec = spec.latest_by(l.clone());
        }
        if let Some((fraction, seed)) = req.sample {
            spec = spec.coverage(CoverageRequest::Sampled { fraction, seed });
        }
        let ctx = CertificateContext {
            proposal: None,
            manifest: pinned.as_ref().map(|p| ManifestAnchor {
                sequence: p.sequence(),
                hash: p.hash,
                age: p.age,
            }),
            inputs: identities
                .into_iter()
                .map(|hash| AgedInput {
                    hash,
                    age: Duration::ZERO,
                })
                .collect(),
            clock: self.options().clock.clone(),
            agg: Vec::new(),
        };
        let mut options = Options::default();
        if let Some(budget) = req.memory_budget {
            options.memory_budget = budget;
        }
        // A side whose fresh index holds an agg key on the reconcile's key
        // and its one decimal total (B7) is reconciled through it: the
        // verifier reads the agg key's per-key rows and sums instead of the
        // side's rows when every other condition holds, and the
        // certificate's `agg` says so.
        let left_agg = agg_index(self, req)?;
        let right_engine = if is_sql_root(&right) {
            None
        } else {
            let options = EngineOptions {
                format: self.options().format,
                schema: self.options().schema.clone(),
                ..EngineOptions::default()
            };
            Engine::open(&right, options).ok()
        };
        let right_agg = match &right_engine {
            Some(e) => agg_index(e, req)?,
            None => None,
        };
        let mut left = Side::new(&*left_source, self.root());
        if let Some(p) = &left_agg {
            left = left.indexed(p);
        }
        let mut right_side = Side::new(&*right_source, &right_root);
        if let Some(p) = &right_agg {
            right_side = right_side.indexed(p);
        }
        let certified = self
            .reconciler(spec, options)?
            .certify(&left, &right_side, &ctx)?;
        let certificate = self.record_certified(&certified)?;
        let lints = rescan_lints(
            req,
            &certified.outcome,
            [
                (self.table_url().is_none()).then(|| complete_specs(self)),
                right_engine.as_ref().map(complete_specs),
            ],
        );
        Ok(ReconcileResult {
            certificate,
            outcome: certified.outcome,
            tables: self.finish_tables()?,
            lints,
        })
    }

    /// The two sides' sources, and each table side's identity (cited by the
    /// certificate beside the catalog digests). When one side is a table and
    /// the other a folder, the table is read at the folder's dataset types
    /// where the two differ only in decimal scale, time unit, or integer or
    /// float width (every value converted exactly or refused; ADR 016), so
    /// the comparison is at one type per column. A `--schema` given for the
    /// data root wins over that.
    #[allow(clippy::type_complexity)]
    fn reconcile_sides(
        &self,
        pinned: Option<&Pinned>,
        right: &str,
        right_root: &str,
    ) -> Result<(
        Box<dyn Source>,
        Box<dyn Source>,
        Vec<bellwether_types::Hash>,
    )> {
        let table = |url: &str, schema| -> Result<(Box<dyn Source>, Arc<SqlSource>)> {
            let s = self.open_table_source(url, schema)?;
            Ok((Box::new(TableSource(Arc::clone(&s))), s))
        };
        let given = self.options().schema.clone();
        let (left, right_src, handles) = match (self.table_url(), is_sql_root(right)) {
            (Some(url), false) => {
                let files = self.output_source(right)?;
                let (probe, _) = table(url, given.clone())?;
                let schema = adopt(
                    &*probe.schema(self.root())?,
                    &*files.schema(right_root)?,
                    given.as_deref(),
                );
                drop(probe);
                let (l, h) = table(url, schema.or(given))?;
                (l, files, vec![h])
            }
            (None, true) => {
                let files = self.source(pinned)?;
                let (probe, _) = table(right, None)?;
                let schema = adopt(
                    &*probe.schema(right_root)?,
                    &*files.schema(self.root())?,
                    None,
                );
                drop(probe);
                let (r, h) = table(right, schema)?;
                (files, r, vec![h])
            }
            (Some(url), true) => {
                let (l, hl) = table(url, given)?;
                let (r, hr) = table(right, None)?;
                (l, r, vec![hl, hr])
            }
            (None, false) => (self.source(pinned)?, self.output_source(right)?, Vec::new()),
        };
        let identities = handles
            .iter()
            .map(|h| h.identity())
            .collect::<Result<Vec<_>>>()?;
        Ok((left, right_src, identities))
    }
}

/// The index of `engine`'s folder as a reconcile side's planner, when its
/// agg key (B7) could stand in for the side's rows: the request totals one
/// decimal column as stored, grouped by nothing or by the key, with no
/// version rule; the folder's index is fresh (no file changed since its
/// build) and declares a complete agg key on exactly the key columns, in
/// order, storing that column's sum. `None` otherwise: the side is read as
/// before. The verifier checks the rest (`agg_side`).
fn agg_index(
    engine: &Engine,
    req: &ReconcileRequest,
) -> Result<Option<IndexPlanner<bellwether_store::ObjectStoreBackend>>> {
    if engine.table_url().is_some()
        || req.latest_by.is_some()
        || !(req.group_by.is_empty() || req.group_by == req.key)
    {
        return Ok(None);
    }
    let [given] = req.totals.as_slice() else {
        return Ok(None);
    };
    let Ok(total) = TotalColumn::parse(given) else {
        return Ok(None);
    };
    if total.quantize.is_some() {
        return Ok(None);
    }
    let Some(pinned) = engine.pin()? else {
        return Ok(None);
    };
    let serves = pinned.specs().0.iter().any(|s| {
        matches!(s, KeySpec::Agg { columns, value, functions }
            if *columns == req.key
                && *value == total.column
                && functions.contains(&AggregateFn::Sum))
    });
    if !serves
        || !folder_changes(engine.root(), &pinned, engine.format(), &engine.excluded())?.is_empty()
    {
        return Ok(None);
    }
    let Some(store) = engine.existing_store()? else {
        return Ok(None);
    };
    Ok(Some(IndexPlanner::open(store, "")?))
}

/// The key specs complete in `engine`'s index; empty when the folder is not
/// indexed or its index cannot be read (the lint then names every
/// declaration).
fn complete_specs(engine: &Engine) -> Vec<KeySpec> {
    engine
        .pin()
        .ok()
        .flatten()
        .map(|p| p.specs().0)
        .unwrap_or_default()
}

/// `R-agg-1` (B7) for a finished reconcile: when it compared per-key rows
/// and one exact decimal total (every compared column a key column, the
/// total grouped by nothing or the key, no version rule), each folder side
/// it read rather than answered from an agg key (`specs[s]` are the side's
/// complete specs, `None` for a table side).
fn rescan_lints(
    req: &ReconcileRequest,
    outcome: &Reconciliation,
    specs: [Option<Vec<KeySpec>>; 2],
) -> Vec<bellwether_types::Lint> {
    let [given] = req.totals.as_slice() else {
        return Vec::new();
    };
    let Ok(total) = TotalColumn::parse(given) else {
        return Vec::new();
    };
    if total.quantize.is_some()
        || req.latest_by.is_some()
        || !(req.group_by.is_empty() || req.group_by == req.key)
        || !outcome.hashed_columns.iter().all(|c| req.key.contains(c))
    {
        return Vec::new();
    }
    let rescans: Vec<bellwether_profile::lints::ReconcileRescan> = ["left", "right"]
        .into_iter()
        .zip(specs)
        .zip(&outcome.sides)
        .filter_map(|((side, complete), report)| {
            let complete = complete?;
            (!outcome.agg.iter().any(|a| a.side == side)).then(|| {
                bellwether_profile::lints::ReconcileRescan {
                    side: side.to_string(),
                    root: report.root.clone(),
                    key: req.key.clone(),
                    value: total.column.clone(),
                    rows: report.rows,
                    complete,
                }
            })
        })
        .collect();
    bellwether_profile::lints::reconcile_rescans(&rescans)
}

/// The dataset types a table side takes from the folder it is compared
/// with: each column both have whose types differ only in decimal scale,
/// time unit or zone, integer width, float width, or string or binary
/// representation, at the folder's type; columns `given` names keep the
/// given type. `None` when nothing changes.
fn adopt(
    table: &arrow::datatypes::Schema,
    files: &arrow::datatypes::Schema,
    given: Option<&arrow::datatypes::Schema>,
) -> Option<arrow::datatypes::SchemaRef> {
    use arrow::datatypes::DataType as D;
    let family = |d: &D| match d {
        D::Decimal128(..) | D::Decimal256(..) => 1,
        D::Timestamp(..) => 2,
        d if d.is_integer() => 3,
        D::Float32 | D::Float64 => 4,
        D::Utf8 | D::LargeUtf8 | D::Utf8View => 5,
        D::Binary | D::LargeBinary | D::BinaryView => 6,
        _ => 0,
    };
    let mut fields: Vec<arrow::datatypes::Field> = given
        .map(|g| g.fields().iter().map(|f| f.as_ref().clone()).collect())
        .unwrap_or_default();
    let before = fields.len();
    for t in table.fields() {
        if fields.iter().any(|f| f.name() == t.name()) {
            continue;
        }
        if let Ok(f) = files.field_with_name(t.name()) {
            let (a, b) = (t.data_type(), f.data_type());
            let decimal256 = matches!(b, D::Decimal256(..));
            if a != b && family(a) != 0 && family(a) == family(b) && !decimal256 {
                fields.push(arrow::datatypes::Field::new(t.name(), b.clone(), true));
            }
        }
    }
    (fields.len() > before || given.is_some())
        .then(|| std::sync::Arc::new(arrow::datatypes::Schema::new(fields)))
}

/// Whether the verifier totals a column of type `dt` as given: a 128- or
/// 256-bit decimal, possibly dictionary-encoded.
fn totalable(dt: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Decimal128(..) | DataType::Decimal256(..) => true,
        DataType::Dictionary(_, v) => totalable(v),
        _ => false,
    }
}

/// Whether `dt` is a float, possibly dictionary-encoded: totalled only when
/// quantized (`col:dec(S)`).
fn floating(dt: &arrow::datatypes::DataType) -> bool {
    match dt {
        arrow::datatypes::DataType::Dictionary(_, v) => floating(v),
        other => other.is_floating(),
    }
}

/// Parses every `--totals` entry (`col` or `col:dec(S)`).
pub(crate) fn total_columns(given: &[String]) -> Result<Vec<TotalColumn>> {
    given
        .iter()
        .map(|t| TotalColumn::parse(t).map_err(Error::KeySpec))
        .collect()
}
