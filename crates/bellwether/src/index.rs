//! `index` (the build crate's builder) and `locate`, `latest`, `asof` (its
//! plan builder), each lookup writing an Evidence of kind `plan`.

use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use bellwether_build::{BuildOptions, Builder, Lookup};
use bellwether_store::parse_rfc3339;
use bellwether_types::{
    format, Error, Evidence, EvidenceBody, KeySpec, Predicate, Provenance, Record, Result, ScanPlan,
};
use bytes::Bytes;

use crate::engine::{folder_changes, guard, schema_hint, Engine, FolderChanges, Pinned};
use crate::keys::KeyFile;
use crate::results::{Cost, IndexResult, LookupKind, PlanResult, SpecState};

impl Engine {
    /// Builds or updates the index so it declares `specs` (in addition to
    /// those it already declares), over every file under the data root, in
    /// one manifest commit (build crate, ADR 012, 013). Returns the committed
    /// sequence, the segment counts, and every declared spec's completeness.
    pub fn index(&self, specs: &[KeySpec]) -> Result<IndexResult> {
        guard(|| self.index_inner(specs)).map_err(schema_hint)
    }

    fn index_inner(&self, specs: &[KeySpec]) -> Result<IndexResult> {
        if specs.is_empty() && self.manifest()?.is_none() {
            return Err(Error::KeySpec(
                "a new index needs at least one key spec; pass --key exact:<column> (or \
                 temporal:<column>@<ts>)"
                    .into(),
            ));
        }
        for s in specs {
            crate::spec::check(s)?;
        }
        let mut options = BuildOptions::new(self.format().kind());
        options.chunk_size = self.options().chunk_size;
        options.schema.clone_from(&self.options().schema);
        options.fingerprint_policy = self.options().fingerprint_policy;
        options.partitioning = self.options().partitioning;
        let report =
            Builder::new(Arc::clone(self.store()?), "").build(self.root(), specs, &options)?;
        let pinned = self.pin_required("index")?;
        let m = pinned.manifest();
        Ok(IndexResult {
            root: self.root().to_string(),
            committed: report.sequence,
            sequence: pinned.sequence(),
            manifest: pinned.hash,
            segments: m.segments.len() as u64,
            segments_written: report.segments_written,
            specs: pinned
                .planner
                .declared_specs()
                .iter()
                .map(|s| SpecState {
                    spec: s.clone(),
                    complete_at: pinned.planner.completeness(s),
                })
                .collect(),
            files_new: report.files_new,
            files_changed: report.files_changed,
            files_removed: report.files_removed,
            files_unchanged: report.files_unchanged,
            row_groups_read: report.row_groups_read,
            row_groups_assigned: report.row_groups_assigned,
            row_groups_retired: report.row_groups_retired,
        })
    }

    /// Plans a lookup of the keys in `keys` with every declared key spec
    /// complete at the current manifest sequence, and writes the plan as an
    /// Evidence of kind `plan`. The plan includes every row group holding a
    /// queried key (invariant 1) and carries an `In` residual (ADR 011).
    pub fn locate(&self, keys: &KeyFile) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Locate)).map_err(schema_hint)
    }

    /// Plans the newest version of each key, through the one temporal spec
    /// that serves the key columns. The residual carries `NotOlderThan` and
    /// `LatestPerKey` (invariant 5).
    pub fn latest(&self, keys: &KeyFile) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Latest)).map_err(schema_hint)
    }

    /// Plans the version of each key current at `at`: an RFC 3339 instant
    /// (or a date, `YYYY-MM-DD`, for a date column), or an integer already in
    /// the timestamp column's unit. The conversion to that unit must be
    /// exact. The residual carries `NotOlderThan` and `LatestPerKey`
    /// (invariant 5).
    pub fn asof(&self, keys: &KeyFile, at: &str) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Asof(at))).map_err(schema_hint)
    }

    fn plan_lookup(&self, keys: &KeyFile, mode: Mode<'_>) -> Result<PlanResult> {
        let verb = match mode {
            Mode::Locate => "locate",
            Mode::Latest => "latest",
            Mode::Asof(_) => "asof",
        };
        let pinned = self.pin_required(verb)?;
        // Invariant 1 over the folder, not only the index: a file that
        // landed or changed after the last build would be silently left out
        // of the plan, so it is refused.
        let changes = folder_changes(self.root(), &pinned, self.format())?;
        if !changes.is_empty() {
            return Err(stale_index(self.root(), verb, &changes));
        }
        let (specs, excluded) = pinned.specs();
        let (run_keys, null_tuples) = keys.run_keys(pinned.planner.schema())?;
        let columns = run_keys.columns().to_vec();
        let tuples: Vec<Vec<u8>> = run_keys.tuples().map(<[u8]>::to_vec).collect();
        let payload = run_keys.to_payload()?;
        let key_set = bellwether_types::Hash::of(&payload);
        self.cache()?.put_cached(&key_set, Bytes::from(payload))?;
        let (lookup, kind) = match mode {
            Mode::Locate => (
                Lookup::Keys {
                    columns: columns.clone(),
                    keys: tuples,
                },
                LookupKind::Locate,
            ),
            Mode::Latest => {
                serving_temporal(self.root(), verb, &specs, &columns)?;
                (
                    Lookup::Latest {
                        columns: columns.clone(),
                        keys: tuples,
                    },
                    LookupKind::Latest,
                )
            }
            Mode::Asof(at) => {
                let ts_column = serving_temporal(self.root(), verb, &specs, &columns)?;
                let ts_type = pinned
                    .planner
                    .column_types(std::slice::from_ref(&ts_column))?
                    .remove(0)
                    .1;
                let ts = instant_in_unit(at, &ts_column, &ts_type)?;
                (
                    Lookup::AsOf {
                        columns: columns.clone(),
                        keys: tuples,
                        ts,
                    },
                    LookupKind::Asof {
                        at: at.to_string(),
                        ts,
                        ts_column,
                    },
                )
            }
        };
        let plan = pinned.planner.plan(&specs, &lookup).map_err(|e| {
            if specs.is_empty() && !excluded.is_empty() {
                not_complete_yet(self.root(), verb, &excluded)
            } else {
                e
            }
        })?;
        if !matches!(kind, LookupKind::Locate) {
            check_temporal_residual(&plan)?;
        }
        let files_total = pinned.manifest().rowgroup_space.files.len() as u64;
        let planned_total = plan.row_groups_total;
        let planned = Cost {
            files: plan.entries.len() as u64,
            row_groups: plan.row_groups_selected,
            bytes: plan.bytes,
        };
        let evidence = self.put(Record::Evidence(plan_evidence(
            &pinned,
            key_set,
            plan,
            self.now(),
        )))?;
        Ok(PlanResult {
            lookup: kind,
            columns,
            keys: run_keys.len() as u64,
            null_tuples,
            key_set,
            sequence: pinned.sequence(),
            specs,
            excluded_incomplete: excluded,
            files_total,
            row_groups_total: planned_total,
            planned,
            evidence,
        })
    }
}

#[derive(Clone, Copy)]
enum Mode<'a> {
    Locate,
    Latest,
    Asof(&'a str),
}

/// The `plan` Evidence: provenance cites the manifest's content hash and the
/// key set's hash, aged by the manifest's commit (invariant 6).
fn plan_evidence(
    pinned: &Pinned,
    key_set: bellwether_types::Hash,
    plan: ScanPlan,
    created_at: String,
) -> Evidence {
    Evidence {
        record_version: format::RECORD_VERSION,
        provenance: Provenance {
            inputs: vec![pinned.hash, key_set],
            oldest_input_age: pinned.age,
            manifest_version: Some(pinned.sequence().to_string()),
            created_at,
        },
        body: EvidenceBody::Plan { plan },
    }
}

/// The refusal for a folder that no longer matches its index: it names the
/// files and `bellwether index`.
pub(crate) fn stale_index(root: &str, verb: &str, c: &FolderChanges) -> Error {
    let name = |p: &str| {
        std::path::Path::new(p)
            .file_name()
            .map_or_else(|| p.to_string(), |n| n.to_string_lossy().into_owned())
    };
    // One source for the lists the message names and the typed fields carry.
    let (untracked, changed, missing) = c.paths();
    let mut parts = Vec::new();
    let list = |paths: &[String]| {
        let shown: Vec<String> = paths.iter().take(5).map(|p| name(p)).collect();
        let more = paths.len().saturating_sub(5);
        if more > 0 {
            format!("{} and {more} more", shown.join(", "))
        } else {
            shown.join(", ")
        }
    };
    if !untracked.is_empty() {
        parts.push(format!(
            "{} new file(s) the index does not cover ({})",
            untracked.len(),
            list(&untracked)
        ));
    }
    if !changed.is_empty() {
        parts.push(format!(
            "{} changed file(s) ({})",
            changed.len(),
            list(&changed)
        ));
    }
    if !missing.is_empty() {
        parts.push(format!(
            "{} removed file(s) ({})",
            missing.len(),
            list(&missing)
        ));
    }
    let message = format!(
        "{root} has {} since its index was built, so {verb} could miss rows (invariant 1); run \
         `bellwether index {root}` first",
        parts.join(" and ")
    );
    Error::StaleIndex {
        untracked,
        changed,
        missing,
        message,
    }
}

/// The lookup's refusal when key specs are declared but none is complete
/// yet: there is nothing to plan with until the index finishes them.
fn not_complete_yet(root: &str, operation: &'static str, excluded: &[KeySpec]) -> Error {
    Error::NotIndexed {
        root: root.to_string(),
        operation,
        needs: Some(
            excluded
                .iter()
                .map(KeySpec::name)
                .collect::<Vec<_>>()
                .join(", "),
        ),
    }
}

/// The timestamp column of the one complete temporal spec whose columns are
/// all key columns. None is [`Error::NotIndexed`]; several is an
/// ambiguous `key_columns`.
fn serving_temporal(
    root: &str,
    operation: &'static str,
    specs: &[KeySpec],
    columns: &[String],
) -> Result<String> {
    let mut found = specs.iter().filter_map(|s| match s {
        KeySpec::Temporal {
            columns: c,
            ts_column,
        } if c.iter().all(|x| columns.contains(x)) => Some(ts_column.clone()),
        _ => None,
    });
    match (found.next(), found.next()) {
        (Some(ts), None) => Ok(ts),
        (None, _) => Err(Error::NotIndexed {
            root: root.to_string(),
            operation,
            needs: Some(format!("temporal:{}@<timestamp column>", columns.join(","))),
        }),
        (Some(_), Some(_)) => Err(Error::InvalidArgument {
            field: "key_columns",
            reason: format!(
                "more than one temporal key spec serves a lookup on {columns:?}; name the key \
                 columns of exactly one"
            ),
        }),
    }
}

/// Invariant 5 at the boundary: a temporal plan's residual must apply
/// `NotOlderThan` before `LatestPerKey`, or a superseded row could be
/// returned. A plan without both is refused, never passed on.
fn check_temporal_residual(plan: &ScanPlan) -> Result<()> {
    let ok = match &plan.residual {
        Some(Predicate::And { all }) => {
            let not_older = all
                .iter()
                .position(|p| matches!(p, Predicate::NotOlderThan { .. }));
            let latest = all
                .iter()
                .position(|p| matches!(p, Predicate::LatestPerKey { .. }));
            matches!((not_older, latest), (Some(a), Some(b)) if a < b)
        }
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(Error::Other(format!(
            "the planner returned a temporal plan without NotOlderThan then LatestPerKey in its \
             residual (invariant 5); this is a bug, please report it at {}",
            crate::engine::ISSUE_TRACKER
        )))
    }
}

/// `at` in `ts_type`'s unit, exactly: an integer is taken as already in the
/// unit; an RFC 3339 instant (or a `YYYY-MM-DD` date) is converted, and a
/// remainder below the unit is an error, never rounded.
fn instant_in_unit(at: &str, ts_column: &str, ts_type: &DataType) -> Result<i64> {
    let at = at.trim();
    if let Ok(n) = at.parse::<i64>() {
        return Ok(n);
    }
    let integer_column = matches!(
        ts_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    );
    if integer_column {
        return Err(Error::InvalidArgument {
            field: "at",
            reason: format!(
                "{ts_column} is an integer column ({ts_type}), so the instant must be an integer \
                 in its unit, not {at:?}"
            ),
        });
    }
    let instant = if at.len() == 10 && at.as_bytes().get(4) == Some(&b'-') {
        format!("{at}T00:00:00Z")
    } else {
        at.to_string()
    };
    let parsed = parse_rfc3339(&instant).map_err(|_| Error::InvalidArgument {
        field: "at",
        reason: format!(
            "the instant {at:?} is neither an RFC 3339 instant (2026-03-03T00:00:00Z), a date \
             (2026-03-03), nor an integer in {ts_column}'s unit"
        ),
    })?;
    let nanos = i128::from(parsed.timestamp()) * 1_000_000_000
        + i128::from(parsed.timestamp_subsec_nanos());
    let per_unit: i128 = match ts_type {
        DataType::Timestamp(TimeUnit::Second, _) => 1_000_000_000,
        DataType::Timestamp(TimeUnit::Millisecond, _) | DataType::Date64 => 1_000_000,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 1,
        DataType::Date32 => 86_400 * 1_000_000_000,
        other => {
            return Err(Error::KeySpec(format!(
                "{ts_column} has type {other}, which asof cannot order by; use a timestamp, \
                 date, or integer column"
            )))
        }
    };
    if nanos % per_unit != 0 {
        return Err(Error::InvalidArgument {
            field: "at",
            reason: format!(
                "the instant {at:?} is finer than {ts_column}'s unit ({ts_type}); give an \
                 instant that is a whole number of that unit"
            ),
        });
    }
    i64::try_from(nanos / per_unit).map_err(|_| Error::InvalidArgument {
        field: "at",
        reason: format!("the instant {at:?} is out of range for {ts_column} ({ts_type})"),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_temporal_residual_without_not_older_than_before_latest_is_refused() {
        let c = vec!["id".to_string()];
        let not_older = Predicate::NotOlderThan {
            columns: c.clone(),
            ts_column: "ts".into(),
            bounds: vec![],
        };
        let latest = Predicate::LatestPerKey {
            columns: c.clone(),
            ts_column: "ts".into(),
        };
        let plan = |residual: Option<Predicate>| ScanPlan {
            manifest_version: "1".into(),
            entries: vec![],
            residual,
            bytes: 0,
            row_groups_total: 0,
            row_groups_selected: 0,
        };
        let and = |all: Vec<Predicate>| Some(Predicate::And { all });
        assert!(
            check_temporal_residual(&plan(and(vec![not_older.clone(), latest.clone()]))).is_ok()
        );
        for bad in [
            and(vec![latest.clone(), not_older.clone()]),
            and(vec![latest.clone()]),
            and(vec![not_older]),
            Some(latest),
            None,
        ] {
            assert!(check_temporal_residual(&plan(bad)).is_err());
        }
    }

    #[test]
    fn instants_convert_exactly_or_are_refused() {
        let us = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            instant_in_unit("1970-01-01T00:00:01Z", "ts", &us).unwrap(),
            1_000_000
        );
        assert_eq!(instant_in_unit("42", "ts", &us).unwrap(), 42);
        let s = DataType::Timestamp(TimeUnit::Second, None);
        assert!(instant_in_unit("1970-01-01T00:00:01.5Z", "ts", &s).is_err());
        assert_eq!(
            instant_in_unit("1970-01-03", "d", &DataType::Date32).unwrap(),
            2
        );
        assert!(instant_in_unit("1970-01-03T01:00:00Z", "d", &DataType::Date32).is_err());
        assert!(instant_in_unit("yesterday", "ts", &us).is_err());
        assert!(instant_in_unit("2026-01-01T00:00:00Z", "v", &DataType::Int64).is_err());
        assert!(instant_in_unit("2026-01-01T00:00:00Z", "v", &DataType::Utf8).is_err());
    }
}
