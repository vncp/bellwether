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
        options.path_template.clone_from(&self.options().path_template);
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
        guard(|| self.plan_lookup(keys, Mode::Locate { window: None })).map_err(schema_hint)
    }

    /// Plans a lookup of the keys in `keys` intersected with `--window A..B`.
    pub fn locate_window(&self, keys: &KeyFile, window: &str) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Locate {
                    window: Some(window),
                },
            )
        })
        .map_err(schema_hint)
    }

    /// Plans the newest version of each key, through the one temporal spec
    /// that serves the key columns. The residual carries `NotOlderThan` and
    /// `LatestPerKey` (invariant 5).
    pub fn latest(&self, keys: &KeyFile) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Latest {
                    known: None,
                    lookback: None,
                },
            )
        })
        .map_err(schema_hint)
    }

    /// Plans `latest` with optional `--known K` (bitemporal) or `--lookback`
    /// (window).
    pub fn latest_with(
        &self,
        keys: &KeyFile,
        known: Option<&str>,
        lookback: Option<&str>,
    ) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Latest { known, lookback })).map_err(schema_hint)
    }

    /// Plans the version of each key current at `at`: an RFC 3339 instant
    /// (or a date, `YYYY-MM-DD`, for a date column), or an integer already in
    /// the timestamp column's unit. The conversion to that unit must be
    /// exact. The residual carries `NotOlderThan` and `LatestPerKey`
    /// (invariant 5).
    pub fn asof(&self, keys: &KeyFile, at: &str) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Asof {
                    at: Some(at),
                    valid: None,
                    known: None,
                    lookback: None,
                },
            )
        })
        .map_err(schema_hint)
    }

    /// Plans `asof` with `--at T` (plus optional `--lookback`) or bitemporal
    /// `--valid V --known K`.
    pub fn asof_with(
        &self,
        keys: &KeyFile,
        at: Option<&str>,
        valid: Option<&str>,
        known: Option<&str>,
        lookback: Option<&str>,
    ) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Asof {
                    at,
                    valid,
                    known,
                    lookback,
                },
            )
        })
        .map_err(schema_hint)
    }

    fn plan_lookup(&self, keys: &KeyFile, mode: Mode<'_>) -> Result<PlanResult> {
        let verb = match mode {
            Mode::Locate { .. } => "locate",
            Mode::Latest { .. } => "latest",
            Mode::Asof { .. } => "asof",
        };
        let pinned = self.pin_required(verb)?;
        // Invariant 1 over the folder, not only the index: a file that
        // landed or changed after the last build would be silently left out
        // of the plan, so it is refused.
        if self.options().at_manifest.is_none() {
            let changes = folder_changes(self.root(), &pinned, self.format())?;
            if !changes.is_empty() {
                return Err(stale_index(self.root(), verb, &changes));
            }
        }
        let (specs, excluded) = pinned.specs();
        let (run_keys, null_tuples) = keys.run_keys(pinned.planner.schema())?;
        let columns = run_keys.columns().to_vec();
        let tuples: Vec<Vec<u8>> = run_keys.tuples().map(<[u8]>::to_vec).collect();
        let payload = run_keys.to_payload()?;
        let key_set = bellwether_types::Hash::of(&payload);
        self.cache()?.put_cached(&key_set, Bytes::from(payload))?;
        let (lookup, kind, check_temporal) = match mode {
            Mode::Locate { window: None } => (
                Lookup::Keys {
                    columns: columns.clone(),
                    keys: tuples,
                },
                LookupKind::Locate,
                false,
            ),
            Mode::Locate { window: Some(win) } => {
                let (ts_column, ts_type) = resolve_window_ts_column(
                    &specs,
                    pinned.planner.schema(),
                    &columns,
                    pinned.manifest(),
                )?;
                let (lo_str, hi_str) = win.split_once("..").ok_or_else(|| {
                    Error::KeySpec(format!(
                        "--window {win:?} must be written as A..B (inclusive bounds, e.g. \
                         2026-03-01..2026-03-05)"
                    ))
                })?;
                let hi_str = hi_str.strip_prefix('=').unwrap_or(hi_str);
                let lo = instant_in_unit(lo_str, &ts_column, &ts_type)?;
                let hi = instant_in_unit(hi_str, &ts_column, &ts_type)?;
                (
                    Lookup::KeysInWindow {
                        columns: columns.clone(),
                        keys: tuples,
                        ts_column: ts_column.clone(),
                        lo,
                        hi,
                    },
                    LookupKind::Window {
                        window: win.to_string(),
                        lo,
                        hi,
                        ts_column,
                    },
                    false,
                )
            }
            Mode::Latest {
                known: Some(k_str),
                lookback: None,
            } => {
                let (valid_column, known_column) = serving_temporal2(&specs, &columns)?;
                let known_type = pinned
                    .planner
                    .column_types(std::slice::from_ref(&known_column))?
                    .remove(0)
                    .1;
                let known_ts = instant_in_unit(k_str, &known_column, &known_type)?;
                (
                    Lookup::Bitemporal {
                        columns: columns.clone(),
                        keys: tuples,
                        valid: None,
                        known: known_ts,
                    },
                    LookupKind::Bitemporal {
                        valid: None,
                        valid_ts: None,
                        known: k_str.to_string(),
                        known_ts,
                        valid_column,
                        known_column,
                    },
                    true,
                )
            }
            Mode::Latest {
                known: None,
                lookback: Some(lb),
            } => {
                let (ts_column, ts_type) = resolve_window_ts_column(
                    &specs,
                    pinned.planner.schema(),
                    &columns,
                    pinned.manifest(),
                )?;
                let delta = parse_lookback(lb, &ts_column, &ts_type)?;
                let hi = self.dataset_max_ts(&pinned, &specs, &columns, &ts_column, &ts_type)?;
                let lo = hi.saturating_sub(delta);
                (
                    Lookup::KeysInWindow {
                        columns: columns.clone(),
                        keys: tuples,
                        ts_column: ts_column.clone(),
                        lo,
                        hi,
                    },
                    LookupKind::Window {
                        window: format!("latest-{lb}"),
                        lo,
                        hi,
                        ts_column,
                    },
                    false,
                )
            }
            Mode::Latest {
                known: Some(_),
                lookback: Some(_),
            } => {
                return Err(Error::KeySpec(
                    "latest takes either --known or --lookback, not both".into(),
                ));
            }
            Mode::Latest {
                known: None,
                lookback: None,
            } => {
                serving_temporal(&specs, &columns)?;
                (
                    Lookup::Latest {
                        columns: columns.clone(),
                        keys: tuples,
                    },
                    LookupKind::Latest,
                    true,
                )
            }
            Mode::Asof {
                at: None,
                valid: Some(v_str),
                known: Some(k_str),
                lookback: None,
            } => {
                let (valid_column, known_column) = serving_temporal2(&specs, &columns)?;
                let valid_type = pinned
                    .planner
                    .column_types(std::slice::from_ref(&valid_column))?
                    .remove(0)
                    .1;
                let known_type = pinned
                    .planner
                    .column_types(std::slice::from_ref(&known_column))?
                    .remove(0)
                    .1;
                let valid_ts = instant_in_unit(v_str, &valid_column, &valid_type)?;
                let known_ts = instant_in_unit(k_str, &known_column, &known_type)?;
                (
                    Lookup::Bitemporal {
                        columns: columns.clone(),
                        keys: tuples,
                        valid: Some(valid_ts),
                        known: known_ts,
                    },
                    LookupKind::Bitemporal {
                        valid: Some(v_str.to_string()),
                        valid_ts: Some(valid_ts),
                        known: k_str.to_string(),
                        known_ts,
                        valid_column,
                        known_column,
                    },
                    true,
                )
            }
            Mode::Asof {
                at: Some(at),
                valid: None,
                known: None,
                lookback: Some(lb),
            } => {
                let (ts_column, ts_type) = resolve_window_ts_column(
                    &specs,
                    pinned.planner.schema(),
                    &columns,
                    pinned.manifest(),
                )?;
                let hi = instant_in_unit(at, &ts_column, &ts_type)?;
                let delta = parse_lookback(lb, &ts_column, &ts_type)?;
                let lo = hi.saturating_sub(delta);
                (
                    Lookup::KeysInWindow {
                        columns: columns.clone(),
                        keys: tuples,
                        ts_column: ts_column.clone(),
                        lo,
                        hi,
                    },
                    LookupKind::Window {
                        window: format!("{at}-{lb}..{at}"),
                        lo,
                        hi,
                        ts_column,
                    },
                    false,
                )
            }
            Mode::Asof {
                at: Some(at),
                valid: None,
                known: None,
                lookback: None,
            } => {
                let ts_column = serving_temporal(&specs, &columns)?;
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
                    true,
                )
            }
            Mode::Asof { .. } => {
                return Err(Error::KeySpec(
                    "asof takes either --at <instant> (with optional --lookback <duration>) or \
                     bitemporal --valid <V> --known <K>"
                        .into(),
                ));
            }
        };
        let ordered_by = pinned.planner.ordered_by(&specs)?;
        let plan = pinned.planner.plan(&specs, &lookup).map_err(|e| {
            if specs.is_empty() && !excluded.is_empty() {
                Error::KeySpec(format!(
                    "{e}; the declared specs are not complete yet, run `bellwether index` to \
                     finish them"
                ))
            } else {
                e
            }
        })?;
        if check_temporal {
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
            ordered_by,
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
    Locate {
        window: Option<&'a str>,
    },
    Latest {
        known: Option<&'a str>,
        lookback: Option<&'a str>,
    },
    Asof {
        at: Option<&'a str>,
        valid: Option<&'a str>,
        known: Option<&'a str>,
        lookback: Option<&'a str>,
    },
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
    let mut parts = Vec::new();
    let list = |paths: Vec<String>| {
        let shown: Vec<String> = paths.iter().take(5).map(|p| name(p)).collect();
        let more = paths.len().saturating_sub(5);
        if more > 0 {
            format!("{} and {more} more", shown.join(", "))
        } else {
            shown.join(", ")
        }
    };
    if !c.untracked.is_empty() {
        parts.push(format!(
            "{} new file(s) the index does not cover ({})",
            c.untracked.len(),
            list(c.untracked.iter().map(|f| f.path.clone()).collect())
        ));
    }
    if !c.changed.is_empty() {
        parts.push(format!(
            "{} changed file(s) ({})",
            c.changed.len(),
            list(c.changed.clone())
        ));
    }
    if !c.missing.is_empty() {
        parts.push(format!(
            "{} removed file(s) ({})",
            c.missing.len(),
            list(c.missing.clone())
        ));
    }
    Error::KeySpec(format!(
        "{root} has {} since its index was built, so {verb} could miss rows (invariant 1); run \
         `bellwether index {root}` first",
        parts.join(" and ")
    ))
}

/// The timestamp column of the one complete temporal spec whose columns are
/// all key columns; none or several is an error naming what to do.
fn serving_temporal(specs: &[KeySpec], columns: &[String]) -> Result<String> {
    let mut found = specs.iter().filter_map(|s| match s {
        KeySpec::Temporal {
            columns: c,
            ts_column,
        } if c.iter().all(|x| columns.contains(x)) => Some(ts_column.clone()),
        _ => None,
    });
    match (found.next(), found.next()) {
        (Some(ts), None) => Ok(ts),
        (None, _) => Err(Error::KeySpec(format!(
            "no complete temporal key spec serves a lookup on {columns:?}; run `bellwether index \
             --key temporal:{}@<timestamp column>` first",
            columns.join(",")
        ))),
        (Some(_), Some(_)) => Err(Error::KeySpec(format!(
            "more than one temporal key spec serves a lookup on {columns:?}; name the key \
             columns of exactly one"
        ))),
    }
}

/// The `(valid_column, known_column)` of the one complete `temporal2` spec
/// whose columns are all key columns.
fn serving_temporal2(specs: &[KeySpec], columns: &[String]) -> Result<(String, String)> {
    let mut found = specs.iter().filter_map(|s| match s {
        KeySpec::Temporal2 {
            columns: c,
            valid_column,
            known_column,
        } if c.iter().all(|x| columns.contains(x)) => {
            Some((valid_column.clone(), known_column.clone()))
        }
        _ => None,
    });
    match (found.next(), found.next()) {
        (Some(pair), None) => Ok(pair),
        (None, _) => Err(Error::KeySpec(format!(
            "no complete temporal2 key spec serves a bitemporal lookup on {columns:?}; run \
             `bellwether index --key temporal2:{}@<valid>,<known>` first",
            columns.join(",")
        ))),
        (Some(_), Some(_)) => Err(Error::KeySpec(format!(
            "more than one temporal2 key spec serves a lookup on {columns:?}; name the key \
             columns of exactly one"
        ))),
    }
}

/// Resolves the timestamp or date column for a window or lookback query:
/// prefers a `Bounds` or `Temporal` spec serving `columns`, then a `Range`
/// spec on a timestamp/date column, then a unique timestamp/date column in the
/// dataset schema.
pub(crate) fn resolve_window_ts_column(
    specs: &[KeySpec],
    schema: &arrow::datatypes::Schema,
    columns: &[String],
    _manifest: &bellwether_store::Manifest,
) -> Result<(String, DataType)> {
    let mut candidates: Vec<String> = Vec::new();
    for s in specs {
        match s {
            KeySpec::Bounds {
                columns: c,
                ts_column,
            }
            | KeySpec::Temporal {
                columns: c,
                ts_column,
            } if c.iter().all(|x| columns.contains(x)) => {
                if !candidates.contains(ts_column) {
                    candidates.push(ts_column.clone());
                }
            }
            _ => {}
        }
    }
    if candidates.is_empty() {
        for s in specs {
            if let KeySpec::Range { column } = s {
                if let Ok(f) = schema.field_with_name(column) {
                    if is_time_or_date(f.data_type()) && !candidates.contains(column) {
                        candidates.push(column.clone());
                    }
                }
            }
        }
    }
    if candidates.is_empty() {
        for f in schema.fields() {
            if is_time_or_date(f.data_type()) && !columns.contains(f.name()) {
                candidates.push(f.name().clone());
            }
        }
    }
    let Some(col) = candidates.into_iter().next() else {
        return Err(Error::KeySpec(
            "cannot determine the timestamp column for the window; index a bounds \
             (`exact:key@ts`), temporal (`temporal:key@ts`), or range (`range:ts`) spec"
                .into(),
        ));
    };
    let dt = schema
        .field_with_name(&col)
        .map_err(|_| Error::KeySpec(format!("column {col:?} is not in the dataset schema")))?
        .data_type()
        .clone();
    Ok((col, dt))
}

fn is_time_or_date(dt: &DataType) -> bool {
    match dt {
        DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _) => true,
        DataType::Dictionary(_, v) => is_time_or_date(v),
        _ => false,
    }
}

/// Parses a calendar lookback duration (`<N><s|m|h|d|w>`) and converts it into
/// `ts_type`'s unit. Trading-day calendars are out of scope and stated in the
/// refusal if requested.
pub fn parse_lookback(spec: &str, ts_column: &str, ts_type: &DataType) -> Result<i64> {
    let s = spec.trim();
    if s.is_empty() {
        return Err(Error::KeySpec(
            "--lookback cannot be empty; pass a calendar duration with unit s, m, h, d, or w \
             (e.g. 252d; trading-day calendars are out of scope)"
                .into(),
        ));
    }
    let unit_start = s
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(s.len());
    let (num_str, unit_str) = s.split_at(unit_start);
    let n: i64 = num_str.parse().map_err(|_| {
        Error::KeySpec(format!(
            "--lookback {spec:?} must start with a positive integer followed by s, m, h, d, or w \
             (calendar units; trading-day calendars are out of scope)"
        ))
    })?;
    if n <= 0 {
        return Err(Error::KeySpec(format!(
            "--lookback {spec:?} must be positive (got {n})"
        )));
    }
    let secs_per_unit: i64 = match unit_str.trim() {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 7 * 86_400,
        other => {
            return Err(Error::KeySpec(format!(
                "unknown --lookback unit {other:?} in {spec:?}; supported units are s, m, h, d, w \
                 (calendar durations only; trading-day calendars are out of scope)"
            )))
        }
    };
    let total_secs = n.checked_mul(secs_per_unit).ok_or_else(|| {
        Error::KeySpec(format!("--lookback {spec:?} overflows 64-bit duration"))
    })?;
    match ts_type {
        DataType::Date32 => {
            if total_secs % 86_400 != 0 {
                return Err(Error::KeySpec(format!(
                    "--lookback {spec:?} is finer than {ts_column}'s Date32 (day) unit; use d or w"
                )));
            }
            Ok(total_secs / 86_400)
        }
        DataType::Date64 | DataType::Timestamp(TimeUnit::Millisecond, _) => total_secs
            .checked_mul(1_000)
            .ok_or_else(|| Error::KeySpec(format!("--lookback {spec:?} overflows milliseconds"))),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(total_secs),
        DataType::Timestamp(TimeUnit::Microsecond, _) => total_secs
            .checked_mul(1_000_000)
            .ok_or_else(|| Error::KeySpec(format!("--lookback {spec:?} overflows microseconds"))),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => total_secs
            .checked_mul(1_000_000_000)
            .ok_or_else(|| Error::KeySpec(format!("--lookback {spec:?} overflows nanoseconds"))),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(total_secs),
        other => Err(Error::KeySpec(format!(
            "{ts_column} has type {other}, which lookback windows cannot order by"
        ))),
    }
}

impl Engine {
    fn dataset_max_ts(
        &self,
        pinned: &Pinned,
        _specs: &[KeySpec],
        _columns: &[String],
        ts_column: &str,
        ts_type: &DataType,
    ) -> Result<i64> {
        let source = self.source(Some(pinned))?;
        let mut max_enc: Option<Vec<u8>> = None;
        for f in &pinned.manifest().rowgroup_space.files {
            for rg in source.row_groups(&f.fingerprint)? {
                if let Some(st) = rg.stats.iter().find(|s| s.column == ts_column) {
                    if let Some(m) = &st.max {
                        if max_enc.as_ref().is_none_or(|cur| m > cur) {
                            max_enc = Some(m.clone());
                        }
                    }
                }
            }
        }
        let Some(enc) = max_enc else {
            return Err(Error::KeySpec(format!(
                "cannot determine latest timestamp for {ts_column:?} from dataset statistics"
            )));
        };
        decode_ts_from_canonical(&enc, ts_type).ok_or_else(|| {
            Error::KeySpec(format!(
                "cannot decode maximum timestamp of {ts_column:?} ({ts_type})"
            ))
        })
    }
}

pub(crate) fn decode_ts_from_canonical(bytes: &[u8], ts_type: &DataType) -> Option<i64> {
    match ts_type {
        DataType::Dictionary(_, v) => decode_ts_from_canonical(bytes, v),
        DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_)
        | DataType::Timestamp(_, _) => {
            if bytes.len() != 11 || bytes[0] != 0x08 {
                return None;
            }
            let mut b: [u8; 8] = bytes[3..11].try_into().ok()?;
            b[0] ^= 0x80;
            Some(i64::from_be_bytes(b))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            if bytes.len() != 9 || bytes[0] != 0x02 {
                return None;
            }
            let mut b: [u8; 8] = bytes[1..9].try_into().ok()?;
            b[0] ^= 0x80;
            Some(i64::from_be_bytes(b))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            if bytes.len() != 9 || bytes[0] != 0x03 {
                return None;
            }
            let b: [u8; 8] = bytes[1..9].try_into().ok()?;
            i64::try_from(u64::from_be_bytes(b)).ok()
        }
        _ => None,
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
pub(crate) fn instant_in_unit(at: &str, ts_column: &str, ts_type: &DataType) -> Result<i64> {
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
        return Err(Error::KeySpec(format!(
            "{ts_column} is an integer column ({ts_type}), so --at must be an integer in its \
             unit, not {at:?}"
        )));
    }
    let instant = if at.len() == 10 && at.as_bytes().get(4) == Some(&b'-') {
        format!("{at}T00:00:00Z")
    } else {
        at.to_string()
    };
    let parsed = parse_rfc3339(&instant).map_err(|_| {
        Error::KeySpec(format!(
            "--at {at:?} is neither an RFC 3339 instant (2026-03-03T00:00:00Z), a date \
             (2026-03-03), nor an integer in {ts_column}'s unit"
        ))
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
        return Err(Error::KeySpec(format!(
            "--at {at:?} is finer than {ts_column}'s unit ({ts_type}); give an instant that is a \
             whole number of that unit"
        )));
    }
    i64::try_from(nanos / per_unit).map_err(|_| {
        Error::KeySpec(format!(
            "--at {at:?} is out of range for {ts_column} ({ts_type})"
        ))
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
