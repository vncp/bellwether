//! `index` (the build crate's builder) and `locate`, `latest`, `asof` (its
//! plan builder), each lookup writing an Evidence of kind `plan`.

use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use bellwether_build::{BuildOptions, Builder, Lookup, PlanOrder};
use bellwether_core::sidecar::{SidecarDef, SidecarKind};
use bellwether_source::Partitioning;
use bellwether_store::parse_rfc3339;
use bellwether_types::{
    format, Error, Evidence, EvidenceBody, KeySpec, Predicate, Provenance, Record, Result, ScanPlan,
};
use bytes::Bytes;

use crate::engine::{folder_changes, guard, schema_hint, Engine, FolderChanges, Pinned};
use crate::keys::KeyFile;
use crate::lookback::{window_start, Lookback};
use crate::results::{Cost, IndexResult, LookupKind, PlanResult, SidecarState, SpecState};
use crate::spec::Declaration;

impl Engine {
    /// Builds or updates the index so it declares `specs` (in addition to
    /// those it already declares), over every file under the data root, in
    /// one manifest commit (build crate, ADR 012, 013). Returns the committed
    /// sequence, the segment counts, and every declared spec's completeness.
    pub fn index(&self, specs: &[KeySpec]) -> Result<IndexResult> {
        let declared: Vec<Declaration> = specs
            .iter()
            .map(|s| Declaration {
                key: Some(s.clone()),
                sidecar: None,
            })
            .collect();
        self.index_declared(&declared)
    }

    /// [`Engine::index`] for `index --key` arguments as
    /// [`crate::spec::parse_declaration`] reads them: key specs, and the
    /// sidecars beside or instead of them (per-key time bounds
    /// `exact:symbol@ts`, bitemporal versions `temporal2:id@valid,known`).
    pub fn index_declared(&self, declared: &[Declaration]) -> Result<IndexResult> {
        guard(|| self.index_inner(declared)).map_err(schema_hint)
    }

    fn index_inner(&self, declared: &[Declaration]) -> Result<IndexResult> {
        self.require_current("index")?;
        if declared.is_empty() && self.manifest()?.is_none() {
            return Err(Error::KeySpec(
                "a new index needs at least one key spec; pass --key exact:<column> (or \
                 temporal:<column>@<ts>)"
                    .into(),
            ));
        }
        let specs: Vec<KeySpec> = declared.iter().filter_map(|d| d.key.clone()).collect();
        for s in &specs {
            crate::spec::check(s)?;
        }
        let mut options = BuildOptions::new(self.format().kind());
        options.chunk_size = self.options().chunk_size;
        options.schema.clone_from(&self.options().schema);
        options.fingerprint_policy = self.options().fingerprint_policy;
        options
            .partitioning
            .clone_from(&self.options().partitioning);
        options.sidecars = declared.iter().filter_map(|d| d.sidecar.clone()).collect();
        let report =
            Builder::new(Arc::clone(self.store()?), "").build(self.root(), &specs, &options)?;
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
            sidecars: pinned
                .planner
                .declared_sidecars()
                .iter()
                .map(|d| SidecarState {
                    sidecar: d.name(),
                    complete_at: pinned.planner.sidecar_completeness(d),
                })
                .collect(),
            unknown_segment_kinds: m.unknown_segment_kinds(),
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

    /// Plans the rows of the keys in `keys` whose time column lies in
    /// `[from, to]` (each an RFC 3339 instant, a `YYYY-MM-DD` date for a
    /// date column, or an integer in the column's unit; converted exactly).
    /// The time column is `ts_column`, or else the one time column that a
    /// complete per-key bounds sidecar (`exact:<keys>@<ts>`) or a range spec
    /// on a timestamp column names. Every bounds sidecar on that column
    /// prunes per key: a row group stays only when the key's own times
    /// there reach into the window. The residual is `In` and `Between`.
    pub fn locate_window(
        &self,
        keys: &KeyFile,
        from: &str,
        to: &str,
        ts_column: Option<&str>,
    ) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Window {
                    from,
                    to,
                    ts_column,
                },
            )
        })
        .map_err(schema_hint)
    }

    /// The bitemporal as-of: per key, the newest row by valid time among
    /// rows known at or before `known` and valid at or before `valid`, ties
    /// on valid time going to the later known time, through the one
    /// complete `temporal2` sidecar serving the key columns. Either bound
    /// may be omitted (unbounded); `latest --known K` is `valid = None`. A
    /// null in either clock never wins (ADR 014). The residual carries both
    /// bounds, `NotOlderThan` on each clock, then `LatestPerKey`.
    pub fn asof_bitemporal(
        &self,
        keys: &KeyFile,
        valid: Option<&str>,
        known: Option<&str>,
    ) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Bitemporal { valid, known })).map_err(schema_hint)
    }

    /// Plans the rows of the keys in `keys` in the lookback window ending at
    /// `at`: every row whose time column lies in `[at - lookback, at]`,
    /// both inclusive. `at` is read as [`Engine::asof`] reads it; `lookback`
    /// is a whole number and a calendar unit, `s`, `m`, `h`, `d` (24 hours
    /// of UTC), or `w` (7 days), or a bare number for an integer time
    /// column ([`crate::Lookback`]; trading-day calendars are out of scope).
    /// The time column is `ts_column`, else the timestamp of the one
    /// temporal spec serving the key columns, else the one a window would
    /// take ([`Engine::locate_window`]). The plan is the window's: every row
    /// group holding a key with a row in the window, pruned by per-key time
    /// bounds, a temporal spec's version chains, a range key on the time
    /// column, and partition or path-template values, each when there is
    /// one. The residual is `In` and `Between`.
    pub fn asof_lookback(
        &self,
        keys: &KeyFile,
        at: &str,
        lookback: &str,
        ts_column: Option<&str>,
    ) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Lookback {
                    at: Some(at),
                    lookback,
                    ts_column,
                },
            )
        })
        .map_err(schema_hint)
    }

    /// [`Engine::asof_lookback`] ending at the newest timestamp among the
    /// versions of the keys in `keys`, index-wide over live row groups
    /// (invariant 5), through the one temporal spec serving the key columns
    /// (whose timestamp is the window's column). One window serves every
    /// key, so a key whose history ended earlier has fewer rows in it. When
    /// no key has a non-null timestamp the plan is empty (ADR 014).
    pub fn latest_lookback(&self, keys: &KeyFile, lookback: &str) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Lookback {
                    at: None,
                    lookback,
                    ts_column: None,
                },
            )
        })
        .map_err(schema_hint)
    }

    /// Invariant 1 over the folder, not only the index: a file that landed
    /// or changed after the last build would be silently left out of the
    /// plan, so it is refused. A plan pinned to an earlier manifest
    /// (`--at-manifest`) answers over the files that manifest recorded, so
    /// files that arrived since are out of scope by design; a recorded file
    /// that changed or vanished is still refused.
    fn refuse_stale(&self, pinned: &Pinned, verb: &str) -> Result<()> {
        let mut changes = folder_changes(self.root(), pinned, self.format())?;
        if self.options().at_manifest.is_some() {
            changes.untracked.clear();
        }
        if changes.is_empty() {
            Ok(())
        } else {
            Err(stale_index(self.root(), verb, &changes))
        }
    }

    fn plan_lookup(&self, keys: &KeyFile, mode: Mode<'_>) -> Result<PlanResult> {
        let verb = match mode {
            Mode::Locate | Mode::Window { .. } => "locate",
            Mode::Latest
            | Mode::Bitemporal { valid: None, .. }
            | Mode::Lookback { at: None, .. } => "latest",
            Mode::Asof(_) | Mode::Bitemporal { .. } | Mode::Lookback { .. } => "asof",
        };
        let pinned = self.pin_required(verb)?;
        self.refuse_stale(&pinned, verb)?;
        let (specs, excluded) = pinned.specs();
        let (run_keys, null_tuples) = keys.run_keys(pinned.planner.schema())?;
        let columns = run_keys.columns().to_vec();
        let tuples: Vec<Vec<u8>> = run_keys.tuples().map(<[u8]>::to_vec).collect();
        let payload = run_keys.to_payload()?;
        let key_set = bellwether_types::Hash::of(&payload);
        self.cache()?.put_cached(&key_set, Bytes::from(payload))?;
        let (lookup, kind) = self.lookup_of(&pinned, &specs, &columns, tuples, mode, verb)?;
        let (plan, order) = pinned.planner.plan_ordered(&specs, &lookup).map_err(|e| {
            if specs.is_empty() && !excluded.is_empty() {
                not_complete_yet(self.root(), verb, &excluded)
            } else {
                e
            }
        })?;
        if matches!(
            kind,
            LookupKind::Latest | LookupKind::Asof { .. } | LookupKind::Bitemporal { .. }
        ) {
            check_temporal_residual(&plan)?;
        }
        let sidecars: Vec<String> = pinned
            .planner
            .sidecars_for(&lookup)
            .iter()
            .map(SidecarDef::name)
            .collect();
        let path_values = pinned.planner.path_column_for(&lookup)?;
        let files_total = pinned.manifest().rowgroup_space.files.len() as u64;
        let planned_total = plan.row_groups_total;
        let planned = Cost {
            files: plan.entries.len() as u64,
            row_groups: plan.row_groups_selected,
            bytes: plan.bytes,
        };
        let (ordered_by, unordered_reason) = match order {
            PlanOrder::Ordered { column } => (Some(column), None),
            PlanOrder::Unspecified { reason } => (None, Some(reason)),
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
            sidecars,
            path_values,
            files_total,
            row_groups_total: planned_total,
            planned,
            ordered_by,
            unordered_reason,
            evidence,
        })
    }

    /// The planner's lookup for `mode` over the key `tuples` on `columns`,
    /// and how the result names it.
    fn lookup_of(
        &self,
        pinned: &Pinned,
        specs: &[KeySpec],
        columns: &[String],
        tuples: Vec<Vec<u8>>,
        mode: Mode<'_>,
        verb: &'static str,
    ) -> Result<(Lookup, LookupKind)> {
        Ok(match mode {
            Mode::Locate => (
                Lookup::Keys {
                    columns: columns.to_vec(),
                    keys: tuples,
                },
                LookupKind::Locate,
            ),
            Mode::Latest => {
                serving_temporal(self.root(), verb, specs, columns)?;
                (
                    Lookup::Latest {
                        columns: columns.to_vec(),
                        keys: tuples,
                    },
                    LookupKind::Latest,
                )
            }
            Mode::Asof(at) => asof_lookup(self.root(), verb, pinned, specs, columns, tuples, at)?,
            Mode::Window {
                from,
                to,
                ts_column,
            } => window_lookup(pinned, specs, columns, tuples, from, to, ts_column)?,
            Mode::Lookback {
                at,
                lookback,
                ts_column,
            } => lookback_lookup(
                self.root(),
                verb,
                pinned,
                specs,
                columns,
                tuples,
                at,
                lookback,
                ts_column,
            )?,
            Mode::Bitemporal { valid, known } => {
                self.bitemporal_lookup(pinned, columns, tuples, valid, known)?
            }
        })
    }

    /// The bitemporal lookup on `columns` through the one complete
    /// `temporal2` sidecar serving them, with both bounds converted to their
    /// columns' units exactly.
    fn bitemporal_lookup(
        &self,
        pinned: &Pinned,
        columns: &[String],
        tuples: Vec<Vec<u8>>,
        valid: Option<&str>,
        known: Option<&str>,
    ) -> Result<(Lookup, LookupKind)> {
        let probe = Lookup::Bitemporal {
            columns: columns.to_vec(),
            keys: Vec::new(),
            valid: None,
            known: None,
        };
        let def = match pinned.planner.sidecars_for(&probe).as_slice() {
            [one] => one.clone(),
            [] => {
                return Err(Error::KeySpec(format!(
                    "no complete temporal2 sidecar serves a lookup on ({}); declare one with \
                     `bellwether index {} --key temporal2:{}@<valid>,<known>`",
                    columns.join(", "),
                    self.root(),
                    columns.join(",")
                )))
            }
            many => {
                return Err(Error::KeySpec(format!(
                    "several temporal2 sidecars serve a lookup on ({}): {}; pass a key file with \
                     exactly the key columns of one (--key)",
                    columns.join(", "),
                    many.iter()
                        .map(SidecarDef::name)
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
            }
        };
        let (valid_column, known_column) =
            (def.time_columns[0].clone(), def.time_columns[1].clone());
        let in_unit =
            |field: &'static str, at: Option<&str>, column: &str| -> Result<Option<i64>> {
                at.map(|a| {
                    let t = pinned
                        .planner
                        .column_types(&[column.to_string()])?
                        .remove(0)
                        .1;
                    instant_in_unit(field, a, column, &t)
                })
                .transpose()
            };
        let valid_ts = in_unit("valid", valid, &valid_column)?;
        let known_ts = in_unit("known", known, &known_column)?;
        Ok((
            Lookup::Bitemporal {
                columns: columns.to_vec(),
                keys: tuples,
                valid: valid_ts,
                known: known_ts,
            },
            LookupKind::Bitemporal {
                valid: valid.map(str::to_string),
                known: known.map(str::to_string),
                valid_ts,
                known_ts,
                valid_column,
                known_column,
            },
        ))
    }
}

#[derive(Clone, Copy)]
enum Mode<'a> {
    Locate,
    Latest,
    Asof(&'a str),
    Window {
        from: &'a str,
        to: &'a str,
        ts_column: Option<&'a str>,
    },
    Lookback {
        at: Option<&'a str>,
        lookback: &'a str,
        ts_column: Option<&'a str>,
    },
    Bitemporal {
        valid: Option<&'a str>,
        known: Option<&'a str>,
    },
}

/// The as-of lookup through the one temporal spec serving `columns`, its
/// instant converted to the timestamp column's unit exactly.
fn asof_lookup(
    root: &str,
    verb: &'static str,
    pinned: &Pinned,
    specs: &[KeySpec],
    columns: &[String],
    tuples: Vec<Vec<u8>>,
    at: &str,
) -> Result<(Lookup, LookupKind)> {
    let ts_column = serving_temporal(root, verb, specs, columns)?;
    let ts_type = pinned
        .planner
        .column_types(std::slice::from_ref(&ts_column))?
        .remove(0)
        .1;
    let ts = instant_in_unit("at", at, &ts_column, &ts_type)?;
    Ok((
        Lookup::AsOf {
            columns: columns.to_vec(),
            keys: tuples,
            ts,
        },
        LookupKind::Asof {
            at: at.to_string(),
            ts,
            ts_column,
        },
    ))
}

/// The window lookup on `columns`: the time column named or found
/// ([`window_column`]), and both ends converted to its unit exactly.
fn window_lookup(
    pinned: &Pinned,
    specs: &[KeySpec],
    columns: &[String],
    tuples: Vec<Vec<u8>>,
    from: &str,
    to: &str,
    ts_column: Option<&str>,
) -> Result<(Lookup, LookupKind)> {
    let ts_column = match ts_column {
        Some(c) => c.to_string(),
        None => window_column(pinned, specs, columns)?,
    };
    let ts_type = pinned
        .planner
        .column_types(std::slice::from_ref(&ts_column))?
        .remove(0)
        .1;
    let lo = instant_in_unit("window", from, &ts_column, &ts_type)?;
    let hi = instant_in_unit("window", to, &ts_column, &ts_type)?;
    if lo > hi {
        return Err(Error::Other(format!(
            "the window starts at {from} ({ts_column} = {lo}) after it ends at {to} ({hi}); \
             pass --from before --to"
        )));
    }
    Ok((
        Lookup::Window {
            columns: columns.to_vec(),
            keys: tuples,
            ts_column: ts_column.clone(),
            lo,
            hi,
        },
        LookupKind::Window {
            from: from.to_string(),
            to: to.to_string(),
            lo,
            hi,
            ts_column,
        },
    ))
}

/// The time column of a window lookup on `columns` when none is named
/// ([`default_window_column`] over the pinned index).
fn window_column(pinned: &Pinned, specs: &[KeySpec], columns: &[String]) -> Result<String> {
    let m = pinned.manifest();
    let scheme = Partitioning::parse(m.source.partitioning.as_deref())?;
    let first = m
        .rowgroup_space
        .files
        .first()
        .map(|f| f.fingerprint.path.as_str());
    let partition = partition_columns(&scheme, &m.source.root, first)?;
    default_window_column(
        pinned.planner.schema(),
        specs,
        bounds_time_columns(pinned, columns),
        partition,
        columns,
    )
}

/// The time columns of the complete per-key bounds sidecars on `columns`.
pub(crate) fn bounds_time_columns(pinned: &Pinned, columns: &[String]) -> Vec<String> {
    pinned
        .planner
        .declared_sidecars()
        .into_iter()
        .filter(|d| {
            d.kind == SidecarKind::Bounds
                && d.columns.iter().all(|c| columns.contains(c))
                && pinned.planner.sidecar_completeness(d).is_some()
        })
        .map(|d| d.time_columns[0].clone())
        .collect()
}

/// The partition columns `scheme` gives the files under `root`: a
/// template's placeholders, or the `name=value` directories of `first`, one
/// file's path (every file has the same ones).
pub(crate) fn partition_columns(
    scheme: &Partitioning,
    root: &str,
    first: Option<&str>,
) -> Result<Vec<String>> {
    Ok(match (scheme, first) {
        (Partitioning::Template(t), _) => t.names().map(str::to_string).collect(),
        (Partitioning::Hive, Some(path)) => scheme
            .values(root, path)?
            .into_iter()
            .map(|v| v.name)
            .collect(),
        _ => Vec::new(),
    })
}

/// The time column of a window on `columns` when none is named: the one
/// time column of the complete per-key bounds sidecars on those columns
/// (`bounded`), else of the range specs on a timestamp or date column,
/// else of the temporal specs on those columns, else of the partition
/// columns (hive directories or path-template placeholders) of a timestamp
/// or date type. Several at the first level that has any is an error asking
/// for `--ts`.
pub(crate) fn default_window_column(
    schema: &arrow::datatypes::Schema,
    specs: &[KeySpec],
    bounded: Vec<String>,
    partition: Vec<String>,
    columns: &[String],
) -> Result<String> {
    let is_time = |c: &String| {
        schema.field_with_name(c).is_ok_and(|f| {
            matches!(
                f.data_type(),
                DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
            )
        })
    };
    let ranged: Vec<String> = specs
        .iter()
        .filter_map(|s| match s {
            KeySpec::Range { column } => Some(column.clone()),
            _ => None,
        })
        .collect();
    let temporal: Vec<String> = specs
        .iter()
        .filter_map(|s| match s {
            KeySpec::Temporal {
                columns: c,
                ts_column,
            } if c.iter().all(|x| columns.contains(x)) => Some(ts_column.clone()),
            _ => None,
        })
        .collect();
    let levels = [
        ("per-key bounds", bounded),
        ("range keys", ranged),
        ("temporal keys", temporal),
        ("partition columns", partition),
    ];
    for (what, found) in levels {
        let mut found: Vec<String> = found.into_iter().filter(|c| is_time(c)).collect();
        found.sort();
        found.dedup();
        match found.as_slice() {
            [] => {}
            [one] => return Ok(one.clone()),
            many => {
                return Err(Error::KeySpec(format!(
                    "{what} exist on several time columns ({}); name one with --ts",
                    many.join(", ")
                )))
            }
        }
    }
    Err(Error::KeySpec(format!(
        "a window lookup on ({}) needs a time column: declare per-key bounds with `--key \
         exact:{}@<ts>`, a range key `--key range:<ts>`, or a temporal key `--key \
         temporal:{}@<ts>`, take the time from file paths (--partitioning hive or \
         --path-template with a date placeholder), or name the column with --ts",
        columns.join(", "),
        columns.join(","),
        columns.join(",")
    )))
}

/// The lookback lookup on `columns` (see [`Engine::asof_lookback`] and
/// [`Engine::latest_lookback`]): the window `[end - lookback, end]` on the
/// time column, `end` the instant `at` or, without one, the newest version
/// of the keys.
#[allow(clippy::too_many_arguments)]
fn lookback_lookup(
    root: &str,
    verb: &'static str,
    pinned: &Pinned,
    specs: &[KeySpec],
    columns: &[String],
    tuples: Vec<Vec<u8>>,
    at: Option<&str>,
    lookback: &str,
    ts_column: Option<&str>,
) -> Result<(Lookup, LookupKind)> {
    let lb = Lookback::parse(lookback)?;
    let ts_column = match (at, ts_column) {
        (_, Some(c)) => c.to_string(),
        (None, None) => serving_temporal(root, verb, specs, columns)?,
        (Some(_), None) => {
            let temporal: Vec<&String> = specs
                .iter()
                .filter_map(|s| match s {
                    KeySpec::Temporal {
                        columns: c,
                        ts_column,
                    } if c.iter().all(|x| columns.contains(x)) => Some(ts_column),
                    _ => None,
                })
                .collect();
            match temporal.as_slice() {
                [one] => (*one).clone(),
                [] => window_column(pinned, specs, columns)?,
                _ => serving_temporal(root, verb, specs, columns)?,
            }
        }
    };
    let ts_type = pinned
        .planner
        .column_types(std::slice::from_ref(&ts_column))?
        .remove(0)
        .1;
    let n = lb.in_unit(&ts_column, &ts_type)?;
    let end = match at {
        Some(a) => Some(instant_in_unit("at", a, &ts_column, &ts_type)?),
        None => pinned.planner.newest_version(specs, columns, &tuples)?,
    };
    // No key has a version: an empty window (no row), never a full scan.
    let (lo, hi) = end.map_or((1, 0), |e| (window_start(e, n, &ts_type), e));
    Ok((
        Lookup::Window {
            columns: columns.to_vec(),
            keys: tuples,
            ts_column: ts_column.clone(),
            lo,
            hi,
        },
        LookupKind::Lookback {
            at: at.map(str::to_string),
            lookback: lb.as_given().to_string(),
            lookback_seconds: lb.seconds(),
            end,
            lo: end.map(|_| lo),
            ts_column,
        },
    ))
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
/// remainder below the unit is an error, never rounded. `field` names the
/// request field the instant came from, for [`Error::InvalidArgument`].
pub(crate) fn instant_in_unit(
    field: &'static str,
    at: &str,
    ts_column: &str,
    ts_type: &DataType,
) -> Result<i64> {
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
            field,
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
        field,
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
                "{ts_column} has type {other}, which a time lookup cannot order by; use a \
                 timestamp, date, or integer column"
            )))
        }
    };
    if nanos % per_unit != 0 {
        return Err(Error::InvalidArgument {
            field,
            reason: format!(
                "the instant {at:?} is finer than {ts_column}'s unit ({ts_type}); give an \
                 instant that is a whole number of that unit"
            ),
        });
    }
    i64::try_from(nanos / per_unit).map_err(|_| Error::InvalidArgument {
        field,
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
            instant_in_unit("at", "1970-01-01T00:00:01Z", "ts", &us).unwrap(),
            1_000_000
        );
        assert_eq!(instant_in_unit("at", "42", "ts", &us).unwrap(), 42);
        let s = DataType::Timestamp(TimeUnit::Second, None);
        assert!(instant_in_unit("at", "1970-01-01T00:00:01.5Z", "ts", &s).is_err());
        assert_eq!(
            instant_in_unit("at", "1970-01-03", "d", &DataType::Date32).unwrap(),
            2
        );
        assert!(instant_in_unit("at", "1970-01-03T01:00:00Z", "d", &DataType::Date32).is_err());
        assert!(instant_in_unit("at", "yesterday", "ts", &us).is_err());
        assert!(instant_in_unit("at", "2026-01-01T00:00:00Z", "v", &DataType::Int64).is_err());
        assert!(instant_in_unit("at", "2026-01-01T00:00:00Z", "v", &DataType::Utf8).is_err());
    }
}
